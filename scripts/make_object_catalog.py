"""
make_object_catalog.py

Save catalogs to parquet from forced-photometry coadds across available filters.
"""
import os

import numpy as np
from astropy.table import join

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults

_default_fill_value = {'i': -1, 'b': False, 'U': ''}

def _ensure_butler_instance(butler_or_repo):
    if not isinstance(butler_or_repo, Butler):
        return Butler(butler_or_repo)
    return butler_or_repo

def _get_fill_value(name, dtype):
    kind = np.dtype(dtype).kind
    fill_value = _default_fill_value.get(kind, np.nan)
    if kind == 'b' and (name.endswith('_flag_bad') or name.endswith('_flag_noGoodPixels')):
        fill_value = True
    return fill_value


def generate_object_catalog(output_dir, butler, tract, patches=None,
                            overwrite=True, verbose=False,
                            filename_prefix='object',
                            parquet_engine='pyarrow',
                            **kwargs):
    """Save catalogs to parquet from forced-photometry coadds across available filters.
    Iterates through patches, saving each in append mode to the save HDF5 file.
    Parameters
    --
    output_dir : str
        Output directory
    butler: Butler or str (of a repo)
        File location of Butler repository+rerun to load.
    tract: int
        Tract of sky region to load
    filename_prefix: str
        filename will be of the form "%s_%d_%s.parquet" % (filename_prefix, tract, patch)
        With the addition that the comma will be removed from the patch name
        to provide a valid Python identifier: e.g., 'coadd_4849_11'
    overwrite: bool
        Overwrite an existing HDF file.
    parquet_engine : str, optional
        default is pyarrow
    """
    if not patches:
        # Extract the patches for this tract from the skymap
        butler = _ensure_butler_instance(butler)
        skymap = butler.get(datasetType='deepCoadd_skyMap')
        patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]
    else:
        try:
            patches = patches.split(',')
        except AttributeError:
            pass
        else:
            if not all(len(p) == 2 for p in patches):
                raise ValueError('patches should be a list or a string in "11,22,33" format')
            patches = ['{},{}'.format(*p) for p in patches]

    for patch in patches:
        if verbose:
            print("Processing tract %d, patch %s" % (tract, patch))

        file_path = os.path.join(
            output_dir,
            '_'.join((filename_prefix, str(tract), patch.replace(',', ''))) + '.parquet',
        )

        if not overwrite and (os.path.exists(file_path) or os.path.exists(file_path + '.empty')):
            if verbose:
                print("  Skipping tract %d, patch %s because output file exist" % (tract, patch))
            continue

        butler = _ensure_butler_instance(butler)
        merged_cat = merge_coadd_forced_src(butler, tract, patch, verbose=verbose, **kwargs)

        if merged_cat is None:
            if verbose:
                print("  No entries for tract %d, patch %s" % (tract, patch))
            open(file_path + '.empty', 'w').close()
            continue

        merged_cat.to_parquet(
            file_path,
            engine=parquet_engine,
            compression=None,
            index=False,
        )
        del merged_cat


def merge_coadd_forced_src(butler, tract, patch, keys_join_on=('id',),
                           filters='ugrizy', verbose=False, return_pandas=True):
    """Load patch catalogs.  Return merged catalog across filters.

    butler: Butler object or str
        Either a Butler object or a filename to the repo
    tract: int
        Tract in skymap
    patch: str
        Patch in the tract in the skymap
    keys_join_on: iterable of str
        Join the catalogs for each filter on these fields
    filters: iterable of str
        Filter names to load

    Returns
    --
    Pandas DataFrame of patch catalog merged across filters.
    """
    butler = _ensure_butler_instance(butler)

    # Define the filters and order in which to sort them.:
    tract_patch_data_id = {'tract': tract, 'patch': patch}
    try:
        ref_table = butler.get(datasetType='deepCoadd_ref',
                               dataId=tract_patch_data_id)
    except NoResults as e:
        if verbose:
            print("  ", e)
        return

    ref_table = ref_table.asAstropy()
    isPrimary = ref_table['detect_isPrimary']
    if not isPrimary.any():
        if verbose:
            print("  No good isPrimary entries for tract {}, patch {}".format(tract, patch))
        return

    merged_cat = ref_table[isPrimary]
    del ref_table

    merged_cat['tract'] = int(tract)
    merged_cat['patch'] = str(patch)

    cat_dtype = None
    missing_filters = list()
    for filter_this in filters:
        filter_name = filters.get(filter_this) if hasattr(filters, 'get') else filter_this
        this_data_id = dict(tract_patch_data_id, filter=filter_name)
        try:
            cat = butler.get(datasetType='deepCoadd_forced_src',
                             dataId=this_data_id)
        except NoResults as e:
            if verbose:
                print("  ", e)
            missing_filters.append(filter_this)
            continue

        cat = cat.asAstropy()
        cat = cat[isPrimary]

        # Magnitudes will be calculated in the GCR reader / DPDD formatting
        # For now we just extract the grey FLUXMAG0
        calib = butler.get('deepCoadd_calexp_photoCalib', this_data_id)
        cat['FLUXMAG0'] = calib.getInstFluxAtZeroMagnitude()

        if cat_dtype is None:
            cat_dtype = cat.dtype

        cat.meta = None
        for name in cat_dtype.names:
            if name in keys_join_on:
                continue
            cat.rename_column(name, '{}_{}'.format(filter_this, name))

        merged_cat = join(merged_cat, cat, list(keys_join_on), 'outer')
        del cat

    assert cat_dtype is not None

    for filter_this in missing_filters:
        for name, (dt, _) in cat_dtype.fields.items():
            if name in keys_join_on:
                continue
            merged_cat['{}_{}'.format(filter_this, name)] = _get_fill_value(name, dt)

    return merged_cat.to_pandas() if return_pandas else merged_cat


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Generate merged static-sky photometry (based on deepCoadd forced photometry)
    and save to parquet file
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('tract', type=int, nargs='+',
                        help='Skymap tract[s] to process.')
    parser.add_argument('--patches', type=str, default="",
                        help='''
Skymap patch[es] within each tract to process. Format should be "11,21,31"
''')
    parser.add_argument('--name', default='object',
                        help='Base name of files: <name>_tract_5062.hdf5')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files')
    parser.add_argument('--hsc', dest='hsc', action='store_true',
                        help='Uses HSC filters')
    parser.add_argument('--parquet_engine', dest='engine', default='pyarrow',
                        choices=['fastparquet', 'pyarrow'],
                        help="""(default: %(default)s)""")
    args = parser.parse_args()

    filters = 'ugrizy'
    if args.hsc:
        filters = {'u': 'HSC-U', 'g': 'HSC-G', 'r': 'HSC-R', 'i': 'HSC-I',
                   'z': 'HSC-Z', 'y': 'HSC-Y'}

    if len(args.tract) > 1 and args.patches:
        print("You specified more than 1 tract but only need partical patches??")

    for tract in args.tract:
        generate_object_catalog(
            args.output_dir, args.repo, tract, args.patches,
            overwrite=args.overwrite, verbose=args.verbose,
            filename_prefix=args.name, parquet_engine=args.engine,
            filters=filters
        )
