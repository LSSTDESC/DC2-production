"""
make_object_catalog.py

Save catalogs to parquet from forced-photometry coadds across available filters.
"""
import os
import re

import numpy as np

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults


def _ensure_butler_instance(butler_or_repo):
    if not isinstance(butler_or_repo, Butler):
        return Butler(butler_or_repo)
    return butler_or_repo


_default_fill_value = {'i': -1, 'b': False, 'U': ''}


def _get_fill_value(name, dtype):
    kind = np.dtype(dtype).kind
    fill_value = _default_fill_value.get(kind, np.nan)
    if kind == 'b' and (name.endswith('_flag_bad') or name.endswith('_flag_noGoodPixels')):
        fill_value = True
    return np.array(fill_value, dtype=np.dtype(dtype))


def generate_object_catalog(output_dir, butler, tract, patches=None,
                            overwrite=True, verbose=False,
                            filename_prefix='object',
                            parquet_engine='pyarrow',
                            **kwargs):
    """Save catalogs to parquet from forced-photometry coadds across available filters.
    Iterates through patches, saving each in append mode to the save parquet file.
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
        Overwrite existing output file(s).
    parquet_engine : str, optional
        default is pyarrow
    """
    if not patches:
        # Extract the patches for this tract from the skymap
        butler = _ensure_butler_instance(butler)
        skymap = butler.get(datasetType='deepCoadd_skyMap')
        patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]
    elif hasattr(patches, 'split'):
        patches = patches.split('^')

    if not all(re.match(r'\d,\d$', p) for p in patches):
        raise ValueError('patches should be a list or a string in "1,1^2,2^3,3" format')

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


def merge_coadd_forced_src(butler, tract, patch, filters='ugrizy',
                           verbose=False, return_pandas=True,
                           debug=False):
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

    ref_table = ref_table[isPrimary]
    ref_table['tract'] = int(tract)
    ref_table['patch'] = str(patch)

    tables_to_merge = dict()
    for filter_this in filters:
        filter_name = filters.get(filter_this) if hasattr(filters, 'get') else filter_this
        this_data_id = dict(tract_patch_data_id, filter=filter_name)
        try:
            cat = butler.get(datasetType='deepCoadd_forced_src',
                             dataId=this_data_id)
        except NoResults as e:
            if verbose:
                print("  ", e)
            continue

        cat = cat.asAstropy()
        cat = cat[isPrimary]
        if debug:
            assert (cat['id'] == ref_table['id']).all()
        del cat['id']

        # Magnitudes will be calculated in the GCR reader / DPDD formatting
        # For now we just extract the grey FLUXMAG0
        calib = butler.get('deepCoadd_calexp_photoCalib', this_data_id)
        cat['FLUXMAG0'] = calib.getInstFluxAtZeroMagnitude()

        tables_to_merge[filter_this] = cat

    try:
        cat_dtype = next(iter(tables_to_merge.values())).dtype
    except StopIteration:
        if verbose:
            print("  No filter can be found in deepCoadd_forced_src")
        return

    if debug:
        assert all(cat_dtype == cat.dtype for cat in tables_to_merge.values())

    merged_cat = ref_table  # merged_cat will start with the reference table
    merged_cat.meta = None
    for filter_this in filters:
        if filter_this in tables_to_merge:
            cat = tables_to_merge[filter_this]
            for name in cat_dtype.names:
                merged_cat['{}_{}'.format(filter_this, name)] = cat[name]
            del cat, tables_to_merge[filter_this]
        else:
            for name, (dt, _) in cat_dtype.fields.items():
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
    parser.add_argument('-p', '--patch', '--patches', dest='patches', type=str,
                        default="", help='''
Skymap patch[es] within each tract to process. Format should be "1,1^2,1^3,1"
''')
    parser.add_argument('--name', default='object',
                        help='Base name of files: <name>_tract_5062_11.parquet')
    parser.add_argument('-o', '--output-dir', default='./',
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
        print("You specified more than 1 tract but only need partial patches??")

    for tract in args.tract:
        generate_object_catalog(
            args.output_dir, args.repo, tract, args.patches,
            overwrite=args.overwrite, verbose=args.verbose,
            filename_prefix=args.name, parquet_engine=args.engine,
            filters=filters
        )
