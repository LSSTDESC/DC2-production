"""
make_metacal_catalog.py

Save catalogs to parquet from metacal runs across available patches.
"""
import os
import re

import numpy as np
from astropy.table import hstack

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults


def _ensure_butler_instance(butler_or_repo):
    if not isinstance(butler_or_repo, Butler):
        return Butler(butler_or_repo)
    return butler_or_repo


def generate_metacal_catalog(output_dir, butler, tract, patches=None,
                             overwrite=True, verbose=False,
                             filename_prefix='metacal',
                             parquet_engine='pyarrow',
                             **kwargs):
    """Save catalogs to parquet from individual metacal patches.
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

        metacal_cat = load_metacal_patch(butler, tract, patch, verbose=verbose, **kwargs)

        if metacal_cat is None:
            if verbose:
                print("  No entries for tract %d, patch %s" % (tract, patch))
            open(file_path + '.empty', 'w').close()
            continue

        metacal_cat.to_parquet(
            file_path,
            engine=parquet_engine,
            compression=None,
            index=False,
        )
        del metacal_cat

def load_metacal_patch(butler, tract, patch, verbose=False, return_pandas=True,
                       fields_to_join=('id',), debug=False):
    """Load metacal patch catalog.

    butler: Butler object or str
        Either a Butler object or a filename to the repo
    tract: int
        Tract in skymap
    patch: str
        Patch in the tract in the skymap
    fields_to_join: iterable of str
        Join the catalogs for each filter on these fields

    Returns
    --
    Pandas DataFrame of patch catalogs.
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

    try:
        metacal = butler.get(datasetType='deepCoadd_mcalmax_deblended',
                             dataId=tract_patch_data_id)
    except NoResults as e:
        if verbose:
            print(" ", e)
        return

    metacal = metacal.asAstropy()
    metacal = metacal[isPrimary]

    if debug:
        assert (metacal['id'] == ref_table['id']).all()

    # Dropping redundant columns with the main reference catalog
    del metacal["coord_ra"]
    del metacal["coord_dec"]
    del metacal["parent"]
    del metacal["id"]

    return metacal.to_pandas() if return_pandas else metacal


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Generate merged metacal catalog and save to parquet file
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
    parser.add_argument('--name', default='metacal',
                        help='Base name of files: <name>_tract_5062_11.parquet')
    parser.add_argument('-o', '--output-dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files')
    parser.add_argument('--parquet_engine', dest='engine', default='pyarrow',
                        choices=['fastparquet', 'pyarrow'],
                        help="""(default: %(default)s)""")
    args = parser.parse_args()

    if len(args.tract) > 1 and args.patches:
        print("You specified more than 1 tract but only need partial patches??")

    for tract in args.tract:
        generate_metacal_catalog(
            args.output_dir, args.repo, tract, args.patches,
            overwrite=args.overwrite, verbose=args.verbose,
            filename_prefix=args.name, parquet_engine=args.engine,
        )
