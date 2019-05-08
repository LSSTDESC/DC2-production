import os
import re
import sys

import numpy as np
import pandas as pd

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults

def load_and_save_tract(repo, tract, filename, patches=None,
                        overwrite=True, verbose=False, **kwargs):
    """Save metacal catalogs to parquet for entire tract.

    Iterates through patches and saves aggregated table for the entire tract .

    Parameters
    --
    repo: str
        File location of Butler repository+rerun to load.
    tract: int
        Tract of sky region to load
    filename: str
        Filename for parquet file.
    """
    butler = Butler(repo)

    if patches is None:
        # Extract the patches for this tract from the skymap
        skymap = butler.get(datasetType='deepCoadd_skyMap')
        patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]

    df = None
    for patch in patches:
        if verbose:
            print("Processing tract %d, patch %s" % (tract, patch))
        patch_merged_cat = load_metacal_patch(butler, tract, patch, verbose=verbose, **kwargs)
        if len(patch_merged_cat) == 0:
            if verbose:
                print("  No good entries for tract %d, patch %s" % (tract, patch))
            continue

        if df is None:
            df = patch_merged_cat
        else:
            df = df.append(patch_merged_cat)

    if overwrite:
        if os.path.exists(filename):
            os.remove(filename)

    df.to_parquet(filename)

def load_metacal_patch(butler_or_repo, tract, patch,
               fields_to_join=('id',),
               verbose=False,
               debug=False
               ):
    """Load metacal patch catalogs.  Return metacal catalog.

    butler_or_repo: Butler object or str
        Either a Butler object or a filename to the repo
    tract: int
        Tract in skymap
    patch: str
        Patch in the tract in the skymap
    fields_to_join: iterable of str
        Join the catalogs for each filter on these fields
    Returns
    --
    Pandas DataFrame of patch metacal catalog.
    """
    if isinstance(butler_or_repo, str):
        butler = Butler(butler_or_repo)
    else:
        butler = butler_or_repo

    # Define the filters and order in which to sort them.:
    tract_patch_data_id = {'tract': tract, 'patch': patch}
    try:
        ref_table = butler.get(datasetType='deepCoadd_ref',
                               dataId=tract_patch_data_id)
        ref_table = ref_table.asAstropy().to_pandas()
    except NoResults as e:
        if verbose:
            print(" ", e)
        return pd.DataFrame()


    isPrimary = ref_table['detect_isPrimary']
    ref_table = ref_table[isPrimary]
    if len(ref_table) == 0:
        if verbose:
            print("  No good isPrimary entries for tract %d, patch %s" % (tract, patch))
        return ref_table

    try:
        metacal = butler.get(datasetType='deepCoadd_mcalmax_deblended',
                             dataId=tract_patch_data_id)
    except NoResults as e:
        if verbose:
            print(" ", e)
        return pd.DataFrame()

    metacal = metacal.asAstropy().to_pandas()
    metacal = metacal[isPrimary]
    # Dropping redundant columns with the main reference catalog
    metacal = metacal.drop(["coord_ra", "coord_dec", "parent"], axis=1)

    # TODO: Remove this when we can create composite catalogs
    metacal = pd.merge(metacal, ref_table, on=fields_to_join, sort=False)

    return metacal

if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Generate merged metacal catalog.

    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('tract', type=int, nargs='+',
                        help='Skymap tract[s] to process.')
    parser.add_argument('--patches', nargs='+',
                        help='''
Skymap patch[es] within each tract to process.
A common use-case for this option is quick testing.
''')
    parser.add_argument('--name', default='metacal',
                        help='Base name of files: <name>_tract_5062.parquet')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    args = parser.parse_args(sys.argv[1:])

    for tract in args.tract:
        filebase = '{:s}_tract_{:d}'.format(args.name, tract)
        filename = os.path.join(args.output_dir, filebase + '.parquet')
        load_and_save_tract(args.repo, tract, filename,
                            patches=args.patches, verbose=args.verbose)
