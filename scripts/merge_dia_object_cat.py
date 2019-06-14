"""
DIA Object extraction and collation

Takes a LSST DM Science Pipelines repository and extracts the key information
needed for a DIA Object table following the LSST Data Products Definition Document.
https://ls.st/dpdd
"""

import math
import os
import sys

import numpy as np
import pandas as pd

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults


def load_patch(butler_or_repo, tract, patch,
               dataset_type='deepDiff_diaObject',
               dia_source_table=None,
               verbose=False,
               debug=False
               ):
    """Load per-patch diaObject catalogs.

    Parameters
    --
    butler_or_repo: Butler object or str
        Either a Butler object or a filename to the repo
    tract: int
        Tract in skymap
    patch: str
        Patch in the tract in the skymap
    dataset_type: str
        LSST DM Butler dataset type for diaObject.

    Returns
    --
    Pandas DataFrame of patch catalog
    """
    if isinstance(butler_or_repo, str):
        butler = Butler(butler_or_repo)
    else:
        butler = butler_or_repo

    tract_patch_data_id = {'tract': tract, 'patch': patch}
    cat = butler.get(datasetType=dataset_type, dataId=tract_patch_data_id)
    cat = cat.asAstropy().to_pandas()

    if dia_source_table is not None:
        cat = calculate_stats_from_dia_source_table(cat)

    return cat


def calculate_stats_from_dia_source_table(dia_object_df, dia_source_table):
    """Calculate summary statistics for DIA Objects from the DIA Source table.

    Parameters
    --
    dia_object_df:
        Pandas DataFrame with list of diaObject IDs in 'diaObjectId'.
    GCRCatalogs catalog:
        The DIA Source Table

    Returns
    --
    Pandas DataFrame with additional summary columns added to input DataFrame.
    """
    dia_flux_columns = ['psFlux', 'psFluxErr', 'filter']
    stats = []
    for dia_object_id in dia_object_df['diaObjectId']:
        condition = ((lambda x: x == dia_object_id), 'diaObjectId')
        df = dia_source_table.get_quantities(dia_flux_columns, filter=condition)
        _stats = calculate_stats_for_one_dia_object(df)
        _stats['diaObjectId'] = dia_object_id
        stats.append(_stats)

    df_stats = pd.DataFrame(stats)
    dia_object_df_with_stats = dia_object_df.join(df_stats, on='diaObjectId')

    return dia_object_df_with_stats


def calculate_stats_for_one_dia_object(dia_source_df):
    """Calculate summary statistics for the DIA Source rows matching a DIA Object.

    Parameters
    --
    dia_source_df:
        Pandas DataFrame

    Returns
    --
    Dict of Summary statistics
    """
    filters = np.unique(dia_source_df['filter'])
    results = {}
    for band in filters:
        df = dia_source_df.query(f'filter == "{band}"')
        results[f'psFluxMean_{band}'] = np.mean(df['psFlux'])
        results[f'psFluxSigma_{band}'] = np.std(df['psFlux'], ddof=1)
        results[f'psFluxNdata_{band}'] = len(df)
        results[f'psFluxMeanErr_{band}'] = \
            results[f'psFluxSigma_{band}'] / math.sqrt(len(df))
        residuals = df['psFlux'] - results[f'psFluxMean_{band}']
        results[f'psFluxChi2_{band}'] = np.sum((residuals / df['psFluxErr'])**2)

    return results


def load_and_save_tract(repo, tract, filename,
                        dataset_type='deepDiff_diaObject',
                        patches=None,
                        overwrite=True, verbose=False, **kwargs):
    """Save catalogs to Parquet from diaObject

    Iterates through patches and then saves the tract as a whole to a Parquet file

    Parameters
    --
    repo: str
        File location of Butler repository+rerun to load.
    tract: int
        Tract of sky region to load
    filename: str
        Filename for Parquet file.
    overwrite: bool
        Overwrite an existing Parquet file.
    """
    butler = Butler(repo)

    if patches is None:
        # Extract the patches for this tract from the skymap
        skymap = butler.get(datasetType='deepCoadd_skyMap')
        patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]

    tract_cat_list = []
    for patch in patches:
        if verbose:
            print("Processing tract %d, patch %s" % (tract, patch))
        tract_patch_data_id = {'tract': tract, 'patch': patch}
        try:
            patch_cat = load_patch(butler, tract, patch, verbose=verbose, **kwargs)
        except NoResults as e:
            if verbose:
                print(e)
                print("  No good entries for tract %d, patch %s" % (tract, patch))
            continue
        if len(patch_cat) == 0:
            if verbose:
                print("  No good entries for tract %d, patch %s" % (tract, patch))
            continue

        tract_cat_list.append(patch_cat)

    tract_cat = pd.concat(tract_cat_list)

    if len(tract_cat) == 0:
        print("  No good entries found.  Not writing file.")
        return

    tract_cat.to_parquet(filename)


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Extract difference-image analysis (DIA) object table.
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('tract', type=int, nargs='+',
                        help='Skymap tract[s] to process.')
    parser.add_argument('--dataset', type=str, default='deepDiff_diaObject',
                        help='''
Butler catalog dataset type. %(default)s
Mostly intended for use if there is a different set of templates with a new datasetType name.
''')
    parser.add_argument('--base_dir', default=None,
                        help='''
Override the base_dir setting of the reader.
This is motivated by the need to run on different file systems due to problems
locking files for access from the compute nodes on some file systems.
''')
    parser.add_argument('--patches', nargs='+',
                        help='''
Skymap patch[es] within each tract to process.
A common use-case for this option is quick testing.
''')
    parser.add_argument('--output_name', default='src',
                        help='Base name of files: <output_name>_visit_0235062.parquet')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--debug', dest='debug', default=True,
                        action='store_true', help='Debug mode.')

    args = parser.parse_args(sys.argv[1:])

    for tract in args.tract:
        filebase = '{:s}_tract_{:d}'.format(args.output_name, tract)
        filename = os.path.join(args.output_dir, filebase + '.parquet')
        load_and_save_tract(args.repo, tract, filename,
                            dataset_type=args.dataset,
                            patches=args.patches,
                            verbose=args.verbose,
                            debug=args.debug)
