"""
DIA Object extraction and collation

Takes a LSST DM Science Pipelines repository extracts the key information
needed for a DIA Object table following the LSST Data Products Definition Document.
https://ls.st/dpdd
"""

import os

import pandas as pd

from lsst.daf.persistence import Butler


def load_patch(butler_or_repo, tract, patch,
               dataset_type='deepCoadd_diaObject',
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

    # Define the filters and order in which to sort them.:
    tract_patch_data_id = {'tract': tract, 'patch': patch}
    cat = butler.get(datasetType=dataset_type, dataId=tract_patch_data_id)
    cat = cat.asAstropy().to_pandas()

    return cat


def load_and_save_tract(repo, tract, filename,
                        dataset_type='deepCoadd_diaObject',
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

    tract_cat = pd.DataFrame()
    for patch in patches:
        if verbose:
            print("Processing tract %d, patch %s" % (tract, patch))
        tract_patch_data_id = {'tract': tract, 'patch': patch}
        cat = butler.get(datasetType=dataset_type, dataId=tract_patch_data_id)
        cat = cat.asAstropy().to_pandas()
        patch_cat = load_patch(butler, tract, patch, verbose=verbose, **kwargs)
        if len(patch_cat) == 0:
            if verbose:
                print("  No good entries for tract %d, patch %s" % (tract, patch))
            continue

        tract_cat.append(patch_cat)

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
    parser.add_argument('tract', type=int, nargs='+',
                        help='Skymap tract[s] to process.')
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
        filebase = '{:s}_tract_{:d}'.format(args.name, tract)
        filename = os.path.join(args.output_dir, filebase + '.hdf5')
        load_and_save_tract(args.repo, tract, filename,
                            dataset_type=args.dataset,
                            patches=args.patches,
                            verbose=args.verbose,
                            debug=args.debug)
