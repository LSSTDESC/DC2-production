import os

from merge_tract_cat import load_patch, load_tract

REPO = '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output'


def example_load_tract(tract=4849, repo=REPO, rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_tract(repo, tract)


def example_load_patch(tract=4849, patch='1,1', repo=REPO, rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_patch(repo, tract, patch)


if __name__ == "__main__":
    tract = 4849
    patch = '1,1'

    patch_cat = example_load_patch(tract=tract, patch=patch)

    filebase = 'merged_tract_%d_%s' % (tract, patch)
    patch_cat.write(filebase+'.ecsv', format='ascii.ecsv')

    # Each of the following is expected to fail:
    ### Column names too long for FITS file standard
    patch_cat.write(filebase+'.fits', format='fits')
    ### No PyTables available on Cori:
    patch_cat.to_pandas().to_hdf(filebase+'.h5', 'table')
