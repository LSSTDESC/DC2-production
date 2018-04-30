import os

from merge_tract_cat import load_patch, load_tract, load_and_save_tract

REPO = '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output'


def example_load_and_save_tract(tract=4849, patches=None, repo=REPO, rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_and_save_tract(repo, tract, patches=patches)


def example_load_tract(tract=4849, patches=None, repo=REPO, rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_tract(repo, tract, patches=patches)


def example_load_patch(tract=4849, patch='1,1', repo=REPO, rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_patch(repo, tract, patch)


if __name__ == "__main__":
    tract = 4849

    load_and_save_tract_example = True
    if load_and_save_tract_example:
        filebase = 'merged_tract_%d' % (tract)
        filename = filebase+'.hdf5'
        example_load_and_save_tract(tract=tract)

    patches=['1,1', '1,2']
    load_tract_example = False
    if load_tract_example:
        filebase = 'merged_tract_%d_%s' % (tract, '_'.join(patches))
        tract_cat = example_load_tract(tract=tract, patches=patches)
        tract_cat.write(filebase+'.ecsv', format='ascii.ecsv')
        tract_cat.to_pandas().to_hdf(filebase+'.hdf5', 'coadd')

    load_patch_example = False
    if load_patch_example:
        patch = patches[0]
        patch_cat = example_load_patch(tract=tract, patch=patch)

        filebase = 'merged_tract_%d_%s' % (tract, patch)
        patch_cat.write(filebase+'.ecsv', format='ascii.ecsv')
        patch_cat.write(filebase+'.fits', format='fits')
        # patch_cat.to_pandas().to_hdf(filebase+'.h5', 'table')

        # Each of the following is expected to fail:
        ### Column names too long for FITS file standard
        patch_cat.write(filebase+'.fits', format='fits')
        ### No PyTables available on Cori:
        patch_cat.to_pandas().to_hdf(filebase+'.h5', 'table')
