import os

from merge_tract_cat import load_patch, load_tract, load_and_save_tract

REPO = '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output'


def example_load_and_save_tract(tract=4849, filename='test.hdf5', patches=None, repo=REPO, rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_and_save_tract(repo, tract, filename, patches=patches)


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


def example_load_and_plot(filename=None,
                          summary_dir=os.path.join(REPO, '..', 'summary')):
    """Example demonstrating loading an HDF file into Pandas and plotting."""
    import matplotlib.pyplot as plt
    import pandas as pd

    if filename is None:
        filebase = 'merged_tract_4849_1,1_1,2.hdf5'
        filename = os.path.join(summary_dir, filebase)
    df = pd.read_hdf(filename)

    plt.hist2d(df['g_mag']-df['r_mag'], df['r_mag']-df['i_mag'],
               range=((-1, +2), (-1, +2)), bins=40)
    plt.colorbar()
    plt.xlabel('r-i')
    plt.ylabel('g-r')
    plt.show()


if __name__ == "__main__":
    tract = 4849

    load_and_plot_example = True
    if load_and_plot_example:
        example_load_and_plot()

    load_and_save_tract_example = False
    if load_and_save_tract_example:
        filebase = 'merged_tract_%d' % (tract)
        filename = filebase+'.hdf5'
        example_load_and_save_tract(tract, filename)

    patches=['1,1', '1,2']
    load_tract_example = False
    if load_tract_example:
        filebase = 'merged_tract_%d_%s' % (tract, '_'.join(patches))
        tract_cat = example_load_tract(tract=tract, patches=patches)
        tract_cat.to_pandas().to_hdf(filebase+'.hdf5', key='coadd', format='table')

    load_patch_example = False
    if load_patch_example:
        patch = patches[0]
        patch_cat = example_load_patch(tract=tract, patch=patch)

        filebase = 'merged_tract_%d_%s' % (tract, patch)
        patch_cat.to_pandas().to_hdf(filebase+'.hdf5', key='coadd', format='table')
