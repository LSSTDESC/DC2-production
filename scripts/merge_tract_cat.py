import os
import sys

from astropy.table import join, vstack
import numpy as np

from lsst.daf.persistence import Butler


def load_tract(repo, tract, patches=None, **kwargs):
    """Merge catalogs from forced-photometry coadds across available filters.

    Parameters
    --
    tract: int
        Tract of sky region to load
    repo: str
        File location of Butler repository+rerun to load.

    Returns
    --
    AstroPy Table of merged catalog
    """
    butler = Butler(repo)
    if patches is None:
        patches = ['%d,%d' % (i, j) for i in range(8) for j in range(8)]

    merged_patch_cats = []
    for patch in patches:
        this_patch_merged_cat = load_patch(butler, tract, patch, **kwargs)
        merged_patch_cats.append(this_patch_merged_cat)

    merged_tract_cat = vstack(merged_patch_cats)
    return merged_tract_cat


def load_patch(butler, tract, patch,
               fields_to_join=('id',),
               filters=('u', 'g', 'r', 'i', 'z', 'y'),
               trim_colnames_for_fits=False,
               ):
    """Load patch catalogs.  Return merged catalog across filters."""
    # Define the filters and order in which to sort them.:
    tract_patch_data_id = {'tract': tract, 'patch': patch}
    ref_table = butler.get(datasetType='deepCoadd_ref',
                           dataId=tract_patch_data_id).asAstropy()

    isPrimary = ref_table['detect_isPrimary']

    merge_filter_cats = {}
    for filt in filters:
        this_data = tract_patch_data_id.copy()
        this_data['filter'] = filt
        try:
            cat = butler.get(datasetType='deepCoadd_forced_src',
                             dataId=this_data).asAstropy()
        except Exception as e:
            print(e)
            continue

        CoaddCalib = butler.get('deepCoadd_calexp_calib', this_data)
        CoaddCalib.setThrowOnNegativeFlux(False)

        mag, mag_err = CoaddCalib.getMagnitude(cat['base_PsfFlux_flux'], cat['base_PsfFlux_fluxSigma'])

        cat['mag'] = mag
        cat['mag_err'] = mag_err
        cat['SNR'] = np.abs(cat['base_PsfFlux_flux'])/cat['base_PsfFlux_fluxSigma']

        cat = cat[isPrimary]

        merge_filter_cats[filt] = cat

    merged_patch_cat = ref_table  # This is not a new copy
    for filt in filters:
        if filt not in merge_filter_cats:
            continue

        cat = merge_filter_cats[filt]
        # Rename duplicate columns with prefix of filter
        prefix_columns(cat, filt, fields_to_skip=fields_to_join)
        merged_patch_cat = join(merged_patch_cat, cat, keys=fields_to_join)

    if trim_colnames_for_fits:
        # FITS column names can't be longer that 68 characters
        # Trim here to ensure consistency across any format we write this out to
        trim_long_colnames(merged_patch_cat)

    return merged_patch_cat


def trim_long_colnames(cat):
    """Trim long column names in an AstroPy Table by specific replacements."""
    import re
    long_short_pairs = [
        ('GeneralShapeletPsf', 'GSPsf'),
        ('DoubleShapelet', 'DS'),
        ('noSecondDerivative', 'NoSecDer')]
    for long, short in long_short_pairs:
        long_re = re.compile(long)
        for col_name in cat.colnames:
            if long_re.search(col_name):
                new_col_name =long_re.sub(short, col_name)
                cat.rename_column(col_name, new_col_name)


def prefix_columns(cat, filt, fields_to_skip=()):
    """Prefix the columns of an AstroPy Table with the filter name.

    >>> from astropy.table import Table
    >>> tab = Table([['a', 'b'], [1, 2]], names=('letter', 'number'))
    >>> prefix_columns(tab, 'filter')
    >>> print(tab)
    filter_letter filter_number
    ------------- -------------
                a             1
                b             2

    """
    old_colnames = cat.colnames
    for field in fields_to_skip:
        field_idx = old_colnames.index(field)
        old_colnames.pop(field_idx)

    new_colnames = ['%s_%s' % (filt, col) for col in old_colnames]
    for oc, nc in zip(old_colnames, new_colnames):
        cat.rename_column(oc, nc)


def example_load_tract(tract=4849,
                       repo='/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output',
                       rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    return load_tract(repo, tract)


def example_load_patch(tract=4849, patch='1,1',
                       repo='/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output',
                       rerun=None):
    """Test the loading of one tract.

    Default arguments set to run on NERSC DC2 Run.1.1 exploratory reduction.
    """
    if rerun is not None:
        repo = os.path.join(repo, 'rerun', rerun)
    from lsst.daf.persistence import Butler
    butler = Butler(repo)
    return load_patch(butler, tract, patch)


if __name__ == '__main__':
    repo, tract = sys.argv[1:]
    tract = int(tract)
    patches_subset = ['1,1', '2,2']
    tract_cat = load_tract(repo, tract, patches=patches)

    filebase = 'merged_tract_%d' % tract
    tract_cat.write(filebase+'.ecsv', format='ascii.ecsv', overwrite=True)
    ### FITS doesn't like some of the column names
#    tract_cat.write(filebase+'.fits', format='fits')
    ### HDF5 doesn't like the header column size.
#    tract_cat.to_hdf(filebase+'.h5', 'table')
