import re
import sys

from astropy.table import join, vstack
from astropy.utils.metadata import MergeStrategy, enable_merge_strategies
import numpy as np

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults


# Copied from
# http://docs.astropy.org/en/stable/api/astropy.utils.metadata.enable_merge_strategies.html
class MergeNumbersAsList(MergeStrategy):
    """Merge scalar numbers as list.

    Intended for use to merge metadata numbers in catalog metadata."""
    types = ((int, float), (int, float))

    @classmethod
    def merge(cls, left, right):
        return [left, right]


class MergeListNumbersAsList(MergeStrategy):
    """Merge list and scalar numbers as list.

    Intended for use to merge metadata numbers in catalog metadata."""
    types = ((list), (int, float))

    @classmethod
    def merge(cls, left, right):
        return left.append(right)


def valid_identifier_name(name):
    """Return a valid Python identifier name from input string.

    For now just strips commas and spaces
    The full regex to satisfy is
        good_identifier_name = "^[a-zA-Z_][a-zA-Z0-9_]*$"
    But that doesn't define the prescription for creating a good one.

    >>> valid_identifier_name('coadd_4849_1,1')
    'coadd_4849_11'
    >>> valid_identifier_name('coadd_4849_1,1_2,3')
    'coadd_4849_11_23'
    >>> valid_identifier_name('coadd_48^49_1,1_2 3;')
    'coadd_4849_11_23'
    >>> valid_identifier_name('2234coadd_48^49_1,1_2 3;')
    'coadd_4849_11_23'
    >>> valid_identifier_name('2234,48^49 3;')
    ''
    """
    remove_characters_regex = '[^a-zA-Z0-9_]'
    name = re.sub(remove_characters_regex, '', name)
    # Remove beginning characters that are numbers
    name = re.sub('^[0-9]*', '', name)
    return name


def load_and_save_tract(repo, tract, filename, key_prefix='coadd', patches=None,
                        overwrite=True, verbose=False, **kwargs):
    """Save catalogs to HDF5 from forced-photometry coadds across available filters.

    Iterates through patches, saving each in append mode to the save HDF5 file.

    Parameters
    --
    repo: str
        File location of Butler repository+rerun to load.
    tract: int
        Tract of sky region to load
    filename: str
        Filename for HDF file.
    key_prefix: str
        Base for the key in the HDF file.
        Keys will be of the form "%s_%d_%s" % (keybase, tract, patch)
        With the addition that the comma will be removed from the patch name
        to provide a valid Python identifier: e.g., 'coadd_4849_11'
    overwrite: bool
        Overwrite an existing HDF file.
    """
    butler = Butler(repo)
    if patches is None:
        patches = ['%d,%d' % (i, j) for i in range(8) for j in range(8)]

    for patch in patches:
        if verbose:
            print("Processing tract %d, patch %s" % (tract, patch))
        try:
            patch_merged_cat = load_patch(butler, tract, patch, **kwargs)
        except NoResults as e:
            print(e)
            continue

        key = '%s_%d_%s' % (key_prefix, tract, patch)
        key = valid_identifier_name(key)
        patch_merged_cat.to_pandas().to_hdf(filename, key, format='fixed')


def load_tract(repo, tract, patches=None, **kwargs):
    """Merge catalogs from forced-photometry coadds across available filters.

    Parameters
    --
    tract: int
        Tract of sky region to load
    repo: str
        File location of Butler repository+rerun to load.
    patches: list of str
        List of patches.  If not specified, will default to '0,0'--'7,7'.

    Returns
    --
    AstroPy Table of merged catalog
    """
    butler = Butler(repo)
    if patches is None:
        patches = ['%d,%d' % (i, j) for i in range(8) for j in range(8)]

    merged_patch_cats = []
    for patch in patches:
        try:
            this_patch_merged_cat = load_patch(butler, tract, patch, **kwargs)
        except NoResults as e:
            print(e)
            continue
        merged_patch_cats.append(this_patch_merged_cat)

    merged_tract_cat = vstack(merged_patch_cats)
    return merged_tract_cat


def load_patch(butler_or_repo, tract, patch,
               fields_to_join=('id',),
               filters=('u', 'g', 'r', 'i', 'z', 'y'),
               trim_colnames_for_fits=False,
               ):
    """Load patch catalogs.  Return merged catalog across filters.

    butler_or_repo: Butler object or str
        Either a Butler object or a filename to the repo
    tract: int
        Tract in skymap
    patch: str
        Patch in the tract in the skymap
    fields_to_join: iterable of str
        Join the catalogs for each filter on these fields
    filters: iterable of str
        Filter names to load
    trim_colnames_for_fits: bool
        Trim column names to satisfy the FITS standard character limit of <68.

    Returns
    --
    AstroPy Table of patch catalog merged across filters.
    """
    if isinstance(butler_or_repo, str):
        butler = Butler(butler_or_repo)
    else:
        butler = butler_or_repo

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
        if len(cat) < 1:
            continue
        # Rename duplicate columns with prefix of filter
        prefix_columns(cat, filt, fields_to_skip=fields_to_join)
        # Merge metadata with concatenation
        with enable_merge_strategies(MergeNumbersAsList, MergeListNumbersAsList):
            merged_patch_cat = join(merged_patch_cat, cat, keys=fields_to_join)

    if trim_colnames_for_fits:
        # FITS column names can't be longer that 68 characters
        # Trim here to ensure consistency across any format we write this out to
        trim_long_colnames(merged_patch_cat)

    return merged_patch_cat


def trim_long_colnames(cat):
    """Trim long column names in an AstroPy Table by specific replacements.

    Intended to help trim down column names to satisfy the FITS standard limit
    of 68 characters.

    Operates on 'cat' in place.

    Parameters
    --
    cat: AstroPy Table
    """
    import re
    long_short_pairs = [
        ('GeneralShapeletPsf', 'GSPsf'),
        ('DoubleShapelet', 'DS'),
        ('noSecondDerivative', 'NoSecDer')]
    for long, short in long_short_pairs:
        long_re = re.compile(long)
        for col_name in cat.colnames:
            if long_re.search(col_name):
                new_col_name = long_re.sub(short, col_name)
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


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Generate merged static-sky photometry (based on deepCoadd forced photometry)

    Note that the following defines the tracts for the DC2 Run 1.1p processing.
    DC2_tracts = {}
    DC2_tracts['Run1.1p'] = (5066, 5065, 5064, 5063, 5062,
                             4852, 4851, 4850, 4849, 4848,
                             4640, 4639, 4638, 4637, 4636,
                             4433, 4432, 4431, 4430, 4429,)
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('repo', type=str,
                        help='Filepath to LSST DM Stack Butler repository.')
    parser.add_argument('tract', type=int, nargs='+',
                        help='Skymap tract[s] to process.')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    args = parser.parse_args(sys.argv[1:])

    for tract in args.tract:
        filebase = 'merged_tract_%d' % tract
        filename = filebase+'.hdf5'
        load_and_save_tract(args.repo, tract, filename, verbose=args.verbose)
