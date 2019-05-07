import os
import re
import sys

import numpy as np
import pandas as pd

from lsst.daf.persistence import Butler
from lsst.daf.persistence.butlerExceptions import NoResults

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


def load_and_save_tract_metacal(repo, tract, filename, key_prefix='coadd', patches=None,
                        overwrite=True, verbose=False, **kwargs):
    """Save catalogs to HDF5 from metacal coadds.

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
        # Extract the patches for this tract from the skymap
        skymap = butler.get(datasetType='deepCoadd_skyMap')
        patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]

    for patch in patches:
        if verbose:
            print("Processing tract %d, patch %s" % (tract, patch))
        patch_metacal_cat = load_patch_metacal(butler, tract, patch, verbose=verbose, **kwargs)
        if len(patch_metacal_cat) == 0:
            if verbose:
                print("  No good entries for tract %d, patch %s" % (tract, patch))
            continue

        key = '%s_%d_%s' % (key_prefix, tract, patch)
        key = valid_identifier_name(key)
        patch_metacal_cat.to_hdf(filename, key, format='fixed')


def load_patch_metacal(butler_or_repo, tract, patch,
               fields_to_join=('id',),
               filters={'u': 'u', 'g': 'g', 'r': 'r', 'i': 'i', 'z': 'z', 'y': 'y'},
               trim_colnames_for_fits=False,
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
    filters: iterable of str
        Filter names to load
    trim_colnames_for_fits: bool
        Trim column names to satisfy the FITS standard character limit of <68.

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

    # For the remaining objects after primary selection, we
    metacal = butler.get(datasetType='deepCoadd_mcalmax_deblended',
                             dataId=tract_patch_data_id)

    metacal = metacal.asAstropy().to_pandas()
    metacal = metacal[isPrimary]

    if trim_colnames_for_fits:
        # FITS column names can't be longer that 68 characters
        # Trim here to ensure consistency across any format we write this out to
        trim_long_colnames(metacal)

    return metacal


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


if __name__ == '__main__':
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Generate merged metacal catalog

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
    parser.add_argument('--name', default='object',
                        help='Base name of files: <name>_metacal_tract_5062.hdf5')
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s)')
    parser.add_argument('--verbose', dest='verbose', default=True,
                        action='store_true', help='Verbose mode.')
    parser.add_argument('--silent', dest='verbose', action='store_false',
                        help='Turn off verbosity.')
    parser.add_argument('--hsc', dest='hsc', action='store_true',
                        help='Uses HSC filters')
    args = parser.parse_args(sys.argv[1:])

    if args.hsc:
        filters = {'u': 'HSC-U', 'g': 'HSC-G', 'r': 'HSC-R', 'i': 'HSC-I',
                   'z': 'HSC-Z', 'y': 'HSC-Y'}
    else:
        filters = {'u': 'u', 'g': 'g', 'r': 'r', 'i': 'i', 'z': 'z', 'y': 'y'}

    for tract in args.tract:
        filebase = '{:s}_metacal_tract_{:d}'.format(args.name, tract)
        filename = os.path.join(args.output_dir, filebase + '.hdf5')
        load_and_save_tract_metacal(args.repo, tract, filename,
                            patches=args.patches, verbose=args.verbose,
                            filters=filters)
