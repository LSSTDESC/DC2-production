#!/usr/bin/env python

"""Take LSST DESC DC2 Generic Catalog Reader accessible Object table
(merged_tract trim cat file) and produce output files labeled with DPDD names.

Output in HDF, FITS, and Parquet

For each format there is one file per tract+patch.

TODO: Exploring writing HDF and Parquet files in full tracts
because those file formats allow easy writing to existing files.
Keep FITS files in tracdt+patch.

Requires

pandas  # version >= 0.21
generic-catalog-reader
LSSTDESC/gcr-catalog

and either
pyarrow or fastparquet.
"""

import sys

from astropy.table import Table
import pandas as pd

import GCRCatalogs


def convert_all_to_dpdd(reader='dc2_coadd_run1.1p', **kwargs):
    """Produce DPDD output files for all available tracts in GCR 'reader'."""
    trim_config = {'filename_pattern': 'trim_merged_tract_.*\.hdf5$'}
    cat = GCRCatalogs.load_catalog(reader, trim_config)

    convert_cat_to_dpdd(cat, **kwargs)


def convert_tract_to_dpdd(tract, reader='dc2_coadd_run1.1p', **kwargs):
    """Produce DPDD output files for specified 'tract' and GCR 'reader'."""
    trim_thistract_config = {
        'filename_pattern': 'trim_merged_tract_{:04d}\.hdf5$'.format(tract)}
    cat = GCRCatalogs.load_catalog(reader, trim_thistract_config)

    convert_cat_to_dpdd(cat, **kwargs)


def convert_cat_to_dpdd(cat, reader='dc2_coadd_run1.1p',
                        key_prefix='object',
                        verbose=True, **kwargs):
    """Save DPDD columns for all tracts, patches in a input GCR catalog.

    This is done in one function because we're doing this chunk-by-chunk.
    """
    columns = cat.list_all_quantities()
    columns.extend(['tract', 'patch'])

    # tract is an int
    # but patch is a string (e.g., '01' for '0,1')
    outfile_base_format = '{base}_tract_{tract:04d}_patch_{patch:s}'
    quantities = cat.get_quantities(columns, return_iterator=True)
    for q in quantities:
        if 'tract' not in q or len(q) < 1:
            continue

        df = pd.DataFrame.from_dict(q)

        # We we know that our GCR reader will chunk by tract+patch
        # So we take the tract and patch in the first entry
        # as the identifying tract, patch for all.
        tract, patch = q['tract'][0], q['patch'][0]
        patch = ''.join(patch.split(','))
        info = {'base': 'dpdd_object', 'tract': tract, 'patch': patch,
                'key_prefix': key_prefix}
        outfile_base = outfile_base_format.format(**info)

        if verbose:
            print("Writing HDF5 DPDD file for ", tract, patch)
        key = '{key_prefix:s}_{tract:04d}_{patch:s}'.format(**info)
        df.to_hdf(outfile_base+'.hdf5', key=key)

        df.to_parquet(outfile_base+'.parquet',
                      engine='fastparquet',
                      compression='gzip')

        if verbose:
            print("Writing FITS DPDD file for ", tract, patch)
        Table.from_pandas(df).write(outfile_base+'.fits')


if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
Produce HDF5, FITS, Parquet output files from DC2 merged_tract object files.
The output files will have columns those specified in the LSST DPDD
(https://ls.st/dpdd), plus 'tract' and 'patch' for convenience.

Example:

To produce files from tract 4850:

python %(prog)s --tract 4850

To produce files for all available tracts call with no arguments:

python %(prog)s

To specify a different reader and produce files for all available tracts:

python %(prog)s --reader dc2_object_run1.2p

[2018-10-02: The 'dc2_object_run1.2p reader doesn't exist yet.]
"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('--tract', type=int, nargs='+', default=[],
                        help='Skymap tract[s] to process.  Default is all.')
    parser.add_argument('--reader', default='dc2_coadd_run1.1p',
                        help='GCR reader to use.')
    parser.add_argument('--verbose', default=False, action='store_true')

    args = parser.parse_args(sys.argv[1:])

    if len(args.tract) == 0:
        convert_all_to_dpdd(reader=args.reader)

    for tract in args.tract:
        convert_tract_to_dpdd(tract, reader=args.reader)
