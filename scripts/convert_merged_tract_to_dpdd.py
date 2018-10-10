#!/usr/bin/env python

"""Take LSST DESC DC2 Generic Catalog Reader accessible Object table
(merged_tract trim cat file) and produce output files labeled with DPDD names.

Output in HDF, FITS, and Parquet

For each format there is one file per tract+patch.

TODO: Exploring writing HDF and Parquet files in full tracts
because those file formats allow easy writing to existing files.
Keep FITS files in tract+patch.

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
    """Produce DPDD output files for all available tracts in GCR 'reader'.

    The input filename is expected to match 'trim_merged_tract_.*\.hdf5$'.

    Parameters
    ----------
    reader : str, optional
        GCR reader to use. Must match an existing yaml file.
        Default is dc2_coadd_run1.1p

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.

    """
    trim_config = {'filename_pattern': 'trim_merged_tract_.*\.hdf5$'}
    cat = GCRCatalogs.load_catalog(reader, trim_config)

    convert_cat_to_dpdd(cat, **kwargs)

def convert_tract_to_dpdd(tract, reader='dc2_coadd_run1.1p', **kwargs):
    """Produce DPDD output files for specified 'tract' and GCR 'reader'.

    The input filename is expected to match 'trim_merged_tract_{:04d}\.hdf5$'.

    Parameters
    ----------
    tract : int
        Skymap tract to process.
    reader : str, optional
        GCR reader to use. Must match an existing yaml file.
        Default is dc2_coadd_run1.1p

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.

    """
    trim_thistract_config = {
        'filename_pattern': 'trim_merged_tract_{:04d}\.hdf5$'.format(tract)}
    cat = GCRCatalogs.load_catalog(reader, trim_thistract_config)

    convert_cat_to_dpdd(cat, **kwargs)

def convert_cat_to_dpdd(cat, reader='dc2_coadd_run1.1p', **kwargs):
    """Save DPDD-named columns files for all tracts,
    patches from input GCR catalog.

    Parameters
    ----------
    cat : DC2ObjectCatalog instance
        Catalog instance returned by `GCRCatalogs.load_catalog`.
    reader : str, optional
        GCR reader to use. Must match an existing yaml file.
        Default is dc2_coadd_run1.1p

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.

    """
    columns = cat.list_all_quantities()
    columns.extend(['tract', 'patch'])

    quantities = cat.get_quantities(columns, return_iterator=True)
    append=False
    for quantities_this_patch in quantities:
        quantities_this_patch = pd.DataFrame.from_dict(quantities_this_patch)
        write_dataframe_to_files(quantities_this_patch, append=append, **kwargs)
        append=True


def write_dataframe_to_files(
        df,
        filename_prefix='dpdd_object',
        hdf_key_prefix='object',
        parquet_engine='fastparquet',
        parquet_compression='gzip',
        append=False,
        verbose=True,
        **kwargs):
    """Write out dataframe to HDF, FITS, and Parquet files.

    Choose file names based on tract (HDF) or tract + patch (FITS, Parquet).

    Parameters
    ----------
    df : Pandas DataFrame
        Pandas DataFrame with the input catalog data to write out.
    filename_prefix : str, optional
        Prefix to be added to the output filename. Default is 'dpdd_object'.
    hdf_key_prefix : str, optional
        Group name within the output HDF5 file. Default is 'object'.
    parquet_compression : str, optional
        Compression algorithm to use when writing Parquet files.
        Potential: gzip, snappy, lzo, uncompressed. Default is gzip.
        Availability depends on the engine used.
    parquet_engine : str, optional
        Engine to write parquet on disk. Available: fastparquet, pyarrow.
        Default is fastparquet.
    verbose : boolean, optional
        If True, print out debug messages. Default is True.
    """
    # We know that our GCR reader will chunk by tract+patch
    # So we take the tract and patch in the first entry
    # as the identifying tract, patch for all.
    tract, patch = df['tract'][0], df['patch'][0]
    patch = patch.replace(',', '')  # Convert '0,1'->'01'

    # Normalise output filename
    outfile_base_tract_format = '{base}_tract_{tract:04d}'
    outfile_base_tract_patch_format = \
        '{base}_tract_{tract:04d}_patch_{patch:s}'

    # tract is an int
    # but patch is a string (e.g., '01' for '0,1')
    key_format = '{key_prefix:s}_{tract:04d}_{patch:s}'
    info = {
        'base': filename_prefix,
        'tract': tract,
        'patch': patch,
        'key_prefix': hdf_key_prefix}
    outfile_base_tract = outfile_base_tract_format.format(**info)
    outfile_base_tract_patch = outfile_base_tract_patch_format.format(**info)

    if verbose:
        print("Writing {} {} to HDF5 DPDD file.".format(tract, patch))
    key = key_format.format(**info)
    df.to_hdf(outfile_base_tract+'.hdf5', key=key, append=append, format='table')

    if verbose:
        print("Writing {} {} to Parquet DPDD file.".format(tract, patch))
    df.to_parquet(outfile_base_tract+'.parquet',
                  append=append,
                  engine=parquet_engine,
                  compression=parquet_compression)
# Consider uses a file format other than 'simple' to enable partition.
# e.g., format='hive', format='drill'
#                  partition_on=('tract', 'patch'))

    if verbose:
        print("Writing {} {} to FITS DPDD file.".format(tract, patch))
    Table.from_pandas(df).write(outfile_base_tract_patch + '.fits')


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

You can also specify the engine to use to write parquet files, and the
compression algorithm to use:

python %(prog)s
    [--parquet_engine PARQUET_ENGINE]
    [--parquet_compression PARQUET_COMPRESSION]

Available engines are fastparquet (default) and pyarrow. Note that they need
to be installed on your machine to be used (pip install PARQUET_ENGINE --user).
Potential compression algorithms are gzip (default), snappy, lzo, uncompressed.
Availability depends on the engine used.

[2018-10-02: The 'dc2_object_run1.2p reader doesn't exist yet.]

"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('--tract', type=int, nargs='+', default=[],
                        help='Skymap tract[s] to process. Default is all.')
    parser.add_argument('--reader', default='dc2_coadd_run1.1p',
                        help='GCR reader to use.')
    parser.add_argument('--parquet_engine', default='fastparquet',
                        help="""Parquet engine to use.
Available: fastparquet (default) or pyarrow.""")
    parser.add_argument('--parquet_compression', default='gzip',
                        help="""Compression algorithm to use.
Potential: gzip (default), snappy, lzo, uncompressed.
Availability depends on the engine used.""")
    parser.add_argument('--verbose', default=False, action='store_true')

    args = parser.parse_args(sys.argv[1:])

    if len(args.tract) == 0:
        convert_all_to_dpdd(
            reader=args.reader,
            parquet_engine=args.parquet_engine,
            parquet_compression=args.parquet_compression)

    for tract in args.tract:
        convert_tract_to_dpdd(
            tract,
            reader=args.reader,
            parquet_engine=args.parquet_engine,
            parquet_compression=args.parquet_compression)
