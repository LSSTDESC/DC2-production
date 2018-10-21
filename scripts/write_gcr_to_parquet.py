#!/usr/bin/env python

"""
Write a GCR Catalog out to a Parquet file.

pandas  # version >= 0.21
generic-catalog-reader
LSSTDESC/gcr-catalog

and either
pyarrow or fastparquet.
"""


import os
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
    cat = GCRCatalogs.load_catalog(reader)
    # We don't want to use the cache we don't want to use the extra memory
    # when we know we are just going through the data once.
    cat.use_cache = False

    convert_cat_to_dpdd(cat, **kwargs)


def convert_cat_to_dpdd(cat, include_native=True, **kwargs):
    """Save columns from input GCR catalog.

    Parameters
    ----------
    cat : GCRCatalog instance
        Catalog instance returned by `GCRCatalogs.load_catalog`.

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.
    """
    columns = cat.list_all_quantities(include_native=include_native)

    quantities = cat.get_quantities(columns, return_iterator=True)
    for quantities_this_chunk in quantities:
        quantities_this_chunk = pd.DataFrame.from_dict(quantities_this_chunk)
        write_dataframe_to_files(quantities_this_chunk, **kwargs)


def write_dataframe_to_files(
        df,
        filename_prefix='cat',
        parquet_scheme='hive',
        parquet_engine='fastparquet',
        parquet_compression='gzip',
        append=True,
        verbose=True,
        **kwargs):
    """Write out dataframe to Parquet file.

    Parameters
    ----------
    df : Pandas DataFrame
        Pandas DataFrame with the input catalog data to write out.
    filename_prefix : str, optional
        Prefix to be added to the output filename. Default is 'cat'.
    parquet_scheme : str, optional   ['simple' or 'hive']
            'simple' stores everything in one file per tract
            'hive' stores one directory with a _metadata file and then
                the columns partitioned into row groups.
            Default is simple
    parquet_engine : str, optional
        Engine to write parquet on disk. Available: fastparquet, pyarrow.
        Default is fastparquet.
    parquet_compression : str, optional
        Compression algorithm to use when writing Parquet files.
        Potential: gzip, snappy, lzo, uncompressed. Default is gzip.
        Availability depends on the engine used.
    verbose : boolean, optional
        If True, print out debug messages. Default is True.
    """
    # Normalise output filename
    outfile = '{}.{}'.format(filename_prefix, 'parquet')

    if verbose:
        print("Writing chunk {} to Parquet file.".format(df))
    # Append iff the file already exists
    parquet_append = append and os.path.exists(parquet_file)
    df.to_parquet(parquet_file,
                  append=parquet_append,
                  file_scheme=parquet_scheme,
                  engine=parquet_engine,
                  compression=parquet_compression)


if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
Produce Parquet output files from GCR catalogs.

Example:

To produce files for all data call with:

python %(prog)s

To specify a different reader and produce files for all available tracts:

python %(prog)s --reader dc2_object_run1.2p

You can also specify the engine to use to write parquet files, and the
compression algorithm to use:

python %(prog)s
    --parquet_scheme hive
    --parquet_engine fastparquet
    --parquet_compression gzip

The selected engine needs to be installed on your machine to use.  E.g.,

pip install fastparquet --user
pip install pyarrow --user

Potential compression algorithms are gzip (default), snappy, lzo, uncompressed.
Availability depends on the installation of the engine used.

[2018-10-02: The 'dc2_object_run1.2p reader doesn't exist yet.]

"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('--reader', default='dc2_coadd_run1.1p',
                        help='GCR reader to use. (default: %(default)s)')
    parser.add_argument('--parquet_scheme', default='hive',
                        choices=['hive', 'simple'],
                        help="""
Parquet storage scheme. (default: %(default)s)
'simple': one file.
'hive': one directory with a metadata file and
the data partitioned into row groups.""")
    parser.add_argument('--parquet_engine', default='fastparquet',
                        choices=['fastparquet', 'pyarrow'],
                        help="""
Parquet engine to use. (default: %(default)s)""")
    parser.add_argument('--parquet_compression', default='gzip',
                        choices=['gzip', 'snappy', 'lzo', 'uncompressed'],
                        help="""
Parquet compression algorithm to use. (default: %(default)s)""")
    parser.add_argument('--verbose', default=False, action='store_true')

    args = parser.parse_args(sys.argv[1:])

    convert_all_to_dpdd(
        reader=args.reader,
        parquet_engine=args.parquet_engine,
        parquet_compression=args.parquet_compression)
