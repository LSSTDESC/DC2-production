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

import pandas as pd

import GCRCatalogs


def convert_cat_to_parquet(reader,
                           output_filename=None,
                           include_native=False,
                           **kwargs):
    """Save columns from input GCR catalog.

    Parameters
    ----------
    reader : str
        GCR reader to use. Must match an existing yaml file.

    Other Parameters
    ----------------
    output_filename : str, optional
        If None, then will be constructed as '<reader>.parquet'
    include_native : Include the native quantities from the GCR reader class
                     in addition to the standardized non-native quantities.
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.
    """
    if output_filename is None:
        output_filename = '{}.{}'.format(reader, 'parquet')

    cat = GCRCatalogs.load_catalog(reader)
    # We don't want to use the cache we don't want to use the extra memory
    # when we know we are just going through the data once.
    cat.use_cache = False

    columns = cat.list_all_quantities(include_native=include_native)

    quantities = cat.get_quantities(columns, return_iterator=True)
    for quantities_this_chunk in quantities:
        quantities_this_chunk = pd.DataFrame.from_dict(quantities_this_chunk)
        write_dataframe_to_files(quantities_this_chunk,
                                 output_filename=output_filename,
                                 **kwargs)


def write_dataframe_to_files(
        df,
        output_filename='cat.parquet',
        parquet_scheme='simple',
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
    output_filename : str, optional
        Output filename. Default is 'cat.parquet'.
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
    if verbose:
        print("Writing chunk {} to Parquet file.".format(df))
    # Append iff the file already exists
    parquet_append = append and os.path.exists(output_filename)
    df.to_parquet(output_filename,
                  #append=parquet_append,
                  #file_scheme=parquet_scheme,
                  engine=parquet_engine,
                  compression=parquet_compression)


if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
Produce Parquet output file from a GCR catalog.

Example:

To produce an output Parquet file from the 'dc2_object_run1.2p' GCR catalog:

python %(prog)s dc2_object_run1.2p

By default the output name will be 'dc2_object_run1.2p.parquet'.  You could specify a different one on the command line:

python %(prog)s dc2_object_run1.2p --output_filename dc2_object.parquet

You can also specify the Parquet scheme, engine, and compression to use.

python %(prog)s
    --parquet_scheme hive
    --parquet_engine fastparquet
    --parquet_compression gzip

The selected engine needs to be installed on your machine to use.  E.g.,

pip install fastparquet --user
pip install pyarrow --user

Potential compression algorithms are gzip (default), snappy, lzo, uncompressed.
Availability depends on the installation of the engine used.
"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('reader', help='GCR catalog to read.')
    parser.add_argument('--output_filename', default=None,
                        help='Output filename')
    parser.add_argument('--include_native', action='store_true', default=False,
                        help='Include the native along with the non-native GCR catalog quantities')
    parser.add_argument('--parquet_scheme', default='simple',
                        choices=['hive', 'simple'],
                        help="""'simple': one file.
'hive': one directory with a metadata file and
the data partitioned into row groups.
(default: %(default)s)
""")
    parser.add_argument('--parquet_engine', default='fastparquet',
                        choices=['fastparquet', 'pyarrow'],
                        help="""(default: %(default)s)""")
    parser.add_argument('--parquet_compression', default='gzip',
                        choices=['gzip', 'snappy', 'lzo', 'uncompressed'],
                        help="""(default: %(default)s)""")
    parser.add_argument('--verbose', default=False, action='store_true')

    args = parser.parse_args()
    kwargs = vars(args)

    convert_cat_to_parquet(**kwargs)
