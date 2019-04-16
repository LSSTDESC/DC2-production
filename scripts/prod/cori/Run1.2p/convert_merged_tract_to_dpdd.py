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

import os
import sys

from astropy.table import Table
import pandas as pd


sys.path.insert(0,'/global/homes/p/plaszczy/DC2/gcr-catalogs')

import GCRCatalogs

def convert_all_to_dpdd(reader='dc2_object_run1.2p', **kwargs):
    """Produce DPDD output files for all available tracts in GCR 'reader'.

    The input filename is expected to match 'trim_merged_tract_.*\.hdf5$'.

    Parameters
    ----------
    reader : str, optional
        GCR reader to use. Must match an existing yaml file.

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.

    """
#    trim_config = {'base_dir': os.getcwd(), 'filename_pattern': 'trim_merged_tract_.*\.hdf5$'}
    #cat = GCRCatalogs.load_catalog(reader, trim_config)

    #default from gcr_catalog
    cat = GCRCatalogs.load_catalog(reader)
    # We don't want to use the cache because we know we are just going through the data once.
    cat.use_cache = False

    convert_cat_to_dpdd(cat, **kwargs)


def convert_cat_to_dpdd(cat, **kwargs):
    """Save DPDD-named columns files for all tracts,
    patches from input GCR catalog.

    Parameters
    ----------
    cat : DC2ObjectCatalog instance
        Catalog instance returned by `GCRCatalogs.load_catalog`.

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.
    """
    columns = cat.list_all_quantities()
    columns.extend(['tract', 'patch'])

    quantities = cat.get_quantities(columns, return_iterator=True)
    for quantities_this_patch in quantities:
        quantities_this_patch = pd.DataFrame.from_dict(quantities_this_patch)
        write_dataframe_to_files(quantities_this_patch, **kwargs)


def write_dataframe_to_files(
        df,
        filename_prefix='dpdd_object',
        hdf_key_prefix='object',
        parquet_scheme='hive',
        parquet_engine='fastparquet',
        parquet_compression='gzip',
        append=True,
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
    hdf_file = outfile_base_tract+'.hdf5'
    # Append iff the file already exists
    hdf_append = append and os.path.exists(hdf_file)
    df.to_hdf(hdf_file, key=key, append=hdf_append, format='table')

##    if verbose:
##        print("Writing {} {} to Parquet DPDD file.".format(tract, patch))
##    parquet_file = outfile_base_tract+'.parquet'
##    # Append iff the file already exists
##    parquet_append = append and os.path.exists(parquet_file)
##    df.to_parquet(parquet_file,
##                  append=parquet_append,
##                  file_scheme=parquet_scheme,
##                  engine=parquet_engine,
##                  compression=parquet_compression)
# Consider uses a file format other than 'simple' to enable partition.
# e.g., format='hive', format='drill'
#                  partition_on=('tract', 'patch'))

#    if verbose:
#        print("Writing {} {} to FITS DPDD file.".format(tract, patch))
#    Table.from_pandas(df).write(outfile_base_tract_patch + '.fits')

############################################################################

if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage="produce files for all available tracts"


    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument('--reader', default='dc2_object_run1.2p',
                        help='GCR reader to use. (default: %(default)s)')
    parser.add_argument('--parquet_scheme', default='simple',
                        choices=['hive', 'simple'],
                        help="""Parquet storage scheme. (default: %(default)s)
'simple': one file.
'hive': one directory with a metadata file and
the data partitioned into row groups.""")
    parser.add_argument('--parquet_engine', default='fastparquet',
                        choices=['fastparquet', 'pyarrow'],
                        help="""Parquet engine to use. (default: %(default)s)""")
    parser.add_argument('--parquet_compression', default='gzip',
                        choices=['gzip', 'snappy', 'lzo', 'uncompressed'],
                        help="""Parquet compression algorithm to use. (default: %(default)s)""")
    parser.add_argument('--verbose', default=True, action='store_true')

    args = parser.parse_args(sys.argv[1:])

    print("reader={}".format(args.reader))

    if len(args.tract) == 0:
        convert_all_to_dpdd(
            reader=args.reader,
            parquet_engine=args.parquet_engine,
            parquet_compression=args.parquet_compression)