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
LSSTDESC/gcr-catalogs

and either
pyarrow or fastparquet.
"""

import os

from astropy.table import Table
import pandas as pd

import GCRCatalogs
from GCRCatalogs.dc2_object import FILE_PATTERN


def convert_cat_to_dpdd(reader='dc2_object_run1.1p',
                        reader_config_overwrite=None, **kwargs):
    """Save DPDD-named columns files for all tracts,
    patches from input GCR catalog.

    Parameters
    ----------
    reader : str, optional
        GCR reader to use. Must match an existing yaml file.
        Default is dc2_object_run1.1p
    reader_config_overwrite : dict, optional
        config_overwrite to be supplied to GCRCatalogs.load_catalog

    Other Parameters
    ----------------
    **kwargs
        *kwargs* are optional properties writing the dataframe to files.
        See `write_dataframe_to_files` for more information.
    """
    cat = GCRCatalogs.load_catalog(reader, reader_config_overwrite)
    columns = list(cat.list_all_quantities())
    columns.extend((col for col in ('tract', 'patch') if col not in columns))

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

    if verbose:
        print("Writing {} {} to Parquet DPDD file.".format(tract, patch))
    parquet_file = outfile_base_tract+'.parquet'
    # Append iff the file already exists
    parquet_append = append and os.path.exists(parquet_file)
    df.to_parquet(parquet_file,
                  append=parquet_append,
                  file_scheme=parquet_scheme,
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
    parser.add_argument('--tract', type=int, nargs='+', default=[],
                        help='Skymap tract[s] to process.')
    parser.add_argument('--reader', default='dc2_object_run1.1p',
                        help='GCR reader to use. (default: %(default)s)')
    parser.add_argument('--parquet_scheme', default='hive',
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
    parser.add_argument('--verbose', default=False, action='store_true')

    args = parser.parse_args()

    cat_config = GCRCatalogs.get_catalog_config(args.reader)
    filename_pattern = cat_config.get('filename_pattern', FILE_PATTERN)
    # Here we assume tract_\d+ always appear in the filename pattern
    filename_pattern.replace(r'tract_\d+', 'tract_{tract}')
    if args.reader == 'dc2_object_run1.1p':
        filename_pattern = 'trim_' + filename_pattern

    for tract in (args.tract or [r'\d+']):
        convert_cat_to_dpdd(
            reader=args.reader,
            reader_config_overwrite=dict(
                filename_pattern=filename_pattern.format(tract=tract),
                use_cache=False,
            ),
            parquet_engine=args.parquet_engine,
            parquet_compression=args.parquet_compression,
        )
