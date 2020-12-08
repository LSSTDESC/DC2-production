#!/usr/bin/env python

"""
Write a catalog in GCRCatalogs out to a Parquet file
"""
import warnings
from argparse import ArgumentParser, RawTextHelpFormatter

import pyarrow as pa
import pyarrow.parquet as pq

import GCRCatalogs
from GCRCatalogs import BaseGenericCatalog
from GCRCatalogs.dc2_dm_catalog import DC2DMTractCatalog

__all__ = ["convert_cat_to_parquet"]


if hasattr(pa.Table, "from_pydict"):
    # only available in pyarrow >= 0.14.0
    pa_table_from_pydict = pa.Table.from_pydict
else:
    def pa_table_from_pydict(mapping):
        names = []
        arrays = []
        for k, v in mapping.items():
            names.append(k)
            arrays.append(pa.array(v))
        return pa.Table.from_arrays(arrays, names)


def convert_cat_to_parquet(reader,
                           output_filename=None,
                           columns=None,
                           include_native=False,
                           partition=False,
                           **kwargs):
    """Write a catalog in GCRCatalogs out to a Parquet file

    Parameters
    ----------
    reader : str
        GCR reader to use. Must match an existing yaml file.

    Other Parameters
    ----------------
    output_filename : str, optional
        If None, then will be constructed as '<reader>.parquet'
    columns : list, optional
        Columns to be stored in the parquet file.
        If None (default), store all columns (see also `include_native`)
    include_native : bool, optional (default: False)
        Include the native quantities from the GCR reader class
        in addition to the standardized derived quantities.
    partition : bool, optional (default: False)
        If true, save each chunk as a separate file
    **kwargs
        Any other keyword arguments will be passed to `config_overwrite` when loading the catalog
    """

    if isinstance(reader, BaseGenericCatalog):
        cat = reader
    else:
        config_overwrite = dict(use_cache=False, **kwargs)
        cat = GCRCatalogs.load_catalog(reader, config_overwrite=config_overwrite)

    is_tract_catalog = isinstance(cat, DC2DMTractCatalog)

    if not columns:
        columns = cat.list_all_quantities(include_native=include_native)
        if is_tract_catalog and not include_native:
            for col in ("tract", "patch"):
                if col not in columns and cat.has_quantity(col):
                    columns.append(col)

    # Check all column names are unique after sanitized
    columns = sorted(columns)
    columns_sanitized = {str(col).lower(): col for col in columns}
    new_columns = list(columns_sanitized.values())
    for col in columns:
        if col not in new_columns:
            warnings.warn(
                "Column name `{0}` collides with `{1}` after sterilized; `{0}` will not be included.".format(
                    col, columns_sanitized[str(col).lower()]
                )
            )
    columns = new_columns
    del columns_sanitized, new_columns

    def chunk_data_generator():
        for data in cat.get_quantities(columns, return_iterator=True):
            table = pa_table_from_pydict(data)
            del data
            try:
                cat.close_all_file_handles()
            except (AttributeError, TypeError):
                pass
            yield table

    if output_filename is None:
        output_filename = str(reader) + '{}.parquet'
    elif '{}' not in output_filename:
        if output_filename.endswith('.parquet'):
            output_filename = output_filename[:-8] + '{}.parquet'
        else:
            output_filename = output_filename + '{}'

    chunk_iter = chunk_data_generator()

    if partition:
        for i, table in enumerate(chunk_iter):
            if is_tract_catalog:
                output_filename_this = output_filename.format('_tract{}'.format(table.column('tract')[0]))
            else:
                output_filename_this = output_filename.format('_chunk{}'.format(i))
            with pq.ParquetWriter(output_filename_this, table.schema, flavor='spark') as pqwriter:
                pqwriter.write_table(table)

    else:
        if is_tract_catalog and kwargs.get('tract'):
            output_filename_this = output_filename.format('_tract{}'.format(kwargs['tract']))
        elif kwargs.get('healpix_pixels'):
            output_filename_this = output_filename.format('_healpix{}'.format(kwargs['healpix_pixels'][0]))
        else:
            output_filename_this = output_filename.format('')

        table = next(chunk_iter)
        with pq.ParquetWriter(output_filename_this, table.schema, flavor='spark') as pqwriter:
            pqwriter.write_table(table)
            for table in chunk_iter:
                pqwriter.write_table(table)


def main():
    usage = """
Produce parquet output file from a catalog within GCRCatalogs. Requires pyarrow.

For example, to produce an output Parquet file from the 'dc2_object_run2.2i_dr3' GCR catalog:

  python %(prog)s dc2_object_run2.2i_dr3

By default the output name will be 'dc2_object_run2.2i_dr3.parquet'.
You could specify a different one on the command line:

  python %(prog)s dc2_object_run2.2i_dr3 --output_filename dc2_object_test.parquet

For catalogs that use a reader that is based on GCRCatalogs.DC2DMTractCatalog (e.g., newer object catalogs),
one can specify a tract to process:

    python %(prog)s dc2_object_run2.2i_dr3 --tract 3830

This would only process one tract and produce the file 'dc2_object_run2.2i_dr3_tract3830.parquet'

"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('reader', help='Catalog (name as in GCRCatalogs) to read.')
    parser.add_argument('--output_filename', help='Output filename. Default to <reader>.parquet')
    parser.add_argument('--include_native', action='store_true',
                        help='Include the native quantities along with the derived GCR quantities')
    parser.add_argument('--partition', action='store_true', help='Store each chunk as a separate file')
    parser.add_argument('--tract', type=int, help='tract to process')
    parser.add_argument('--healpix', type=int, nargs=1, dest='healpix_pixels', help='healpix to process (for cosmoDC2)')

    convert_cat_to_parquet(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
