#!/usr/bin/env python

"""
Write a catalog in GCRCatalogs out to a Parquet file
"""
from argparse import ArgumentParser, RawTextHelpFormatter

import pyarrow as pa
import pyarrow.parquet as pq

import GCRCatalogs

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
    **kwargs
        Any other keyword arguments will be passed to `config_overwrite` when loading the catalog
    """

    if output_filename is None:
        output_filename = str(reader)
        if kwargs.get('tract'):
            output_filename += '_tract{}'.format(kwargs['tract'])
        output_filename += '.parquet'

    config_overwrite = dict(use_cache=False, **kwargs)
    cat = GCRCatalogs.load_catalog(reader, config_overwrite=config_overwrite)

    if not columns:
        columns = cat.list_all_quantities(include_native=include_native)
        if not include_native:
            for col in ("tract", "patch"):
                if cat.has_quantity(col):
                    columns.append(col)

    def chunk_data_generator():
        for data in cat.get_quantities(columns, return_iterator=True):
            table = pa_table_from_pydict(data)
            del data
            try:
                cat.close_all_file_handles()
            except (AttributeError, TypeError):
                pass
            yield table

    chunk_iter = chunk_data_generator()
    table = next(chunk_iter)

    with pq.ParquetWriter(output_filename, table.schema, flavor='spark') as pqwriter:
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
    parser.add_argument('--tract', type=int, help='tract to process')

    convert_cat_to_parquet(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
