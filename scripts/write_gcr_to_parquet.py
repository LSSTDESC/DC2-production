#!/usr/bin/env python

"""
Write a catalog in GCRCatalogs out to a Parquet file
"""
import warnings
import multiprocessing as mp
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


def _chunk_data_generator(cat, columns, native_filters=None, **kwargs):
    for data in cat.get_quantities(columns, native_filters=native_filters, return_iterator=True):
        table = pa_table_from_pydict(data)
        del data
        try:
            cat.close_all_file_handles()
        except (AttributeError, TypeError):
            pass
        yield table


def _write_one_partition(kwargs):
    my_print = (lambda *x: None) if kwargs.get("silent") else print
    output_path = kwargs["output_path"]
    my_print("Generating", output_path)
    chunk_iter = _chunk_data_generator(**kwargs)
    table = next(chunk_iter)
    with pq.ParquetWriter(output_path, table.schema, flavor='spark') as pqwriter:
        pqwriter.write_table(table)
        for table in chunk_iter:
            pqwriter.write_table(table)
    my_print("Done with", output_path)


def convert_cat_to_parquet(reader,
                           output_filename=None,
                           columns=None,
                           include_native=False,
                           partition=False,
                           n_cores=1,
                           silent=False,
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
    my_print = (lambda *x: None) if silent else print

    if isinstance(reader, BaseGenericCatalog):
        cat = reader
    else:
        my_print("Loading", reader, "from GCRCatalogs")
        config_overwrite = dict(use_cache=False, **kwargs)
        cat = GCRCatalogs.load_catalog(reader, config_overwrite=config_overwrite)

    my_print("Checking if all column names exist and are unique")

    if columns:
        if not cat.has_quantities(columns):
            raise ValueError("Not all columns available in {}".format(reader))
    else:
        columns = cat.list_all_quantities(include_native=include_native)
        if isinstance(cat, DC2DMTractCatalog) and not include_native:
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
                "Column name `{0}` collides with `{1}` after sanitized; `{0}` will not be included.".format(
                    col, columns_sanitized[str(col).lower()]
                )
            )
    columns = new_columns
    del columns_sanitized, new_columns

    my_print("Determining partition scheme")
    partition_values = None
    if partition:
        if isinstance(cat, DC2DMTractCatalog):
            partition = "tract"
            partition_values = sorted(cat.available_tracts)
            filename_postfix = "_tract{}"
        elif "healpix_pixel" in cat.native_filter_quantities and hasattr(cat, "_file_list"):
            partition = "healpix_pixel"
            partition_values = sorted((k[1] for k in cat._file_list))
            filename_postfix = "_healpix{}"
        else:
            partition = "iter"
            filename_postfix = "_chunk{}"
    else:
        filename_postfix = ""

    my_print("Partition scheme is", partition)
    if partition_values:
        my_print("Partition values are", partition_values)

    # Format output filename
    if output_filename is None:
        output_filename = str(reader) + filename_postfix + '.parquet'
    elif '{}' not in output_filename:
        if output_filename.endswith('.parquet'):
            output_filename = output_filename[:-8] + filename_postfix + '.parquet'
        else:
            output_filename = output_filename + filename_postfix

    if not partition:
        _write_one_partition(dict(cat=cat, columns=columns, output_path=output_filename))

    elif partition == "iter":
        for i, table in enumerate(_chunk_data_generator(cat, columns)):
            output_path = output_filename.format(i)
            my_print("Generating", output_path)
            with pq.ParquetWriter(output_filename.format(i), table.schema, flavor='spark') as pqwriter:
                pqwriter.write_table(table)
            my_print("Done with", output_path)

    elif partition_values:
        kwargs_array = []
        for value in partition_values:
            kwargs_this = dict(
                cat=cat,
                columns=columns,
                native_filters="{} == {}".format(partition, value),
                output_path=output_filename.format(value),
                silent=silent,
            )
            kwargs_array.append(kwargs_this)

        n_cores = max(n_cores, 1)
        with mp.Pool(n_cores) as pool:
            pool.map(_write_one_partition, kwargs_array)

    else:
        raise ValueError("Unknown partition scheme")


def main():
    usage = """
Produce parquet output file from a catalog within GCRCatalogs. Requires pyarrow.

To produce a single Parquet file from the 'dc2_object_run2.2i_dr3' GCR catalog:

  python %(prog)s dc2_object_run2.2i_dr3

By default the output name will be 'dc2_object_run2.2i_dr3.parquet'.
You could specify a different one on the command line:

  python %(prog)s dc2_object_run2.2i_dr3 --output dc2_object_test.parquet

For catalogs that use a reader that is based on GCRCatalogs.DC2DMTractCatalog (e.g., newer object catalogs),
one can specify a subset of tracts to process. Use --partition to put each tract in one file.

    python %(prog)s dc2_object_run2.2i_dr3 --tracts 3830 3831 --partition

This would process only two tracts and produce two files:
'dc2_object_run2.2i_dr3_tract3830.parquet' and 'dc2_object_run2.2i_dr3_tract3831.parquet'

If you want to process all tracts, but create one output file per tract, use just --partition.
You can optionally specify --n-cores to speed things up.
Because this is an I/O intensive work, it is *not* recommended that you use too many cores at once.

   python %(prog)s dc2_object_run2.2i_dr3 --partition --n-cores=4

If you are working with cosmoDC2, replace "tract" with "healpix" and the above instructions still apply.

"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('reader', help='Catalog (name as in GCRCatalogs) to read.')
    parser.add_argument('--output-filename', help='Output filename. Default to <reader>.parquet')
    parser.add_argument('--include_native', action='store_true',
                        help='Include the native quantities along with the derived GCR quantities')
    parser.add_argument('--partition', action='store_true', help='Store each chunk as a separate file')
    parser.add_argument('--tracts', type=int, nargs="+", help='Limiting tracts to process (for tract catalogs)')
    parser.add_argument('--healpix-pixels', type=int, nargs="+", help='Limiting healpix pixels to process (for cosmoDC2)')
    parser.add_argument("--n-cores", type=int, default=1)

    convert_cat_to_parquet(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
