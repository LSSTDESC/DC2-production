#!/usr/bin/env python

"""
Write a catalog in GCRCatalogs out to a Parquet file
"""
import warnings
import time
import os
from argparse import ArgumentParser, RawTextHelpFormatter
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq

import GCRCatalogs
from GCRCatalogs import BaseGenericCatalog
from GCRCatalogs.dc2_dm_catalog import DC2DMTractCatalog

__all__ = ["convert_cat_to_parquet"]


class Checkpoint():
    def __init__(self, path, checkpoint_dir=None):
        if checkpoint_dir is None:
            self._checkpoint_lock = self._checkpoint_done = None
            self.has_run = self._has_run_no_lock
            self.run = self._run_no_lock
        else:
            checkpoint_base = os.path.join(checkpoint_dir, os.path.basename(str(path)))
            self._checkpoint_lock = checkpoint_base + ".lock"
            self._checkpoint_done = checkpoint_base + ".done"
            self.has_run = self._has_run
            self.run = self._run

    @contextmanager
    def _run(self):
        with open(self._checkpoint_lock, "w"):
            pass
        try:
            yield
        except:  # noqa: E722
            raise
        else:
            with open(self._checkpoint_done, "w"):
                pass
        finally:
            try:
                os.unlink(self._checkpoint_lock)
            except FileNotFoundError:
                pass

    @contextmanager
    def _run_no_lock(self):
        yield

    def _has_run(self):
        return os.path.isfile(self._checkpoint_done) or os.path.isfile(self._checkpoint_lock)

    def _has_run_no_lock(self):
        return False


def _chunk_data_generator(cat, columns, native_filters=None):
    columns = sorted(columns)
    for data in cat.get_quantities(columns, native_filters=native_filters, return_iterator=True):
        # fix some data types
        bad_fields = {"lightcone_replication": ["int64", "int32"],
              "lightcone_rotation": ["int64", "int32"],
              "baseDC2/source_halo_mvir": ["float32", "float64"]}
        for field in bad_fields:
            if field in data and data[field].dtype == bad_fields[field][0] :
                print("Fix dtype for field ", field)
                data[field] = data[field].astype(bad_fields[field][1])
        table = pa.Table.from_arrays([pa.array(data[col]) for col in columns], columns)
        del data
        try:
            cat.close_all_file_handles()
        except (AttributeError, TypeError):
            pass
        yield table


def _write_one_parquet_file(
    output_path,
    cat=None,
    get_quantities_kwargs=None,
    schema=None,
    return_schema=False,
    silent=False,
    checkpoint_dir=None
):
    my_print = (lambda *x: None) if silent else print

    checkpoint = Checkpoint(output_path, checkpoint_dir)

    if checkpoint.has_run():
        my_print("Skipping", output_path, " - checkpoint exists!")
        return

    with checkpoint.run():
        my_print("Generating", output_path, time.strftime("[%H:%M:%S]"))
        if not get_quantities_kwargs:
            if schema is None:
                schema = cat.schema
            with pq.ParquetWriter(output_path, schema, flavor='spark') as pqwriter:
                pqwriter.write_table(cat)
        else:
            chunk_iter = _chunk_data_generator(cat, **get_quantities_kwargs)
            if schema is None:
                table = next(chunk_iter)
                schema = table.schema
            else:
                table = None
            with pq.ParquetWriter(output_path, schema=schema, flavor='spark') as pqwriter:
                if table is not None:
                    pqwriter.write_table(table)
                for table in chunk_iter:
                    schema_table = table.schema
                    if schema_table != schema :
                        print("******* Schema mismatch",schema_table)
                        print("******* ", schema_table["baseDC2/source_halo_mvir"], schema_table["lightcone_replication"], schema_table["lightcone_rotation"])
                    pqwriter.write_table(table)
        my_print("Done with", output_path, time.strftime("[%H:%M:%S]"))

    if return_schema:
        return schema


def convert_cat_to_parquet(reader,
                           output_filename=None,
                           columns=None,
                           include_native=False,
                           partition=False,
                           checkpoint_dir=None,
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
    checkpoint_dir : str, optional
        Path to a directory that can store checkpoints. If none, do not use checkpoints.
    silent : bool, optional
        Suppress print outs.
    **kwargs
        Any other keyword arguments will be passed to `config_overwrite` when loading the catalog
    """
    my_print = (lambda *x: None) if silent else print

    # make sure checkpoint dir is writable
    if checkpoint_dir is not None:
        os.makedirs(checkpoint_dir, exist_ok=True)
        if not os.access(checkpoint_dir, os.R_OK + os.W_OK):
            raise ValueError(checkpoint_dir, "not writable!")

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
            partition_values = sorted(set((k[1] for k in cat._file_list)))
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
    my_print("Output path pattern is", output_filename)

    if not partition:
        _write_one_parquet_file(
            output_path=output_filename,
            cat=cat,
            get_quantities_kwargs=dict(columns=columns),
            silent=silent,
            checkpoint_dir=checkpoint_dir,
        )

    elif partition == "iter":
        schema = None
        for i, table in enumerate(_chunk_data_generator(cat, columns)):
            schema = _write_one_parquet_file(
                output_path=output_filename.format(i),
                cat=table,
                schema=schema,
                return_schema=True,
                silent=silent,
                checkpoint_dir=checkpoint_dir,
            )

    elif partition_values:
        schema = None
        for value in partition_values:
            schema = _write_one_parquet_file(
                output_path=output_filename.format(value),
                cat=cat,
                get_quantities_kwargs=dict(columns=columns, native_filters="{} == {}".format(partition, value)),
                schema=schema,
                return_schema=True,
                silent=silent,
                checkpoint_dir=checkpoint_dir,
            )

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

If you want to process *all* tracts, but create one output file per tract, use just --partition with out --tracts:

   python %(prog)s dc2_object_run2.2i_dr3 --partition

If you are working with cosmoDC2, replace "--tracts" with "--healpix-pixels" and the above instructions still apply.

When running this script with cosmoDC2, it's useful to enable --checkpoint-dir. This will keep track of the generation
status in the checkpoint dir by creating empty files <filename>.lock and <filename>.done.
The former means the corresponding file is being generated, and the latter means the corresponding file is completed.

   python %(prog)s cosmoDC2_v1.1.4_image --partition --checkpoint-dir=/path/to/checkpoint/dir

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
    parser.add_argument("--silent", action="store_true")
    parser.add_argument("--checkpoint-dir", help="Path to a directory that contains checkpoint. Files with existing checkpoints are skipped.")

    convert_cat_to_parquet(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
