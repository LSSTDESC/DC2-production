"""
merge_parquet_files.py
"""

import pandas as pd

try:
    import pyarrow.parquet as pq
except ImportError:
    _HAS_PYARROW_ = False
else:
    _HAS_PYARROW_ = True


def load_parquet_files_into_dataframe(parquet_files):
    return pd.concat(
        [pd.read_parquet(f) for f in parquet_files],
        axis=0,
        ignore_index=True,
    )


def run(input_files, output_file, sort_input_files=False,
        parquet_engine='pyarrow', assume_consistent_schema=False):
    if sort_input_files:
        input_files = sorted(input_files)

    if assume_consistent_schema:
        if parquet_engine != "pyarrow" or not _HAS_PYARROW_:
            raise ValueError("Must use/have pyarrow when assume_consistent_schema is set to True")
        if not input_files:
            raise ValueError("No input files to merge")

        t = pq.read_table(input_files[0])
        with pq.ParquetWriter(output_file, t.schema, flavor='spark') as pqwriter:
            pqwriter.write_table(t)
            for input_file in input_files[1:]:
                t = pq.read_table(input_file)
                pqwriter.write_table(t)

    else:
        df = load_parquet_files_into_dataframe(input_files)
        df.to_parquet(
            output_file,
            engine=parquet_engine,
            compression=None,
            index=False,
        )


if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Merge set of parquet files into one Parquet file.
    The schema must agree.

    Examples
    --
    python %(prog)s object_4850_*.parquet -o bar/object_tract_4850.parquet --sort-input-files

    Will take the glob of `object_4850_*.parquet` file in the current directory
    merge them, and write an output file `bar/object_tract_4850.parquet`.
    """

    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('input_files', type=str, nargs='+', default=[],
                        help='Space-separated list of Parquet files to merge.')
    parser.add_argument('-o', '--output-file', default='merged.parquet',
                        help='Output filepath. (default: %(default)s)')
    parser.add_argument('-s', '--sort-input-files', action='store_true',
                        help='Sort input files')
    parser.add_argument('--parquet_engine', default='pyarrow',
                        choices=['fastparquet', 'pyarrow'],
                        help="""(default: %(default)s)""")
    parser.add_argument('--assume-consistent-schema', action='store_true',
                        help='Assume schema is consistent across input files')
    args = parser.parse_args()

    if not args.input_files:
        parser.error('Must provide input files')
    run(**vars(args))
