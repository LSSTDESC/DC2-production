import sys

import pandas as pd

# Write with append for Parquet is not supported as of Pandas 0.23
# So we build up a dataframe from all tracts and then write once

def load_files_into_dataframe(parquet_files):
    df = None
    for data_file in parquet_files:
        this_df = pd.read_parquet(data_file)
        if df is None:
            df = this_df
        else:
            df = df.append(this_df)

    return df


def run(input_files, output_file='dpdd_object.parquet'):
    df = load_files_into_dataframe(input_files)
    df.to_parquet(output_file)


if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Merge set of parquet files into one Parquet file.
    The schema must agree.

    Examples
    --
    python %(prog)s dpdd_object_*.parquet

    Will take the glob of `dpdd_object_*.parquet` file in the current directory
    and write an output file `dpdd_object.parquet`.

    python %(prog)s foo/dpdd_object_*.parquet --output_file bar/dpdd.parquet

    will take the glob of `foo/dpdd_object_*.parquet` and write an output file
    `bar/dpdd.parquet`.
    """

    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('input_files', type=str, nargs='+', default=[],
                        help='Space-separated list of Parquet files to merge.')
    parser.add_argument('--output_file', default='dpdd_object.parquet',
                        help='Output filepath. (default: %(default)s)')

    args = parser.parse_args(sys.argv[1:])
    run(input_files=args.input_files, output_file=args.output_file)
