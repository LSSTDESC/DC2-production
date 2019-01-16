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
    input_files = sys.argv[1:]
    run(input_files)
