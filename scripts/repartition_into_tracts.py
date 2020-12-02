#!/usr/bin/env python

"""
Repartition a parquet file into tracts
"""
from argparse import ArgumentParser, RawTextHelpFormatter

import pandas as pd

__all__ = ["repartition_into_tracts"]


def repartition_into_tracts(input_file, output_root_dir=None, **kwargs):
    pass


def main():
    usage = """
TODO
"""
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('input_file', help='Parquet file to read.')
    parser.add_argument('--output-root-dir', default='.')

    repartition_into_tracts(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
