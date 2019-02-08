#!/usr/bin/env python

import os
import sys
import re
import warnings

import pandas as pd

from GCRCatalogs import BaseGenericCatalog
from GCRCatalogs.dc2_object import DC2ObjectCatalog, GROUP_PATTERN


class DummyDC2ObjectCatalog(BaseGenericCatalog):
    """
    A dummy reader class that can be used to generate all native quantities
    required for the DPDD columns in DC2 Object Catalog
    """
    def __init__(self, schema_version=None):
        self._quantity_modifiers = DC2ObjectCatalog._generate_modifiers(dm_schema_version=schema_version)

    @property
    def required_native_quantities(self):
        """
        the set of native quantities that are required by the quantity modifiers
        """
        return set(self._translate_quantities(self.list_all_quantities()))


def load_trim_save_patch(outfile, infile_handle, key, columns_to_keep):
    df = infile_handle.get_storer(key).read()
    if not columns_to_keep.issubset(df.columns):
        warnings.warn('Not all columns to keep are present in the data file.')
    columns_to_keep_present = list(columns_to_keep.intersection(df.columns))
    trim_df = df[columns_to_keep_present]
    trim_df.to_hdf(outfile, key=key)


def make_trim_file(infile, output_file=None, output_dir=None,
                   clobber=True, schema_version=None,
                   check_all_patches_exist=False):

    if output_file is None:
        if output_dir is None:
            output_dir = os.path.dirname(infile)
        basename = os.path.basename(infile)
        output_file = os.path.join(output_dir, "trim_"+basename)

    # Remove existing outputfile
    if clobber and os.path.exists(output_file):
        os.remove(output_file)

    columns_to_keep = DummyDC2ObjectCatalog(schema_version).required_native_quantities

    patches = []
    with pd.HDFStore(infile, 'r') as fh:
        for key in fh:
            if not re.match(GROUP_PATTERN, key.lstrip('/')):
                continue
            load_trim_save_patch(output_file, fh, key, columns_to_keep)
            patches.append(key.rpartition('_')[-1])

    if check_all_patches_exist:
        nx, ny = 8, 8
        patches_all = set(('%d%d' % (i, j) for i in range(nx) for j in range(ny)))
        if not patches_all.issubset(patches):
            warnings.warn('Some patches do not exist!')


if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """
    Create a restricted sample of an HDF5 merged object catalog
    that contains only the columns necessary to support the DPDD
    columns exposed in the GCRCatalog DC2 reader

    Examples
    --
    python %(prog)s foo/object_catalog_48??.hdf5 --output_dir bar/

    will create `trim_object_catalog_48??.hdf5` files in directory `bar`.
    """

    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('input_files', type=str, nargs='+', default=[],
                        help='Input HDF5 files to be trimmed.')
    parser.add_argument('--schema_version', default=3,
                        help="""
The schema version of the DM tables.
v1: '_flux', '_fluxSigma'
v2: '_flux', '_fluxError'
v3: '_instFlux', '_instFluxError'
""")
    parser.add_argument('--output_dir', default='./',
                        help='Output directory.  (default: %(default)s))')

    args = parser.parse_args(sys.argv[1:])
    for infile in args.input_files:
        make_trim_file(infile,
                       schema_version=args.schema_version,
                       output_dir=args.output_dir)
