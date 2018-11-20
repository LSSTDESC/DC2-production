#!/usr/bin/env python

"""
Create a restricted sample of an HDF5 merged coadd catalog
that contains only the columns necessary to support the DPDD
columns exposed in the GCRCatalog DC2 reader
"""

import os
import sys
import re
import warnings

import pandas as pd

from GCRCatalogs import BaseGenericCatalog
from GCRCatalogs.dc2_object import DC2ObjectCatalog


class DummyDC2ObjectCatalog(BaseGenericCatalog):
    """
    A dummy reader class that can be used to generate all native quantities
    required for the DPDD columns in DC2 Object Catalog
    """
    def __init__(self, schema_version=None):
        self._quantity_modifiers = DC2ObjectCatalog._generate_modifiers(version=schema_version)

    @property
    def required_native_quantities(self):
        """
        the set of native quantities that are required by the quantity modifiers
        """
        return set(self._translate_quantities(self.list_all_quantities()))


def load_trim_save_patch(infile, outfile, patch, key_prefix='coadd',
                         verbose=False, schema_version=None):
    r = re.search('merged_tract_([0-9]+)\.', infile)
    tract = int(r[1])

    key = "%s_%s_%s" % (key_prefix, tract, patch)
    try:
        df = pd.read_hdf(infile, key=key)
    except KeyError as e:
        if verbose:
            print(e)
        return

    columns_to_keep = DummyDC2ObjectCatalog(schema_version).required_native_quantities
    if not columns_to_keep.issubset(df.columns):
        warnings.warn('Not all columns to keep are present in the data file.')
    columns_to_keep_present = list(columns_to_keep.intersection(df.columns))
    trim_df = df[columns_to_keep_present]
    trim_df.to_hdf(outfile, key=key)


def make_trim_file(infile, outfile=None, clobber=True):
    # Note '%d%d' instead of '%d,%d'
    nx, ny = 8, 8
    patches = ['%d%d' % (i, j) for i in range(nx) for j in range(ny)]

    if outfile is None:
        dirname = os.path.dirname(infile)
        basename = os.path.basename(infile)
        outfile = os.path.join(dirname, "trim_"+basename)

    # Remove existing outputfile
    if clobber:
        if os.path.exists(outfile):
            os.remove(outfile)

    for patch in patches:
        load_trim_save_patch(infile, outfile, patch)


if __name__ == "__main__":
    for infile in sys.argv[1:]:
        make_trim_file(infile)
