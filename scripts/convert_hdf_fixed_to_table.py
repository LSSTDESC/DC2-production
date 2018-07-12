import os
import re
import sys
import warnings

import pandas as pd
import tables


def get_keys_columns(fpath):
    columns = set()
    data_sets = list()

    groupname_re = re.compile(r'coadd_\d+_\d\d$')
    with tables.open_file(fpath, 'r') as ofile:
        for key in ofile.root._v_children:  # pylint: disable=W0212
            if not groupname_re.match(key):
                warn_msg = '{} has incorrect group names; skipped'
                warnings.warn(warn_msg.format(os.path.basename(fpath)))
                return list(), set()

            group = getattr(ofile.root, key)
            if 'axis0' not in group:
                warn_msg = '{} has incorrect hdf5 format; skipped'
                warnings.warn(warn_msg.format(os.path.basename(fpath)))
                return list(), set()

            data_sets.append((fpath, key))
            columns.update((c.decode() for c in group.axis0))

    return data_sets, columns


def convert_hdf_fixed_to_table(infile, outfile=None, clobber=True, verbose=False):
    """Convert an HDF5 file in the 'fixed' format to 'table' format."""

    if outfile is None:
        dirname = os.path.dirname(infile)
        basename = os.path.basename(infile)
        outfile = os.path.join(dirname, "table_"+basename)

    # Remove existing outputfile
    if os.path.exists(outfile):
        if clobber:
            os.remove(outfile)
        else:
            if verbose:
                print("'%s' already exists.  Skipping." % outfile)
                return

    keys, columns = get_keys_columns(infile)
    for fpath, key in keys:
        if verbose:
            print(key)
        df = pd.read_hdf(infile, key=key)
        try:
            df = pd.read_hdf(infile, key=key)
        except (TypeError, KeyError) as e:
            if verbose:
                print(key, e)
            continue

        df.to_hdf(outfile, key=key, format='table')


if __name__ == "__main__":
    verbose = False
    clobber = False
    files = sys.argv[1:]
    for file in files:
        convert_hdf_fixed_to_table(file, verbose=verbose, clobber=clobber)
