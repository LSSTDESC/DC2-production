#!/usr/bin/env python

"""Take merged_tract HDF trim cat file and convert to a DPDD version.

Output in HDF, FITS, or Parquet"""

import sys

from astropy.table import Table
import numpy as np
import pandas as pd

import GCRCatalogs
from GCR import GCRQuery

def load_catalog(tract, reader='dc2_coadd_run1.1p'):
    ### load catalog
    config = {}

    trim_thistract_config = config.copy()
    trim_thistract_config['filename_pattern'] = \
        'trim_merged_tract_{:04d}\.hdf5$'.format(tract)

    return GCRCatalogs.load_catalog(reader, trim_thistract_config)


def convert_tract_to_dpdd(tract, reader='dc2_coadd_run1.1p',
                          key_prefix='object',
                          verbose=True):
    """Use the GCR reader to load and save DPDD columns for a given tract.

    This is done in one function because we're doing this chunk-by-chunk.
    There is perhaps some more functional perspective that might regain some modularity.
    """
    cat = load_catalog(tract, reader=reader)
    columns = cat.list_all_quantities()
    columns.extend(['tract', 'patch'])

    # tract is an int
    # but patch is a string (e.g., '01' for '0,1')
    outfile_base_format = '{base}_tract_{tract:04d}_patch_{patch:s}'
    quantities = cat.get_quantities(columns, return_iterator=True)
    for q in quantities:
        if 'tract' not in q or len(q) < 1:
            continue

        df = pd.DataFrame.from_dict(q)

        # We implicitly know that we will chunk by tract+patch
        # So we can take the tract and patch in the first entry as the tract and patch
        tract, patch = q['tract'][0], q['patch'][0]
        patch = ''.join(patch.split(','))
        info = {'base': 'dpdd_object', 'tract': tract, 'patch': patch,
               'key_prefix': key_prefix}
        outfile_base = outfile_base_format.format(**info)

        if verbose:
            print("Writing HDF5 DPDD file for ", tract, patch)
        key = '{key_prefix:s}_{tract:04d}_{patch:s}'.format(**info)
        df.to_hdf(outfile_base+'.hdf5', key=key)

        # Can't get Parquet support on NERSC set up quite right
        # Need PyArrow or fastparquet
        # df.to_parquet(outfile_base+'.parquet')

        if verbose:
            print("Writing FITS DPDD file for ", tract, patch)
        Table.from_pandas(df).write(outfile_base+'.fits')


if __name__ == "__main__":
    import sys

    from argparse import ArgumentParser, RawTextHelpFormatter
    usage = """Convert merged_tract_cat object files to HDF5, FITS, Parquet
    files labeled with DPDD names, and restricted to only the available columns in the DPDD
    plus tract and patch for convenience.
    """
    parser = ArgumentParser(description=usage,
                            formatter_class=RawTextHelpFormatter)
    parser.add_argument('tract', type=int, nargs='+',
                        help='Skymap tract[s] to process.  Default is all.')
    parser.add_argument('--verbose', default=False, action='store_true')

    args = parser.parse_args(sys.argv[1:])
    
    for tract in args.tract:
        convert_tract_to_dpdd(tract)
