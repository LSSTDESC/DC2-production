#!/usr/bin/env python

"""Take merged_tract HDF trim cat file and convert to a DPDD version.

Output in HDF, FITS, or Parquet"""

import sys

import numpy as np
import pandas as pd

import GCRCatalogs
# Load the coadd catalog
catalog = GCRCatalogs.load_catalog('dc2_coadd_run1.1p')

def load_catalog(tract, reader='dc2_coadd_run1.1p'):
    ### load catalog
    config = {}

    trim_config = config.copy()
    trim_config['filename_pattern'] = r'trim_merged_tract_\d+\.hdf5$'

    trim_thistract_config = config.copy()
    trim_thistract_config['filename_pattern'] = 
        'trim_merged_tract_{:04d}\.hdf5$'.format(tract)

    return GCRCatalogs.load_catalog(reader, config)


def convert_columns_to_dpdd(tract, reader='dc2_coadd_run1.1p'):
    """Use the GCR reader to do this.

    Question:  Should we use GCR to actually just do the conversion?
    """
    cat = load_catalog(tract, reader=reader)
    


def convert_trim_cat_to_dpdd(infile, outfile, clobber=True):
    cat = pd.read_hdf(infile)
    
    # Remove existing outputfile
    if clobber:
        if os.path.exists(outfile):
            os.remove(outfile)

    cat.to_hdf(outfile)


if __name__ == "__main__":
    import sys

    clobber = True
    write_hdf = True

    write_fits = False
    write_parquet = False

    infile = sys.argv[1]
    outfile = sys.argv[2]
    
    if outfile == "":
        print("No outfile specified.")
        sys.exit()
        
    if write_hdf:
        convert_trim_cat_to_dpdd(infile, outfile, clobber=clobber)
