#!/usr/bin/env python

"""Take merged_tract HDF trim cat file and convert to a DPDD version.

Output in HDF, FITS, or Parquet"""

import sys

import numpy as np
import pandas as pd


def convert_trim_cat_to_dpdd(infile, outfile, clobber=True):
    cat = pd.read_hdf(infile)
    
    # Remove existing outputfile
 #   if clobber:
 #       if os.path.exists(outfile):
 #           os.remove(outfile)

    cat.to_hdf(outfile)


if __name__ == "__main__":
    import sys

    write_hdf = True

    write_fits = False
    write_parquet = False

    infile = sys.argv[1]
    outfile = sys.argv[2]
    
    if outfile == "":
        print("No outfile specified.")
        sys.exit()
        
    if write_fits:
        convert_hdf_to_fits