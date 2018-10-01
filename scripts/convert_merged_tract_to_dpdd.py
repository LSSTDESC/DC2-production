#!/usr/bin/env python

"""Take merged_tract HDF trim cat file and convert to a DPDD version.

Output in HDF, FITS, or Parquet"""

import sys

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


def convert_tract_to_dpdd_and_save(tract, reader='dc2_coadd_run1.1p'):
    """Use the GCR reader to load and save DPDD columns for a given tract.

    This is done in one function because we're doing this chunk-by-chunk.
    There is probably some more functional perpsective that might regain some modularity.
    """
    cat = load_catalog(tract, reader=reader)
    columns = cat.list_all_native_quantities()
    tracts_and_patches = cat.available_tracts_and_patches

    # tract is an int
    # but patch is a string (e.g., '01' for '0,1')
    outfile_format = '{base}_tract_{tract:04d}_patch_{patch:s}.{suffix}'
    for tp in tracts_and_patches:
        tract = tp['tract']
        # Reformat '0,1'->'01'
        patch = ''.join(tp['patch'].split(','))
        print(['tract == {tract:04d}'.format(**tp),
               'patch == "{patch:s}"'.format(**tp)])

        tract_patch_filter = [
            GCRQuery('tract == {tract:04d}'.format(**tp)),
            GCRQuery('patch == "{patch:s}"'.format(**tp))]
        print(tract_patch_filter)
        quantities = cat.get_quantities(columns, native_filters=tract_patch_filter)
        df = pd.DataFrame.from_dict(q)

        outfile = outfile_format.format(base='dpdd_object', suffix='hdf5',
                                        **tp)
        df.to_hdf(outfile)


def save_trim_cat_to_hdf(tract, outfile):
    df_iterator = convert_columns_to_dpdd(tract)
    for df in df_iterator:
        print(df)
        df.to_hdf(outfile)


def save_trim_cat_to_fits(tract, outfile):
    df = convert_columns_to_dpdd(tract)
    cat = Table(df)
    cat.write(outfile)


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
