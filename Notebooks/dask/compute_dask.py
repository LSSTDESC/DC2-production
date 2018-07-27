import os

import numpy as np


from datetime import datetime

import dask as da
import dask.dataframe as dd

tract = 4850

base_dir = '/global/projecta/projectdirs/lsst/global/in2p3/Run1.1/summary'

datafile = os.path.join(base_dir, 'table_trim_merged_tract_%d.hdf5' % tract)
datafile_pattern = os.path.join(base_dir, 'table_trim_merged_tract_*.hdf5')

da_df = dd.read_hdf(datafile, key='coadd_*', mode='r')
da_df_all = dd.read_hdf(datafile_pattern, key='coadd_*', mode='r')

df2 = np.mean(da_df['g_mag'] - da_df['r_mag'])
df2_all = np.mean(da_df_all['g_mag'] - da_df_all['r_mag'])

print(datetime.now())

df2_mean = df2.compute()
df2_all_mean = df2_all.compute()
print(df2_mean)
print(df2_all_mean)

print(datetime.now())
