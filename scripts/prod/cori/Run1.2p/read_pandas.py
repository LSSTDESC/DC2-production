import sys,os,glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pandas.api.types import is_numeric_dtype

#is_numeric_dtype(df['B'])
#mask1=~np.isnan(df[var1])

zename=sys.argv[1]

store = pd.HDFStore(zename,'r')
keys = store.keys()

#df=pd.read_hdf("test.hdf5")

var1='r_mag'
#var2='r_modelfit_mag'

print("{}, #patches={}".format(zename,len(keys)))

print("key\t\tvar\tN\tvalid")
len1=[]
frac1=[]
for k in keys:
    df=store.get(k)
    cols=df.columns
    if not var1 in cols:
        print("no {} column!".format(var1))
        continue
    mask1=~np.isnan(df[var1])
    len1.append(len(mask1))
    frac1.append(sum(mask1)/len1[-1])
    print("{}\t{}\t{}\t{:3.1f}".format(k,var1,len1[-1],frac1[-1]*100))

    #mask2=~np.isnan(df[var2])
    #mask=mask1&mask2
