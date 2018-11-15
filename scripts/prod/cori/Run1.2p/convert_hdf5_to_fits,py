import sys,os,glob
import numpy as np
import pandas as pd

from astropy.table import Table


tnum=sys.argv[1]

fin="dpdd_object_tract_"+tnum+".hdf5"
fout="dpdd_object_tract_"+tnum+".fits"

store = pd.HDFStore(fin,'r')
keys = store.keys()

#df=pd.read_hdf("test.hdf5")
var1='mag_r'

print("{}, #patches={}".format(fin,len(keys)))

dfs=[]
totsize=0
for k in keys:
    print(k)
    df=store.get(k)
    cols=df.columns
    totsize+=df.index.size
    dfs.append(df)

dftot= pd.concat(dfs, ignore_index=True)
store.close()
del dfs

print("N tot={} vs {} ".format(dftot.index.size,totsize))
#
#print("writing {}".format(fout))
#Table.from_pandas(dftot).write(fout)
#del dftot
