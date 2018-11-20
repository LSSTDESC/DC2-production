import os,glob
import numpy as np
import pandas as pd
from astropy.table import Table
from pandas.api.types import is_numeric_dtype

def correctNans(n,df1,df2):
    s1=str(df1.dtypes[n])
    s2=str(df2.dtypes[n])
    if s1=='bool' and s2=='float64':
        mask=np.isnan(df2[n])
        if sum(mask)==len(df2[n]):
            df2[n]=~df2[n].astype(bool) 
            print("OK: corrected Nan only column {} to False".format(n))
            return True
    if s2=='bool' and s1=='float64':
        mask=np.isnan(df1[n])
        if sum(mask)==len(df1[n]):
            df1[n]=~df1[n].astype(bool) 
            print("OK: corrected Nan only column {} to False".format(n))
            return True
    return False


#check datatypes are all consistent
def sameTypes(df1,df2):
    assert(df1.dtypes.index.size == df2.dtypes.index.size)

    for n in df1.dtypes.index:
        s1=str(df1.dtypes[n])
        s2=str(df2.dtypes[n])
        if not s1==s2:
            print("different types for {}: {} vs {}".format(n,s1,s2))
            assert(correctNans(n,df1,df2))
    #print("all types checked: OK")
    return True


ff=glob.glob("dpdd_object_tract_*.hdf5")
print("about to run on {} files".format(len(ff)))

#define ref
_ref=('dpdd_object_tract_4850.hdf5','/object_4850_44')
df_ref=pd.read_hdf(_ref[0],_ref[1])
print("ref schema from {}{}".format(_ref[0],_ref[1]))
#check no NAN-only column!
for n in df_ref.dtypes.index:
    if is_numeric_dtype(df_ref[n]):
        nans=np.isnan(df_ref[n])
        assert(sum(nans)<len(df_ref[n]))

#
overwrite=False
singleOutput=True

for fin in ff :
    fout=fin.replace(".hdf5",".parquet")
    #skip if file exists
    if not singleOutput and not overwrite:
        if os.path.exists(fout):
            print("{} exists-> skipping".format(fout))
            continue
    dfs=[]
    store = pd.HDFStore(fin,'r')
    keys = store.keys()
    print("{}, #patches={}".format(fin,len(keys)))
    for k in keys:
        print(k)
        df=store.get(k)
        if not sameTypes(df_ref,df):
            print("WARNING!!!!! inconsistent types in {} {}".format(fin,k))
        # sort col names for fits writing always the same way
        df=df[df_ref.dtypes.index]
        dfs.append(df)

    dftot= pd.concat(dfs, ignore_index=True)
    if singleOutput:
        fout="full_catalog_simple_uncompressed.parquet"
        print("appending to {}".format(fout))
        append=os.path.exists(fout)
        dftot.to_parquet(fout,append=append,file_scheme='simple',engine='fastparquet',compression=None)
    else:
        print("writing {}".format(fout))
        dftot.to_parquet(fout)
    store.close()
