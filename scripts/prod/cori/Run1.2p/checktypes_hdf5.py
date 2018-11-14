import sys,os,glob
import numpy as np
import pandas as pd

def sameTypes(t1,t2):
    if not t1.index.size == t2.index.size :
        print("not same size")
        return False
    for n in t1.index:
        s1=str(t1[n])
        s2=str(t2[n])
        if not s1==s2:
            print("different types for {}: {} vs {}".format(n,s1,s2))
            return False
    print("all types checked: OK")
    return True


ff=glob.glob("dpdd_object_tract_*.hdf5")

print("ref will be {}".format(ff[0]))

store = pd.HDFStore(ff[0],'r')
keys = store.keys()
df_ref=store.get(keys[0])
tref=df_ref.dtypes
store.close()

for fin in ff :
    print("comparing with {}".format(fin))
    store = pd.HDFStore(fin,'r')
    keys = store.keys()
    for k in keys:
        print(k)
        df=store.get(k)
        assert(sameTypes(tref,df.dtypes))
    store.close()


#df_ref['I_flag']=df_ref['I_flag'].astype(float)
