import glob
import pandas as pd
from astropy.table import Table

#check datatypes are all consistent
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
    #print("all types checked: OK")
    return True


ff=glob.glob("dpdd_object_tract_*.hdf5")
print("about to run on {} files".format(len(ff)))
print(ff)
input()

for fin in ff :
    store = pd.HDFStore(fin,'r')
    keys = store.keys()
    print("{}, #patches={}".format(fin,len(keys)))
    #first key is the reference
    df_ref=store.get(keys[0])
    tref=df_ref.dtypes
    dfs=list(df_ref)
    
    for k in keys[1:]:
#        print(k)
        df=store.get(k)
        if not sameTypes(tref,df.dtypes)):
            print("WARNING!!!!! inconsistent types in {} {}".format(fin,k))
        dfs.append(df)

    dftot= pd.concat(dfs, ignore_index=True)
    print("writing {}".format(fout))
    fout=fin.replace(".hdf5",".fits")
    Table.from_pandas(dftot).write(fout)
    store.close()
