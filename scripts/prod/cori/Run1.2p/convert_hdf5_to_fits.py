import glob
import pandas as pd
from astropy.table import Table


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
    if not df1.dtypes.index.size == df2.dtypes.index.size :
        print("not same size")
        return False
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
print(ff)
input()

for fin in ff :
    store = pd.HDFStore(fin,'r')
    keys = store.keys()
    print("{}, #patches={}".format(fin,len(keys)))
    #first key is the reference
    df_ref=store.get(keys[0])
    dfs=list(df_ref)
    
    for k in keys[1:]:
#        print(k)
        df=store.get(k)
        if not sameTypes(df_ref,df):
            print("WARNING!!!!! inconsistent types in {} {}".format(fin,k))
        dfs.append(df)

    dftot= pd.concat(dfs, ignore_index=True)
    print("writing {}".format(fout))
    fout=fin.replace(".hdf5",".fits")
    Table.from_pandas(dftot).write(fout)
    store.close()
