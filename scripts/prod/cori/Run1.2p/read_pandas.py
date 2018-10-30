import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pandas.api.types import is_numeric_dtype

#is_numeric_dtype(df['B'])
#mask1=~np.isnan(df[var1])

store = pd.HDFStore("test.hdf5",'r')
keys = store.keys()

#first key
df=store.get(keys[0])
#df=pd.read_hdf("test.hdf5")

cols=df.columns

var1='r_mag'
var2='r_modelfit_mag'

if not var1 in cols:
    print("no {} column!".format(var1))
if not var2 in cols:
    print("no {} column!".format(var2))



mask1=~np.isnan(df[var1])
mask2=~np.isnan(df[var2])
mask=mask1&mask2

print("{} valid fraction={}".format(var1,sum(mask1)/len(mask1)))
print("{} valid fraction={}".format(var2,sum(mask2)/len(mask2)))
plt.plot(df[var1][mask])
plt.plot(df[var2][mask])
plt.show()
