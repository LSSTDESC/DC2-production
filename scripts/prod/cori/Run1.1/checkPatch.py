import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


df=pd.read_hdf("test.hdf5")

var1='r_mag'
var2='r_modelfit_mag'

mask1=~np.isnan(df[var1])
mask2=~np.isnan(df[var2])
mask=mask1&mask2

print("{} valid fraction={}".format(var1,sum(mask1)/len(mask1)))
print("{} valid fraction={}".format(var2,sum(mask2)/len(mask2)))
plt.plot(df[var1][mask])
plt.plot(df[var2][mask])

plt.show()
