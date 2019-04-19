
import os
import sys
import numpy as np
import pandas as pd
import healpy as hp
from time import time

sys.path.insert(0,'/global/homes/p/plaszczy/DC2/gcr-catalogs')
import GCRCatalogs

#version GCR
print('GCRCatalogs =', GCRCatalogs.__version__, '|' ,'GCR =', GCRCatalogs.GCR.__version__)
#liste des catalogues
print('\n'.join(sorted(GCRCatalogs.get_available_catalogs())))


cat='dc2_truth_run1.2_static'

gc = GCRCatalogs.load_catalog(cat)


#data=gc.get_quantities([q for q in gc.list_all_quantities()],native_filters=['healpix_pixel == 9813'])

#copy all quantities (no filter)
#cols=gc.list_all_quantities(include_native=True)
cols=['object_id', 'ra', 'dec', 'redshift', 'healpix_2048','agn', 'star', 'sprinkled', 'u', 'g', 'r', 'i', 'z', 'y']
print(cols)

#output
parquet_file=cat+"_hive.parquet"


#pixels list
nside=2048
pix=range(12*nside**2)

for ipix in pix:
    print(ipix)
    t0=time()
    data=gc.get_quantities(cols,native_filters=["healpix_2048 == {}".format(ipix)])
    if len(data['ra']==0):
        continue
    df=pd.DataFrame(data)
    t1=time()
    print("Read {}M data: {:2.1f}s".format(df.index.size/1e6,t1-t0))
    #convert float64 columns to float32
    for n in cols:
        if str(df.dtypes[n])=='float64':
            df[n]=df[n].astype('float32') 
    #writing
    df.to_parquet(parquet_file,append=os.path.exists(parquet_file),
                      file_scheme='hive',engine='fastparquet',compression=None)
    t2=time()
    print("Wrote to {}".format(parquet_file))
    print("Tot time to process {:2.1f}s".format(t2-t0))
