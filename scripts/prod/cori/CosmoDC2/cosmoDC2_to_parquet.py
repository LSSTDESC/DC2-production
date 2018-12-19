
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
gc = GCRCatalogs.load_catalog('cosmoDC2_v1.1.4_small')
print(gc.get_catalog_info('description'))


#data=gc.get_quantities([q for q in gc.list_all_quantities()],native_filters=['healpix_pixel == 9813'])

##cols=['halo_id','is_central','position_x','position_y','position_z','position_angle_true','ra','ra_true','dec','dec_true','redshift','redshift_true','size_true']
##filters=['u','g','r','i','z','Y']
##for f in filters:
##    s="Mag_true_{0}_lsst_z0,mag_true_{0}_lsst,mag_true_{0}_lsst_no_host_extinction,mag_{0}_lsst".format(f)
##    cols+=s.split(',')


cols=['halo_id','is_central','position_x','position_y','position_z','ra','dec','redshift','redshift_true','size_true']

print(cols)

#output
parquet_file="xyz_v1.1.4"

pix=np.loadtxt("healpix_1.4.4",unpack=True).astype('int')
nskip=0
for ipix in pix:
    ## parquet_file="xyz_{}.parquet".format(ipix)
    ## if os.path.exists(parquet_file):
    ##     nskip+=1
    ##     print("skipping {}: {}".format(parquet_file,nskip))
    ##     continue
    print(ipix)
    t0=time()
    data=gc.get_quantities(cols,native_filters=["healpix_pixel == {}".format(ipix)])
    df=pd.DataFrame(data)
    t1=time()
    print("Read {}M data: {:2.1f}s".format(df.index.size/1e6,t1-t0))
    #convert float64 columns to float32
    for n in cols:
        if str(df.dtypes[n])=='float64':
            df[n]=df[n].astype('float32') 
    #writing
    df.to_parquet(parquet_file,append=os.path.exists(parquet_file),
                      file_scheme='simple',engine='fastparquet',compression=None)
    t2=time()
    print("Wrote to {}".format(parquet_file))
    print("Tot time to process {:2.1f}s".format(t2-t0))
