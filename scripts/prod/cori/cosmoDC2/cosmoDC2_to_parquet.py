
import os
import sys
import numpy as np
import pandas as pd
import healpy as hp


sys.path.insert(0,'/global/homes/p/plaszczy/DC2/gcr-catalogs')
import GCRCatalogs


gc = GCRCatalogs.load_catalog('cosmoDC2_v1.0')

#data=gc.get_quantities([q for q in gc.list_all_quantities()],native_filters=['healpix_pixel == 9813'])

cols=['halo_id','is_central','position_x','position_y','position_z','position_angle_true','ra','ra_true','dec','dec_true','redshift','redshift_true','size_true']
filters=['u','g','r','i','z','Y']
for f in filters:
    s="Mag_true_{0}_lsst_z0,mag_true_{0}_lsst,mag_true_{0}_lsst_no_host_extinction,mag_{0}_lsst".format(f)
    cols+=s.split(',')
print(cols)

#loop on pixels
nside=32

parquet_file="v1.parquet"

nfiles=0
for ipix in range(hp.nside2npix()):
    data=gc.get_quantities(cols,native_filters=["healpix_pixel == {}".format(ipix)])
    if len(data)==0 :
        continue
    df=pd.DataFrame(data)
    print("Reading {} data from pixfile={}".format(df.index.size,ipix))
    parquet_append = append and os.path.exists(parquet_file)
    print("Writing to {}=".format(parquet_file))
    df.to_parquet(parquet_file,append=parquet_append,file_scheme='simple',engine='fastparquet',compression='gzip')
    nfiles+=1
    print(nfiles)
    
