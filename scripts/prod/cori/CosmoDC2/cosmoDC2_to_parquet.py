# desc_python
import os
import sys
import numpy as np
import pandas as pd
import healpy as hp
from time import time

sys.path.insert(0,'/global/homes/p/plaszczy/DC2/gcr-catalogs')
import GCRCatalogs


CATALOG='cosmoDC2_v1.1.4_image'

#version GCR
print('GCRCatalogs =', GCRCatalogs.__version__, '|' ,'GCR =', GCRCatalogs.GCR.__version__)
#liste des catalogues
print('\n'.join(sorted(GCRCatalogs.get_available_catalogs(include_default_only=False))))
gc = GCRCatalogs.load_catalog(CATALOG)
print(gc.get_catalog_info('description'))
#print(', '.join(sorted(gc.list_all_quantities())))                                                    


#data=gc.get_quantities([q for q in gc.list_all_quantities()],native_filters=['healpix_pixel == 9813'])

cols=['galaxy_id','is_central','halo_mass','stellar_mass','ra','dec','redshift']
filters=['u','g','r','i','z','y']
for f in filters:
    s="mag_{0},Mag_true_{0}_lsst_z0".format(f)
    cols+=s.split(',')

print(cols)

#output
out=CATALOG+".parquet"
OUTDIR="/global/cscratch1/sd/plaszczy/CosmoDC2"

#creer output
OUTDIR=os.path.join(OUTDIR,out)
#parquet_file=os.path.join(OUTDIR,out)
assert os.path.exists(OUTDIR)

#pix=np.loadtxt("healpix_1.4.4",unpack=True).astype('int')

#from /global/homes/p/plaszczy/DC2/gcr-catalogs/GCRCatalogs/catalog_configs/cosmoDC2_v1.1.4_image.yaml
healpix_pixels=[8786, 8787, 8788, 8789, 8790, 8791, 8792, 8793, 8794, 8913, 8914, 8915, 8916, 8917, 8918, 8919, 8920, 8921, 9042, 9043, 9044, 9045, 9046, 9047, 9048, 9049,9050, 9169, 9170, 9171, 9172, 9173, 9174, 9175, 9176, 9177, 9178, 9298, 9299, 9300, 9301, 9302, 9303, 9304, 9305, 9306, 9425, 9426, 9427, 9428, 9429, 9430,9431, 9432, 9433, 9434, 9554, 9555, 9556, 9557, 9558, 9559, 9560, 9561, 9562, 9681, 9682, 9683, 9684, 9685, 9686, 9687, 9688, 9689, 9690, 9810, 9811, 9812,9813, 9814, 9815, 9816, 9817, 9818, 9937, 9938, 9939, 9940, 9941, 9942, 9943, 9944, 9945, 9946, 10066, 10067, 10068, 10069, 10070, 10071, 10072, 10073, 10074, 10193,10194, 10195, 10196, 10197, 10198, 10199, 10200, 10201, 10202, 10321, 10322, 10323, 10324, 10325, 10326, 10327, 10328, 10329, 10444, 10445, 10446, 10447, 10448, 10449, 10450,10451, 10452]



nskip=0
for ipix in healpix_pixels:
    print(ipix)
    t0=time()
    data=gc.get_quantities(cols,native_filters=["healpix_pixel == {}".format(ipix)])
    df=pd.DataFrame(data)
    t1=time()
    print("Read {}M data: {:2.1f}s".format(df.index.size/1e6,t1-t0))
    #convert float64 columns to float32
    #for n in cols:
    #    if str(df.dtypes[n])=='float64':
    #        df[n]=df[n].astype('float32') 
    #writing
    #df.to_parquet(parquet_file,append=os.path.exists(parquet_file),
    #                  file_scheme='hive',engine='fastparquet',compression=None)

    parquet_file=os.path.join(OUTDIR,str(ipix))
    df.to_parquet(parquet_file,engine='fastparquet',compression=None)
    t2=time()
    print("Wrote to {}".format(parquet_file))
    print("Tot time to process {:2.1f}s".format(t2-t0))
