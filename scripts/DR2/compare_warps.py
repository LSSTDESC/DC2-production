import numpy as np
from astropy.io import fits

dr2_warp = fits.open('/global/cscratch1/sd/descdm/DC2/DR2/repo/rerun/dr2-coadd/deepCoadd/i/5064/1,3/psfMatchedWarp-i-5064-1,3-174551.fits')

srs_warp = fits.open('/global/cfs/cdirs/lsst/production/DC2_ImSim/Run2.2i/desc_dm_drp/v19-dr2-warps/sps/lssttest/dataproducts/desc/DC2/Run2.2i/v19.0.0-v1/rerun/run2.2i-coadd-wfd-dr6-v1-grizy/deepCoadd/i/5064/1,3/psfMatchedWarp-i-5064-1,3-174551.fits')

for hdu in range(1, 4):
    diff = dr2_warp[hdu].data - srs_warp[hdu].data

    index = np.where(diff == diff)
    print(hdu, np.min(diff[index]), np.max(diff[index]))
