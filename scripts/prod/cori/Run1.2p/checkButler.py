# And then from Python:
repo='/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-all2'
from lsst.daf.persistence import Butler
butler = Butler(repo)
# Test retrieval of a specific tract+patch
cat = butler.get(datasetType='deepCoadd_ref', dataId={'tract': 4850, 'patch': '3,4'}
print(cat)
