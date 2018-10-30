# And then from Python:
repo='/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-all2'
print(repo)

from lsst.daf.persistence import Butler
butler = Butler(repo)


tract=4850
patch='3,4'
# Test retrieval of a specific tract+patch
cat = butler.get(datasetType='deepCoadd_ref', dataId={'tract': tract, 'patch': patch})


prim=cat['detect_isPrimary']
print("#objects={} primaries={} frac={}".format(len(prim),sum(prim),sum(prim)/len(prim)))
