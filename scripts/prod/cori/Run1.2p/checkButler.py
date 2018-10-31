# And then from Python:
import os
repo=os.getenv('DM_REPO')
print(repo)

from lsst.daf.persistence import Butler
butler = Butler(repo)


tract=4850
patch='3,4'
# Test retrieval of a specific tract+patch
cat = butler.get(datasetType='deepCoadd_ref', dataId={'tract': tract, 'patch': patch})

prim=cat['detect_isPrimary']
print("#objects={} primaries={} frac={}".format(len(prim),sum(prim),sum(prim)/len(prim)))

skymap = butler.get(datasetType='deepCoadd_skyMap')
patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]
print(patches)
