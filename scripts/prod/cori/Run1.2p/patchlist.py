# And then from Python:
import os,sys
repo=os.getenv('DM_REPO')
print("DM_REPO={}".format(repo))

tract=int(sys.argv[1])
print("analyzing tract={}".format(tract))

from lsst.daf.persistence import Butler
butler = Butler(repo)


skymap = butler.get(datasetType='deepCoadd_skyMap')
patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]
print(patches)
