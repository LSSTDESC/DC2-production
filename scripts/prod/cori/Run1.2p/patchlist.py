# And then from Python:
import os,sys
repo=os.getenv('DM_REPO')
print("DM_REPO={}".format(repo))

tract=int(sys.argv[1])
print("analyzing tract={}".format(tract))

from lsst.daf.persistence import Butler
butler = Butler(repo)
skymap = butler.get(datasetType='deepCoadd_skyMap')

tracts=[4429,4430,4431,4432,4433,4636,4637,4638,4639,4640,4848,4849,4850,4851,4852,5062,5063,5064,5065,5066]

for tract in tracts:
    print(tract,":")
    patches = ['%d,%d' % patch.getIndex() for patch in skymap[tract]]
    for patch in patches:
        print(tract,patch)
