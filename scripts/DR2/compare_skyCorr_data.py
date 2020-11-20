import numpy as np
import lsst.daf.persistence as dp

dr2_repo = 'repo/rerun/dr2-calexp'

srs_repo = '/global/cscratch1/sd/descdm/DC2/Run2.2i-parsl/v19.0.0-v1/rerun/run2.2i-calexp-v1-copy'

dr2_butler = dp.Butler(dr2_repo)
srs_butler = dp.Butler(srs_repo)

visits = (2249, 159496, 181866, 174551, 13271, 5898)

for visit in visits:
    datarefs = srs_butler.subset('skyCorr', visit=visit)
    print(visit)
    for dataref in datarefs:
        dataId = dataref.dataId
        detname = '_'.join((dataId['raftName'], dataId['detectorName']))
        try:
            srs_sky_corr = dataref.get('skyCorr')
        except dp.butlerExceptions.NoResults:
            srs_sky_corr = None
            print('missing srs_sky_corr:', visit, detname)
        try:
            dr2_sky_corr = dr2_butler.get('skyCorr', dataId=dataId)
        except dp.butlerExceptions.NoResults:
            dr2_sky_corr = None
            print('missing dr2_sky_corr:', visit, detname)
        if srs_sky_corr is None or dr2_sky_corr is None:
            continue
        srs_image = srs_sky_corr.getImage()
        srs_image -= dr2_sky_corr.getImage()
        print(dataId['raftName'], dataId['detectorName'],
              np.min(srs_image.array), np.max(srs_image.array),
              np.mean(srs_image.array), np.median(srs_image.array))
    print()
