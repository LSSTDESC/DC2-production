import pickle
import numpy as np
import pandas as pd
from lsst.sims.catUtils.utils import ObservationMetaDataGenerator

opsim_db = 'minion_1016_sqlite_new_dithers.db'
obs_gen = ObservationMetaDataGenerator(opsim_db)

# Inclusive fov is 2.1 deg radius; protoDC2 is 5x5 box centered on 0, 0,
# with vertical sides aligned N-S and horizontal sides aligned E-W
half_extent = 10
fov = 2.1
radius = fov
obs_list = obs_gen.getObservationMetaData(fieldRA=(0, half_extent),
                                          fieldDec=(-half_extent, half_extent),
                                          boundLength=radius)
obs_list.extend(obs_gen.getObservationMetaData(fieldRA=(360-half_extent, 360.),
                                               fieldDec=(-half_extent, half_extent),
                                               boundLength=radius))

df = pd.DataFrame(columns=['obsHistID', 'fieldRA', 'fieldDec',
                           'ditheredRA', 'ditheredDec',
                           'filter', 'fieldID', 'propID', 'expMJD'])

outer_box_size = 2.5 + 1.77
for obs in obs_list:
    ditheredRA = obs.summary['OpsimMetaData']['ditheredRA']*180./np.pi
    if ditheredRA > 180:
        ditheredRA -= 360.
    ditheredDec = obs.summary['OpsimMetaData']['ditheredDec']*180./np.pi

    if (-outer_box_size > ditheredRA or ditheredRA > outer_box_size or
        -outer_box_size > ditheredDec or ditheredDec > outer_box_size):
        continue

    fieldRA = obs.summary['OpsimMetaData']['fieldRA']*180./np.pi
    if fieldRA > 180:
        fieldRA -= 360.
    df.loc[len(df)] = (obs.summary['OpsimMetaData']['obsHistID'],
                       fieldRA,
                       obs.summary['OpsimMetaData']['fieldDec']*180./np.pi,
                       ditheredRA, ditheredDec,
                       obs.summary['OpsimMetaData']['filter'],
                       obs.summary['OpsimMetaData']['fieldID'],
                       obs.summary['OpsimMetaData']['propID'],
                       obs.summary['OpsimMetaData']['expMJD'])

pickle.dump(df, open('protoDC2_visits_%i.pkl' % half_extent, 'wb'), protocol=2)

print(len(df))
