import pickle
import numpy as np
import pandas as pd
from lsst.sims.catUtils.utils import ObservationMetaDataGenerator

def fov_overlaps_protoDC2(x, y, half_size=2.5, radius=1.77):
    if x > half_size and y > half_size:
        return (x - half_size)**2 + (y - half_size)**2 < radius**2
    elif x < -half_size and y > half_size:
        return (x + half_size)**2 + (y - half_size)**2 < radius**2
    elif x < -half_size and y < -half_size:
        return (x + half_size)**2 + (y + half_size)**2 < radius**2
    elif x > half_size and y < -half_size:
        return (x - half_size)**2 + (y + half_size)**2 < radius**2
    outer_box_size = half_size + radius
    return (-outer_box_size < x and x < outer_box_size and
            -outer_box_size < y and y < outer_box_size)

opsim_db = 'minion_1016_sqlite_new_dithers.db'
obs_gen = ObservationMetaDataGenerator(opsim_db)

# Inclusive fov is 2.1 deg radius; protoDC2 is 5x5 box centered on
# (RA, Dec) = (0, 0), with vertical sides aligned N-S and horizontal
# sides aligned E-W.  Use 20x20 box to ensure all viable dithered
# visits are considered
dxy = 10
fov = 2.1
radius = fov
obs_list = obs_gen.getObservationMetaData(fieldRA=(0, dxy),
                                          fieldDec=(-dxy, dxy),
                                          boundLength=radius)
obs_list.extend(obs_gen.getObservationMetaData(fieldRA=(360-dxy, 360.),
                                               fieldDec=(-dxy, dxy),
                                               boundLength=radius))

df = pd.DataFrame(columns=['obsHistID', 'fieldRA', 'fieldDec',
                           'randomDitherFieldPerVisitRA',
                           'randomDitherFieldPerVisitDec',
                           'filter', 'fieldID', 'propID', 'expMJD'])

for obs in obs_list:
    ditheredRA = (obs.summary['OpsimMetaData']['randomDitherFieldPerVisitRA']
                  *180./np.pi)
    if ditheredRA > 180:
        ditheredRA -= 360.
    ditheredDec = (obs.summary['OpsimMetaData']['randomDitherFieldPerVisitDec']
                   *180./np.pi)

    if not fov_overlaps_protoDC2(ditheredRA, ditheredDec):
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

pickle.dump(df, open('protoDC2_visits_%i.pkl' % dxy, 'wb'), protocol=2)

print(len(df))
