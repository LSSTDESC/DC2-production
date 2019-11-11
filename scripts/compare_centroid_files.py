"""
Script to perform pair-wise comparisons of model and realized
fluxes from centroid files between common datasets generated at Cori,
Theta, and the UKGrid.
"""
import os
import glob
import gzip
import numpy as np
import lsst.afw.math as afw_math
from diff_image_stats import get_det_names

# Run2.2i test visit
visit = 479028
band = 'i'

det_names = get_det_names()
site_pairs = (('cori', 'theta'), ('cori', 'grid'), ('grid', 'theta'))
test_dir = '/global/projecta/projectdirs/lsst/production/DC2_ImSim/Run2.2i/sim/tests'

def extract_fluxes(centroid_file):
    model_fluxes = dict()
    realized_fluxes = dict()
    with gzip.open(centroid_file) as fd:
        for i, line in enumerate(fd):
            if i == 0:
                continue
            tokens = line.split()
            model_fluxes[tokens[0]] = tokens[1]
            realized_fluxes[tokens[0]] = tokens[2]
    return model_fluxes, realized_fluxes

for site_pair in site_pairs:
    site1, site2 = site_pair
    print(f'{site1} vs {site2}')
    for det_name in det_names:
        centroid_file_1 \
            = os.path.join(test_dir, f'{visit:08d}_{site1}',
                           f'centroid_{visit}_{det_name}_{band}.txt.gz')
        centroid_file_2 \
            = os.path.join(test_dir, f'{visit:08d}_{site2}',
                           f'centroid_{visit}_{det_name}_{band}.txt.gz')
        model1, realized1 = extract_fluxes(centroid_file_1)
        model2, realized2 = extract_fluxes(centroid_file_2)
        mismatches = 0
        for obj_id, flux1 in model1.items():
            if obj_id not in model2:
                continue
            if flux1 != model2[obj_id]:
                mismatches += 1
        realized_mismatches = 0
        pulls = []
        for obj_id, flux1 in realized1.items():
            if obj_id not in realized2:
                continue
            flux2 = realized2[obj_id]
            if flux1 != flux2:
                realized_mismatches += 1
                pulls.append((float(flux1) - float(flux2))
                             /np.sqrt(2.*float(model1[obj_id])))
        pulls = np.array(pulls)
        if len(pulls) > 0:
            stats = afw_math.makeStatistics(pulls, afw_math.STDEVCLIP)
            stdev_clip = '%.2f' % stats.getValue(afw_math.STDEVCLIP)
        else:
            stdev_clip = 'N/A'
        id_mismatches = set(model1.keys()).symmetric_difference(model2.keys())
        print(f'{det_name}  {mismatches}  {realized_mismatches}  {stdev_clip}')

#        print(f'{det_name}  {mismatches}  {realized_mismatches}  '
#              f'{stdev_clip}  {len(id_mismatches)}  {len(model1)}  {len(model2)}')
