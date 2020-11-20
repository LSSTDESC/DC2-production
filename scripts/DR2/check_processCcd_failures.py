import os
import glob
import subprocess

command = 'grep FATAL logging/processCcd*.log'

calexp_visits = [int(os.path.basename(_).split('-')[0]) for _ in
                 glob.glob('repo/rerun/dr2-calexp/calexp/*')]


output = subprocess.check_output(command, shell=True).decode('utf-8')\
                                                     .strip().split('\n')
print('failed processCcd sensor-visits:', len(output))
visits = set()
for line in output:
    visit_band, raft, slot = os.path.basename(line.split(':')[0]).split('_')[1:4]
    visit, band = visit_band.split('-')
    visit = int(visit)
    slot = slot.split('.')[0]
    #print(visit, band, raft, slot)
    visits.add(visit)

    run22_raw_file = os.path.join('/global/cfs/cdirs/lsst/projecta/lsst/production/DC2_ImSim/Run2.2i/sim/y1-wfd', f'{visit:08d}', f'lsst_a_{visit}_{raft}_{slot}_{band}.fits')
    if os.path.isfile(run22_raw_file):
        print(run22_raw_file)

print('problem visits:', visits)

print('missing from calexps:')
for visit in visits:
    if visit not in calexp_visits:
        print(visit)

