import os
import glob
import sqlite3
import pandas as pd

"""
repo/rerun/dr2-calexp/calexp/00001472-z/R32/calexp_00001472-z-R32-S00-det135.fits
"""


with sqlite3.connect('repo/registry.sqlite3_run3.1i_only') as con:
    registry = pd.read_sql('select * from raw', con)

calexp_dir = 'repo/rerun/dr2-calexp'

missing = []
for _, row in registry.iterrows():
    visit = row['visit']
    band = row['filter']
    raft = row['raftName']
    slot = row['detectorName']
    det = row['detector']
    visit_id = f'{visit:08d}-{band}'
    calexp = os.path.join(calexp_dir, 'calexp', visit_id , raft,
                          f'calexp_{visit_id}-{raft}-{slot}-det{det:03d}.fits')
    if not os.path.isfile(calexp):
        missing.append(calexp)
        continue
    skycorr = os.path.join(calexp_dir, 'skyCorr', visit_id , raft,
                          f'skyCorr_{visit_id}-{raft}-{slot}-det{det:03d}.fits')
    if not os.path.isfile(skycorr):
        print(skycorr)
