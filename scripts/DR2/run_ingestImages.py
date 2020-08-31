"""
Script to ingest WFD raw data from Run2.2i or Run3.1i for DR2.
"""
import os
import glob
import time
import sqlite3
import subprocess
import pandas as pd

dc2_info_dir = '/global/cfs/cdirs/descssim/DC2'

# Get the list of visits that overlap with the DDF.
with open(os.path.join(dc2_info_dir, 'Run3.0i/Run3.0i_visit_list.txt')) as fd:
    ddf_visits = [int(_.split()[0]) for _ in fd]

# The Summary table in this opsim db file has been down-selected to include
# just the DC2 visits as tabulated in
# https://github.com/LSSTDESC/sims_GCRCatSimInterface/blob/master/workspace/run2.1/data/master_obshistid_list.txt
opsim_db_file \
    = os.path.join(dc2_info_dir, 'minion_1016_desc_dithered_v4_trimmed.db')

# MJD values corresponding to y1 and y2 selections.
t0 = 59580               # start of minion 1016 survey.
t1 = t0 + 365
t2 = t0 + 2.*365

# Get y1 visit list for WFD visits (propID=54).
tmax = t1
propID = 54
with sqlite3.connect(opsim_db_file) as conn:
    df = pd.read_sql(f'''select * from summary where propID=={propID}
                         and expMJD<{tmax} order by obsHistID asc''', conn)
    visits = list(df['obsHistID'])

# Loop over visits and find directory of raw image files for each visit.
run = 'Run3.1i'
raw_file_dirs = f'/global/cfs/cdirs/lsst/production/DC2_ImSim/{run}/sim/y*-???'

repo = 'repo'

found = []
missing = []
for visit in visits:
    try:
        pattern = os.path.join(raw_file_dirs, f'{visit:08d}')
        visit_dir = glob.glob(pattern)[0]
        found.append(visit)
    except IndexError as eobj:
        # Directory with raw files not found for this visit.  Identify
        # it as missing if it overlaps the DDF or if we are ingesting
        # Run2.2i data.
        if (run == 'Run2.2i') or (visit in ddf_visits):
            missing.append(visit)
    else:
        command = f'ingestImages.py {repo} {visit_dir}/lsst*.fits --ignore-ingested'
        print(command)
        t0 = time.time()
        subprocess.check_call(command, shell=True)
        print(f"{visit} ingest time:", time.time() - t0)

print(f"# {run} visits found:", len(found))
print(f"# {run} visits missing:", len(missing))
