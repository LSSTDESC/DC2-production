import os
import sqlite3
import pandas as pd

with sqlite3.connect('repo/registry.sqlite3_run3.1i_only') as con:
    registry = pd.read_sql('select * from raw', con)

with sqlite3.connect('repo/rerun/dr2-calexp/tracts_mapping.sqlite3') as con:
    overlaps = pd.read_sql('select * from overlaps', con)

deepCoadd_dir = 'repo/rerun/dr2-coadd/deepCoadd'

unneeded_warp_files = set()
for i, row in registry.iterrows():
    print(i, len(registry))
    detector = row['detector']
    visit = row['visit']
    band = row['filter']
    df = overlaps.query(f'visit=={visit} and detector=={detector} '
                        f'and filter=="{band}"')
    for _, item in df.iterrows():
        tract = str(item['tract'])
        patch = '{},{}'.format(*[_ for _ in item['patch'] if _.isdigit()])
        warp_file = os.path.join(deepCoadd_dir, band, tract, patch,
                                 f'warp-{band}-{tract}-{patch}-{visit}.fits')
        if os.path.isfile(warp_file):
            unneeded_warp_files.add(warp_file)

with open('unneeded_Run2.2i_warps.txt', 'w') as output:
    for item in unneeded_warp_files:
        output.write(item + '\n')
