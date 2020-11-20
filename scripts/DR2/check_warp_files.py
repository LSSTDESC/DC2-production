import os
import glob
from astropy.io import fits
import pandas as pd

df0 = pd.read_json('DR2_first_pass_missing_filter-tract-patches.json')

repo = '/global/cscratch1/sd/descdm/DC2/DR2/repo'
deepCoadd_dir = os.path.join(repo, 'rerun', 'dr2-coadd', 'deepCoadd')

df = df0.query('tract == 4849 and band == "y"')

with open('DR2_corrupted_warps.txt', 'w') as output:
    for i, (_, row) in zip(range(len(df0)), df0.iterrows()):
        print(i, len(df0))
        band, tract, patch = row['band'], row['tract'], row['patch']
        warp_dir = os.path.join(deepCoadd_dir, band, str(tract), patch)
        warp_files = glob.glob(os.path.join(warp_dir, 'warp*.fits'))
        for warp_file in warp_files:
            with fits.open(warp_file) as warp:
                try:
                    warp.verify(option='exception')
                    for i in range(1, 4):
                        warp[i].data.shape
                except Exception as eobj:
                    print(os.path.basename(warp_file), eobj)
                    output.write(warp_file + '\n')
