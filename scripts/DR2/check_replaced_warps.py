import os
from astropy.io import fits

dr2_dir = '/global/cscratch1/sd/descdm/DC2/DR2'
with open(os.path.join(dr2_dir, 'test_results', 'DR2_corrupted_warps.txt')) as fd:
    for line in fd:
        warp_file = line.strip()
        with fits.open(warp_file) as warp:
            try:
                warp.verify('exception')
                for i in (1, 2, 3):
                    warp[i].data.shape
            except Exception as eobj:
                print(eobj)
                print(warp_file)
