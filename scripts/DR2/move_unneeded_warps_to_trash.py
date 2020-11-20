import os
import shutil

dest = '/global/cscratch1/sd/descdm/DC2/DR2/repo/rerun/trash'
with open('unneeded_Run2.2i_warps.txt') as fd:
    for i, line in enumerate(fd):
        src = line.strip()
        if not os.path.isfile(src):
            continue
        print(src)
        shutil.move(src, dest)
