import os,sys
sys.path.insert(0,os.getenv('SCRIPT_DIR'))

from merge_tract_cat import load_and_save_tract,load_patch

repo=os.getenv('DM_REPO')
print(repo)

tract=4849
patch=["1,1"]

load_and_save_tract(repo,tract,"test.hdf5",patches=patch,verbose=True)
