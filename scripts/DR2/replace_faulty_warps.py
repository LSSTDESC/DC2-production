import os
import shutil

dr2_dir = '/global/cscratch1/sd/descdm/DC2/DR2'
repo = os.path.join(dr2_dir, 'repo')

with open(os.path.join(dr2_dir, 'test_results/DR2_corrupted_warps.txt')) as fd:
    corrupted = [_.strip() for _ in fd]

trash = os.path.join(repo, 'rerun', 'trash', 'faulty_warps')
os.makedirs(trash, exist_ok=True)

for item in corrupted:
    assert(os.path.isfile(item))
    rerun_path = os.path.join(*item.split(os.path.sep)[-5:])
    replacement = os.path.join(repo, 'rerun', 'replacement-warps', rerun_path)
    discard_path = os.path.join(trash, os.path.basename(item))

    if not os.path.isfile(replacement):
        continue

    print(item)
    print(discard_path)
    print()
    print(replacement)
    print('\n')
#    shutil.move(item, discard_path)
#    os.symlink(replacement, item)
