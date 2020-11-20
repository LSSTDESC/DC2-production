"""
Script to make symlinks to Run2.2i single frame processing outputs.

This should be run after ingesting the Run2.2i WFD raw data.
"""
import os
import glob
import sqlite3
import pandas as pd

def sorted_glob(*args):
    """Alias for `sorted(glob.glob(os.path.join(*args)))`"""
    return sorted(glob.glob(os.path.join(*args)))

def is_dir_or_link(path):
    """Alias for `os.path.isdir(path) or os.path.islink(path)`"""
    return os.path.isdir(path) or os.path.islink(path)

def make_symlink(src, dest, dry_run=False):
    """Make the symlink."""
    print(f"{src} -> {dest}")
    if dry_run:
        return None
    return os.symlink(src, dest)

repo = 'repo'
rerun_dir = os.path.join(repo, 'rerun', 'dr2-calexp')
calexp_dir = ('/global/cscratch1/sd/descdm/DC2/Run2.2i-parsl/v19.0.0-v1/'
              'rerun/run2.2i-calexp-v1-copy')

with sqlite3.connect(os.path.join(repo, 'registry.sqlite3')) as conn:
    df = pd.read_sql('select * from raw', conn)

visits = sorted(list(set(zip(df['visit'], df['filter']))))

dataset_dirs = ['processCcd_metadata', 'calexp', 'srcMatch', 'src', 'icSrc']

for dataset_dir in dataset_dirs:
    # Loop over visits in the registry.
    for visit, band in visits:
        visit_folder = f'{visit:08d}-{band}'
        src_dir = os.path.join(calexp_dir, dataset_dir, visit_folder)
        dest_dir = os.path.join(rerun_dir, dataset_dir, visit_folder)
        if not is_dir_or_link(dest_dir):
            # Make symlink to Run2.2i visit folder if there is no
            # Run3.1i folder or an already existing symlink.
            make_symlink(src_dir, dest_dir)
            continue
        if os.path.islink(dest_dir):
            continue
        # Loop over Run2.2i raft folders, and make symlinks for rafts
        # not in Run3.1i data.
        for src_raft_dir in sorted_glob(src_dir, 'R*'):
            dest_raft_dir = os.path.join(dest_dir,
                                         os.path.basename(src_raft_dir))
            if not is_dir_or_link(dest_raft_dir):
                make_symlink(src_raft_dir, dest_raft_dir)
                continue
            if os.path.islink(dest_raft_dir):
                continue
            # Loop over items in Run2.2i raft folders and make symlinks
            # in destination raft folders for missing items.
            for src_item in sorted_glob(src_raft_dir, '*'):
                dest_item = os.path.join(dest_raft_dir,
                                         os.path.basename(src_item))
                if not os.path.isfile(dest_item):
                    make_symlink(src_item, dest_item)
