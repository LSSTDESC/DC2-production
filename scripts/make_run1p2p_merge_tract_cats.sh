# 2019-01-23 Michael Wood-Vasey
# Invoke with the 'start-kernel-cli.py' command from
# github.com/LSSTDESC/nersc
# python ~/local/lsst/nersc/scripts/start-kernel-cli.py desc-stack
 
# On NERSC
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_39/rerun/coadd-v4
TRACTS="5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429"

WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog_new
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/w/wmwv/local/lsst/DC2-production/scripts
# These weren't actually run this way in one list.
# I instead ran each TRACT one-by-one on a head node.
# These "should" be done through SLURM.  But given the long queue times it's not clearly faster to do so.
nohup python "${SCRIPT_DIR}"/merge_tract_cat.py "${REPO}" "${TRACTS}" > merge_tract_cat.log 2>&1 < /dev/null 
