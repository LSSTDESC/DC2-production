# 2019-01-15 Michael Wood-Vasey
#
# Use 
# shifter --image=lsstdesc/stack-jupyter:prod
# I don't know how to more specifically control the version of the shifter image
# . /global/common/software/lsst/common/miniconda/kernels/stack-prod.sh

. setup_shifter_env.sh
 
# On NERSC
REPO=/global/cscratch1/sd/desc/DC2/data/Run1.2i_globus_in2p3_20181217/w_2018_39/rerun/multiband
TRACTS="5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429"

WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/object_catalog_new
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/w/wmwv/local/lsst/DC2-production/scripts
# These weren't actually run this way in one list.
# I instead ran each TRACT one-by-one on a head node.
# These "should" be done through SLURM.  But given the long queue times it's not clearly faster to do so.
nohup python "${SCRIPT_DIR}"/merge_tract_cat.py "${REPO}" "${TRACTS}" > merge_tract_cat.log 2>&1 < /dev/null 
