# 2019-06-7 Francois Lanusse
# Copy-pasted from script by Michael Wood-Vasey
# Use 
# shifter --image=lsstdesc/stack-jupyter:prod
# I don't know how to more specifically control the version of the shifter image
# . /global/common/software/lsst/common/miniconda/kernels/stack-prod.sh

. setup_shifter_env.sh
 
# On NERSC
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run2.1i/w_2019_19-v1/rerun/coadd-v1/
TRACTS="2905 2906 2907 2908 3082 3083 3084 3085 3086 3265 3266 3267 3268 3452 3453 3454 \
3641 3642 3643 3834 3835 3836 3837 4027 4028 4032 4033 4034 4035 4230 4231 4232 4233 4234 \
4235 4236 4435 4436 4437 4438 4439 4440 4441 4644 4645 4646 4647 4648 4857 4858 4859 4860"

WORKING_DIR=/global/homes/f/flanusse/scratch/object_catalog
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/f/flanusse/repo/DC2-production/scripts
nohup python "${SCRIPT_DIR}"/merge_tract_cat.py ${REPO} ${TRACTS} > merge_mcal_cat.log 2>&1 < /dev/null 
