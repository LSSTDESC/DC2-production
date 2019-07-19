# 2019-06-7 Francois Lanusse
# Copy-pasted from script by Michael Wood-Vasey
# Use
# shifter --image=lsstdesc/stack-jupyter:prod
# I don't know how to more specifically control the version of the shifter image
# . /global/common/software/lsst/common/miniconda/kernels/stack-prod.sh

. setup_shifter_env.sh

# On NERSC
REPO=/global/cscratch1/sd/desc/DC2/data/Run2.1i/rerun/coadd-dr1b-v1
cd ${REPO}/deepCoadd-results/merged
TRACTS=`find . -name "*mcal*" | cut -c3-6 | uniq | sort | sed -e 's/[\r\n]/ /g'`

WORKING_DIR=/global/homes/f/flanusse/scratch/object_mcal_catalog_coaddv1_dr1b
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/f/flanusse/repo/DC2-production/scripts
echo $TRACTS
nohup python "${SCRIPT_DIR}"/merge_metacal_cat.py ${REPO} ${TRACTS} > merge_mcal_cat.log 2>&1 < /dev/null
