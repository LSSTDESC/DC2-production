# 2019-06-7 Francois Lanusse
# Copy-pasted from script by Michael Wood-Vasey
# Use
# shifter --image=lsstdesc/stack-jupyter:prod
# I don't know how to more specifically control the version of the shifter image
# . /global/common/software/lsst/common/miniconda/kernels/stack-prod.sh

. setup_shifter_env.sh

# On NERSC
REPO=/global/cscratch1/sd/desc/DC2/data/Run2.1i/rerun/metacal-griz/
TRACTS="2728 2729 2730 2731 2732 2733 2734 2735 2899 2900 2901 2902 2903 2904 2905
2906 2907 2908 2909 3077 3078 3079 3080 3081 3082 3083 3084 3085 3086 3258 3259 3260
3261 3262 3263 3264 3265 3266 3267 3268 3443 3444 3445 3446 3447 3448 3449 3450
3451 3452 3453 3454 3632 3633 3634 3635 3636 3637 3638 3639 3640 3641 3643 3826
3827 3828 3829 3830 3831 3832 3833 3834 3835 3836 3837 4023 4024 4025 4026 4027
4028 4029 4030 4031 4032 4033 4034 4035 4224 4225 4226 4227 4228 4229 4230 4231 4232
4233 4234 4235 4236 4429 4430 4431 4432 4433 4434 4435 4436 4437 4438 4439 4440 4441 4637
4638 4639 4640 4641 4642 4643 4644 4645 4646 4647 4648 4849 4850 4851 4852 4853 4854
4855 4856 4857 4858 4859 4860 5064 5065 5066 5067 5068 5069 5070 5071 5072 5073 5074"

WORKING_DIR=/global/homes/f/flanusse/scratch/object_mcal_catalog_coaddv1
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/f/flanusse/repo/DC2-production/scripts
nohup python "${SCRIPT_DIR}"/merge_metacal_cat.py ${REPO} ${TRACTS} > merge_mcal_cat.log 2>&1 < /dev/null
