#!/bin/bash

# stack setup
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup -r /opt/lsst/software/stack/obs_lsstCam -j
echo "stack $STACK_VERSION init done"
#

cd ${RUNDIR}

TRACT=$1
echo "running on tract=$TRACT"

time python "${SCRIPT_DIR}"/merge_tract_cat.py "${DM_REPO}" "${TRACT}"

echo "TRACT=$TRACT done" 

#for the moment (just for functional tests)
#time python $HERE/singlePatch.py
#python $HERE/checkButler.py
