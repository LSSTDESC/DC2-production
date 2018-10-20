#!/bin/bash

# stack setup
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup -r /opt/lsst/software/stack/obs_lsstCam -j
echo "stack $STACK_VERSION init done"
#
cd ${OUTPUT_DIR}

TRACT=$1
echo "running on tract=$TRACT"

#python "${SCRIPT_DIR}"/merge_tract_cat.py "${DM_REPO}" "${TRACT}"

#for the moment (just for functinal tests)
python $HERE/singlePatch.py
#python $HERE/checkButler.py

