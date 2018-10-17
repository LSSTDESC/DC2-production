#!/bin/bash

# stack setup
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup -r /opt/lsst/software/stack/obs_lsstCam -j
echo "stack init done"
#
cd ${OUTPUT_DIR}

echo "running on tract=$1"
TRACT=$1
python "${SCRIPT_DIR}"/merge_tract_cat.py "${DM_REPO}" "${TRACT}"
