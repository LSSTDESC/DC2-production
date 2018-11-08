#!/bin/bash

source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup -r /opt/lsst/software/stack/obs_lsstCam -j
echo "stack init done"
echo "$1"

#time python singlePatch.py
export PYTHONPATH=$HOME/DC2/desc_dc2_dm_data:$PYTHONPATH
