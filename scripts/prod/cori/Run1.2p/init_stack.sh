#!/bin/bash

# stack setup
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup -r /opt/lsst/software/stack/obs_lsstCam -j
echo "stack $STACK_VERSION init done"
#
