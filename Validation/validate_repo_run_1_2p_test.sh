#!/bin/bash

# Author: Michael Wood-Vasey <wmwv@pitt.edu>
# Date Started: 2018-08-01
# Purpose: Run LSST SRC validate_drp on DC2 Run 1.2p-test data

source /global/common/software/lsst/cori-haswell-gcc/stack/w.2018.26_sim2.9.0-v2/loadLSST.bash

setup lsst_distrib
# 2018-08-04  MWV: I needed local versions of obs_lsstCam and a small change to validate_drp
#  By the time anyone wants to re-run this, these should both be in lsst_distrib.
# If you need to run them right now and don't have permission to access my account:
# git clone git@github.com:lsst/obs_lsstCam
# git clone git@github.com:lsst/validate_drp
# cd validate_drp; git checkout tickets/DM-15323
setup -r ~wmwv/local/lsst/obs_lsstCam
setup -k -r ~wmwv/local/lsst/validate_drp

RUN_1_2P_TEST_REPO='/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p-test/w_2018_28/rerun/newformat-071718'

# Each of the YAML files was created with a random subset of 10 visitis from the successful calexp
# present on NERSC as of 2018-08-04
validateDrp.py "${RUN_1_2P_TEST_REPO}" --verbose --noplot --configFile Run_1.2p-test_u.yaml
validateDrp.py "${RUN_1_2P_TEST_REPO}" --verbose --noplot --configFile Run_1.2p-test_g.yaml
validateDrp.py "${RUN_1_2P_TEST_REPO}" --verbose --noplot --configFile Run_1.2p-test_r.yaml
validateDrp.py "${RUN_1_2P_TEST_REPO}" --verbose --noplot --configFile Run_1.2p-test_i.yaml
validateDrp.py "${RUN_1_2P_TEST_REPO}" --verbose --noplot --configFile Run_1.2p-test_z.yaml
validateDrp.py "${RUN_1_2P_TEST_REPO}" --verbose --noplot --configFile Run_1.2p-test_y.yaml

# I put in ran --noplot above so that the above would still work 
# if you started with a DISPLAY but lost the connection and 
# no longer had a display when validateDrp went to plot things.
# You 
# To generate the plots, run the following line:
# for filt in u g r i z y; do validateDrp.py *_${filt}.json; done

# This will generate the plots.
# It currently [2018-08-05] will spit out warning messages and fail to print out 
# the summary table because of some silly inconsistencies 
# in the 'validate_drp' and 'verify' that MWV will fix "real soon".
