#!/bin/bash

VISIT_FILE=$1
echo "Processing: " $VISIT_FILE

REPO=/global/cscratch1/sd/desc/DC2/data/Run1.2p/w_2018_39/rerun/coadd-v4
SCRIPT_DIR=${HOME}/local/lsst/DC2-production/scripts/
OUTPUT_DIR=${SCRATCH}/DC2/Run1.2p/src_visit
OBJECT_TABLE=dc2_object_run1.2p

mkdir -p ${OUTPUT_DIR}

# DESCPYTHONPATH will get prepended to the PYTHONPATH
# by the sourcing of the setup file
export DESCPYTHONPATH=${HOME}/local/lsst/gcr-catalogs
. ${SCRIPT_DIR}/setup_shifter_env.sh

python ${SCRIPT_DIR}/merge_forced_source_cat.py ${REPO} \
   --output_dir ${OUTPUT_DIR} \
   --visit_file ${VISIT_FILE}
