#!/bin/bash

# TRACT=$1
TRACT=4849
echo "Processing: " $VISIT_FILE

### This repo was prepared with a variant of 2019.13
REPO=/global/cscratch1/sd/rearmstr/new_templates/diffim_template
SCRIPT_DIR=${HOME}/local/lsst/DC2-production/scripts
OUTPUT_DIR=${SCRATCH}/DC2/Run1.2p/dia_object

mkdir -p ${OUTPUT_DIR}

# DESCPYTHONPATH will get prepended to the PYTHONPATH
# by the sourcing of the setup file
# Need w.2019.13_diff branch of obs_lsst
export DESCPYTHONPATH=${HOME}/local/lsst/obs_lsst

# . ${SCRIPT_DIR}/setup_shifter_env.sh

python ${SCRIPT_DIR}/merge_dia_object_cat.py ${REPO} \
   ${TRACT} \
   --dataset deepDiff_diaObject \
   --dia_source_reader dc2_dia_source_run1.2p_test \
   --base_dir /global/cscratch1/sd/wmwv/DC2/Run1.2p/dia_src_visit_from_deepDiff_diaObject \
   --output_name dia_object \
   --verbose \
   --output_dir ${OUTPUT_DIR} \
