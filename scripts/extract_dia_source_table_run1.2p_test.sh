#!/bin/bash

# VISIT_FILE=$1
VISIT_FILE=run_1.2_visits.txt
echo "Processing: " $VISIT_FILE

REPO=/global/cscratch1/sd/rearmstr/new_templates/diffim_template
# REPO=/global/cscratch1/sd/wmwv/DC2/diffim_template
SCRIPT_DIR=${HOME}/local/lsst/DC2-production/scripts
OUTPUT_DIR=${SCRATCH}/DC2/Run1.2p/dia_src_visit

mkdir -p ${OUTPUT_DIR}

# DESCPYTHONPATH will get prepended to the PYTHONPATH
# by the sourcing of the setup file
# Need issues/290 branch of gcr-catalogs
# Need w.2018.39-run1.2-v3_diff branch of obs_lsst
export PYTHONPATH=${HOME}/local/lsst/gcr-catalogs:${PYTHONPATH}

# . ${SCRIPT_DIR}/setup_shifter_env.sh

#   --object_reader dc2_dia_object_run1.2p_test \
python ${SCRIPT_DIR}/merge_source_cat.py ${REPO} \
   --dataset deepDiff_diaSrc \
   --object_dataset deepDiff_diaObject \
   --output_name dia_src \
   --output_dir ${OUTPUT_DIR} \
   --visit_file ${VISIT_FILE}
