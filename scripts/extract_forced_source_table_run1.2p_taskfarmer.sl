#!/bin/sh
#SBATCH --image=lsstdesc/stack-jupyter:prod
#SBATCH -N 8 -c 64
#SBATCH --qos regular
#SBATCH --time 00:60:00
#SBATCH --constraint haswell
#SBATCH --licenses SCRATCH

echo "Job running in: " $PWD

export PATH=$PATH:/usr/common/tig/taskfarmer/1.5/bin:$(pwd)
export THREADS=16

# Create a visit list for each
# We are asking for 15 worker nodes (16-1), with 16 processes per node
# This conservative choice is for memory use reasons.
# We load a copy of the Object Table id, ra, dec for each run.
# Haswell nodes have 128 GB of memory, giving 8 GB per process
VISIT_LIST=run_1.2_visits.txt
TASK_LIST_DIR=extract_forced_source_job
NUM_TASKS=256

mkdir -p ${TASK_LIST_DIR}
cp ${VISIT_LIST} ${TASK_LIST_DIR}/

VISIT_LIST=${TASK_LIST_DIR}/${VISIT_LIST}
VISIT_LIST_BASE=${VISIT_LIST%.*}

python stripe_visits.py ${VISIT_LIST} ${NUM_TASKS}

TASK_LIST_FILE=task_list.txt
TASK_LIST_FULLPATH=${TASK_LIST_DIR}/${TASK_LIST_FILE}
# Initialize task list file to be empty
> $TASK_LIST_FULLPATH

# stripe_visits.py produces 0-indexed files, and seq is inclusive in its range
# so we want 0 to NUM_TASKS-1
for i in `seq 0 $((NUM_TASKS - 1))`; do 
    echo shifter /bin/bash "${PWD}/extract_forced_source_table_run1.2p.sh" "${PWD}/${VISIT_LIST_BASE}_${i}.txt" >> ${TASK_LIST_FULLPATH}
done

# Use the Task Farmer runcommands.sh to setup the server node
# and farm out each task to the worker nodes.
cd ${TASK_LIST_DIR}
runcommands.sh ${TASK_LIST_FILE}
