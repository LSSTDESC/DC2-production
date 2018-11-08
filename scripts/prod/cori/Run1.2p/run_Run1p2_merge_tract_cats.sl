#!/bin/bash -l

# Run with module load taskfarmer
# sbatch run_Run1p1_merge_tract_cats.sl

#SBATCH -N 2 -c 64
#SBATCH -t 16:00:00
#SBATCH -q regular
#SBATCH -C haswell
#SBATCH --image=lsstdesc/stack-jupyter:w_2018_30-run1.2-v2
#SBATCH -L SCRATCH

pwd
echo "HERE=$HERE"
echo "SCRIPT_DIR=${SCRIPT_DIR}"

export PATH=${PATH}:/usr/common/tig/taskfarmer/1.5/bin:$(pwd)
export THREADS=32

runcommands.sh tasks.txt
#$HERE/run_tract.sh 5860
