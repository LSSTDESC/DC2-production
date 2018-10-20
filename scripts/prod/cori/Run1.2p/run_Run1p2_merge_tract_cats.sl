#!/bin/bash -l

# Run with module load taskfarmer
# sbatch run_Run1p1_merge_tract_cats.sl

#SBATCH -N 2 -c 64
#SBATCH -t 00:20:00
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH --license=projecta
#SBATCH --image=lsstdesc/stack-jupyter:w_2018_30-run1.2-v2

pwd
echo "HERE=$HERE"
echo "SCRIPT_DIR=${SCRIPT_DIR}"

export PATH=${PATH}:/usr/common/tig/taskfarmer/1.5/bin:$(pwd)
export THREADS=32

#runcommands.sh tasks.txt
$HERE/run_tract.sh 5860
