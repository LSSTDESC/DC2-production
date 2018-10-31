#!/bin/bash -l

# Run with module load taskfarmer
# sbatch run_Run1p1_merge_tract_cats.sl

#SBATCH -N 2 -c 64
#SBATCH -t 05:00:00
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -L SCRATCH

export PATH=${PATH}:/usr/common/tig/taskfarmer/1.5/bin:$(pwd)
export THREADS=32

runcommands.sh tasks.txt
