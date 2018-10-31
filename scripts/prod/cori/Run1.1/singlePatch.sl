#!/bin/bash -l

#SBATCH -N 1
#SBATCH -t 00:10:00
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -L SCRATCH

echo "batch workir=$PWD"
./singlePatch.sh
