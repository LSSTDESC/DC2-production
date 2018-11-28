#!/bin/bash
# source this scripts directly from the directory where it is located 
# ie: source setup.sh
# do it once (also in batch). you can redefine your own variable with the follwing envs:
# export STACK_VERSION=
# export DM_REPO=
# export RUNDIR=
#
#
#  author S. Plaszczynski 9/oct 18 (plaszczy@lal.in2p3.fr)
#
export HERE=$PWD
echo "HERE=$HERE"
#stack version
export STACK_VERSION=lsstdesc/stack-jupyter:w_2018_30-run1.2-v2

export SCRIPT_DIR=$(readlink -f $PWD/../../..)


#DM output directory
#export DM_REPO="/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-all2"
export DM_REPO=/global/cscratch1/sd/desc/DC2/data/Run1.2p/w_2018_30/rerun/coadd-all2


#output DIR
export RUNDIR=/global/cscratch1/sd/plaszczy/Run1.2p/run
#export RUNDIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog

if ! [ -d "$RUNDIR" ] ; then
echo "missing RUNDIR=$RUNDIR"
return 
fi



echo "STACK_VERSION=${STACK_VERSION}"
echo "SCRIPT_DIR=${SCRIPT_DIR}"
echo "DM_REPO=${DM_REPO}"
echo "RUNDIR=${RUNDIR}"


#checks
if ! [ -d ${RUNDIR} ]; then
echo "RUNDIR does not exist"
return
fi
if ! [ -d $REPO ]; then
echo "REPO does not exist"
return
fi

#for running multiple seq jobs on single node
module load taskfarmer
