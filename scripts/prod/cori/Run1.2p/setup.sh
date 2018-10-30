#!/bin/bash
# source this scripts directly from the directory where it is located 
# ie: source setup.sh
# do it once (also in batch). you can redefine your own variable with the follwing envs:
# export STACK_VERSION=
# export DM_REPO=
# export OUTPUT_DIR=
#
#
#  author S. Plaszczynski 9/oct 18 (plaszczy@lal.in2p3.fr)
#
export HERE=$PWD
echo "HERE=$HERE"
#stack version
if [ -z "${STACK_VERSION}" ] ; then
export STACK_VERSION=lsstdesc/stack-jupyter:w_2018_30-run1.2-v2
fi

if [ -z "${SCRIPT_DIR}" ] ; then
export SCRIPT_DIR=$(readlink -f $PWD/../../..)
fi
#DM output directory
if [ -z "${DM_REPO}" ] ; then
#export DM_REPO="/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-all2"
export DM_REPO=/global/cscratch1/sd/desc/DC2/data/Run1.2p/rerun/coadd-all2

fi

#output DIR
if [ -z "${OUTPUT_DIR}" ] ; then
#export OUTPUT_DIR=${DM_REPO}/summary
#export OUTPUT_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2-testSP/summary
export OUTPUT_DIR=/global/cscratch1/sd/plaszczy/Run1.2p
fi
mkdir -p ${OUTPUT_DIR}

echo "STACK_VERSION=${STACK_VERSION}"
echo "SCRIPT_DIR=${SCRIPT_DIR}"
echo "DM_REPO=${DM_REPO}"
echo "OUTPUT_DIR=${OUTPUT_DIR}"


#checks
if ! [ -d ${OUTPUT_DIR} ]; then
echo "OUTPUT_DIR does not exist"
return
fi
if ! [ -d $REPO ]; then
echo "REPO does not exist"
return
fi

#for running multiple seq jobs on single node
module load taskfarmer
