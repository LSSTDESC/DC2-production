#!/bin/bash
#
#setup to run properly Run1.1 scripts on cori
# S.Plaszczynski 9/oct 18
#

#stack version
if [ -z "${STACK_VERSION}" ] ; then
export STACK_VERSION=/global/common/software/lsst/cori-haswell-gcc/stack/w.2018.26_sim2.9.0-v2/loadLSST.bash
fi

if [ -z "${SCRIPT_DIR}" ] ; then
export SCRIPT_DIR=$(readlink -f $PWD/../../..)
fi
#DM output directory
if [ -z "${REPO}" ] ; then
export REPO="/global/cscratch1/sd/desc/DC2/global/in2p3/Run1.1/output"
fi

#output DIR
if [ -z "${WORKING_DIR}" ] ; then
export WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-testSP/summary
fi

echo "STACK_VERSION=${STACK_VERSION}"
echo "WORKING_DIR=${WORKING_DIR}"
echo "SCRIPT_DIR=${SCRIPT_DIR}"
echo "REPO=${REPO}"

#checks
if ! [ -d ${WORKING_DIR} ]; then
echo "WORKING_DIR does not exist"
return
fi
if ! [ -d $REPO ]; then
echo "REPO does not exist"
return
fi

#necessary for running multiples taks on single node
module load taskfarmer
