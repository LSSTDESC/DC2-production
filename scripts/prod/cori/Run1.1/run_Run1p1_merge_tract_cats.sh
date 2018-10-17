#!/bin/bash

if [ -z "${STACK_VERSION}" ] ; then
echo "Missing STACK_VERSION. did you sourced setup.sh?"
fi

# DC2 version
source ${STACK_VERSION}
setup lsst_distrib

#
cd ${OUTPUT_DIR}

TRACT=$1

python "${SCRIPT_DIR}"/merge_tract_cat.py "${DM_REPO}" "${TRACT}"
