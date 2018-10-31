
pwd
source ${STACK_VERSION}
setup lsst_distrib

OUTPUT_DIR=$PWD
source setup.sh

time python $OUTPUT_DIR/singlePatch.py
