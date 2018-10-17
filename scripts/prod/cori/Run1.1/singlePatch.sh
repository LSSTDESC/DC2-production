
source ${STACK_VERSION}
setup lsst_distrib

OUTPUT_DIR=$PWD
source setup.sh

python $SCRIPT_DIR/example_merge_tract_cat.py
