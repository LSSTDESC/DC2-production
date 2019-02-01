# 2019-01-15 Michael Wood-Vasey
# The setup to run inside the shifter image when running
# The Jupyter DESC DM stack kernel from the command line.
source /opt/lsst/software/stack/loadLSST.bash ""
setup lsst_distrib
setup -r /opt/lsst/software/stack/obs_lsstCam
setup lsst_sims
export OMP_NUM_THREADS=1

export PYTHONNOUSERSITE=' '

if [ -n "$DESCPYTHONPATH" ]; then
    export PYTHONPATH="$DESCPYTHONPATH:$PYTHONPATH"
    echo "Including user python path: $DESCPYTHONPATH"
fi
