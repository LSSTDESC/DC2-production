# 2018-05-01: Wood-Vasey 
#   Running the validate_drp.py LSST SRD analysis is very straightforward.
#   Just run the below against the repository of interest.
#
#   However, this is not memory efficient.
#   This takes several hours just to run on the 66 visits of Run1.1-test2
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.1-test2/output
validateDrp.py ${REPO}
