#/bin/bash

# Author: Michael Wood-Vasey
# Started: 2018-08-23

# Based off script by Merlin Fisher-Levine
# Run at NCSA lsst-dev on 2018-08-23

# Load LSST DM stack at NCSA:
source /software/lsstsw/stack/loadLSST.bash
setup lsst_distrib -t w_2018_33

setup -k -r ${HOME}/local/lsst/obs_base
setup -k -r ${HOME}/local/lsst/obs_lsstCam
setup -k -r ${HOME}/local/lsst/ip_isr

export OMP_NUM_THREADS=2

# Ingest Images
# 204595: i-band visit
# 219976: r-band visit
RAW_DATA_DIR=/scratch/jchiang/DC2/imsim_data/v*-*/
BF_FLAT_DATA_DIR=/project/wmwv/DC2/imsim/bf_flats

LSSTCAM_REPO_DIR=imsim_repo

mkdir ${LSSTCAM_REPO_DIR}
echo "lsst.obs.lsstCam.LsstCamMapper" > ${LSSTCAM_REPO_DIR}/_mapper

# ingestImages.py "${LSSTCAM_REPO_DIR}" /scratch/jchiang/DC2/imsim_data/*/*.fits  --mode=link
ingestImages.py "${LSSTCAM_REPO_DIR}" "${RAW_DATA_DIR}"/*.fits --mode=link
ingestImages.py "${LSSTCAM_REPO_DIR}" "${BF_FLAT_DATA_DIR}"/lsst_a_*.fits --mode=link

# Copy in the gains
cp -pr calibrations "${LSSTCAM_REPO_DIR}"

# Link the reference catalog
ln -s /scratch/jchiang/DC2/ref_cats/ "${LSSTCAM_REPO_DIR}"/ref_cats

# Make BF correction kernel
## PhoSim
# makeBrighterFatterKernel.py "${LSSTCAM_REPO_DIR}" --rerun test --id detector=94 --visit-pairs 1,5 3,9 4,8 10,2 0,11 6,7 -c xcorrCheckRejectLevel=2 doCalcGains=False --clobber-config --clobber-versions
## ImSim
# makeBrighterFatterKernel.py "${LSSTCAM_REPO_DIR}" --rerun test --id detector=0..3 --visit-pairs 161899,161900 -c xcorrCheckRejectLevel=2 doCalcGains=False --clobber-config --clobber-versions

### Copy the BF correction kernel for one amp to all amps:
cd calibrations_one_sensor; python copy_one_amp_bf_kernel_to_all_amps.py; cd -
cp -pr calibrations_one_sensor "${LSSTCAM_REPO_DIR}"/calibrations

# test one 
# processCcd.py $LSSTCAM_REPO_DIR --calib /project/wmwv/DC2/imsim/CALIB --rerun test --id detector=0 visit=204595 -c isr.doBias=False isr.doDark=False
processCcd.py "${LSSTCAM_REPO_DIR}" --rerun no_bf_correction --id detector=0 visit=204595 -c isr.doBias=False isr.doDark=False isr.doFlat=False \
   -j 4 \
   > processCcd_one_no_bf.log 2>&1

processCcd.py "${LSSTCAM_REPO_DIR}" --rerun with_bf_correction --id detector=0 visit=204595 -c isr.doBias=False isr.doDark=False isr.doFlat=False isr.doBrighterFatter=True \
   -j 4 \
   > processCcd_one_with_bf.log 2>&1

# test all
processCcd.py $LSSTCAM_REPO_DIR --rerun no_bf_correction --id visit=204595,219976 -c isr.doBias=False isr.doDark=False isr.doFlat=False isr.doBrighterFatter=False \
    -j 16 \
    > processCcd_all_no_bf.log 2>&1

processCcd.py $LSSTCAM_REPO_DIR --rerun with_bf_correction --id visit=204595,219976 -c isr.doBias=False isr.doDark=False isr.doFlat=False isr.doBrighterFatter=True \
    -j 16 \
    > processCcd_all_with_bf.log 2>&1
