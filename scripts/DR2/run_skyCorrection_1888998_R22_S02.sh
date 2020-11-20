visit=188998
raft=R22
slot=S02
(time skyCorrection.py repo --rerun dr2-calexp:dr2-calexp_skycorr --id visit=${visit} raftName=${raft} detectorName=${slot} --batch-type none --calib /global/cscratch1/sd/descdm/DC2/DR2/repo/CALIB) >& skyCorr_${visit}_${raft}_${slot}.log
