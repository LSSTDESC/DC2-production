# u
#visit=2249
#(time skyCorrection.py repo --rerun dr2-calexp --id visit=${visit} --batch-type none --procs 4) >& skyCorr_${visit}.log
# g
visit=159496
(time skyCorrection.py repo --rerun dr2-calexp --id visit=${visit} --batch-type none) >& skyCorr_${visit}.log&
# r
visit=181866
(time skyCorrection.py repo --rerun dr2-calexp --id visit=${visit} --batch-type none) >& skyCorr_${visit}.log&
# i
#visit=174551
#(time skyCorrection.py repo --rerun dr2-calexp --id visit=${visit} --batch-type none) >& skyCorr_${visit}.log
# z
visit=13271
(time skyCorrection.py repo --rerun dr2-calexp --id visit=${visit} --batch-type none) >& skyCorr_${visit}.log&
# y
#visit=5898
#(time skyCorrection.py repo --rerun dr2-calexp --id visit=${visit} --batch-type none) >& skyCorr_${visit}.log
