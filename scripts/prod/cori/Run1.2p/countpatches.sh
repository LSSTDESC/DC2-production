
#repo=/global/cscratch1/sd/desc/DC2/data/Run1.1p/Run1.1/output
#repo=/global/cscratch1/sd/desc/DC2/data/Run1.2p/w_2018_30/rerun/coadd-all2
repo=/global/cscratch1/sd/desc/DC2/data/Run1.2p_globus/w_2018_30/rerun/coadd-all2
#repo=/global/cscratch1/sd/desc/DC2/data/Run1.2i/rerun/coadd

echo $repo
for filter in u g r i z y; do 
echo ${filter}
ls $repo/deepCoadd-results/${filter}/* | wc -l 
done
