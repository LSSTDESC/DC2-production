
for f in `cat trim.tracts` ; do 
t=`echo ${f%.hdf5} | cut -d "_" -f4`
echo "processing $t"
python convert_merged_tract_to_dpdd.py --tract $t
done
