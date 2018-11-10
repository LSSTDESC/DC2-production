#!/bin/bash

for tract in `cat $1` ; do
echo $tract

cat > batch_$tract.sl <<EOF
#!/bin/bash -l

#SBATCH -N 1
#SBATCH -t 16:00:00
#SBATCH -q regular
#SBATCH -C haswell
#SBATCH --image=lsstdesc/stack-jupyter:w_2018_30-run1.2-v2
#SBATCH -L SCRATCH
#SBATCH -J tract_$tract

pwd
echo "HERE=\$HERE"
echo "SCRIPT_DIR=\${SCRIPT_DIR}"

export THREADS=32

$HERE/run_tract.sh $tract

EOF

#sbatch batch_$tract.sl 

done
