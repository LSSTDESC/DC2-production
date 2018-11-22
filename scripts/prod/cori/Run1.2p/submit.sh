#!/bin/bash

if ! [ -d "$RUNDIR" ] ; then
echo "RUNDIR $RUNDIR missing "
return
fi

njobs=$(wc -l $1)
jobdir=$(readlink -e $RUNDIR)

echo "about to run $njobs jobs in $jobdir: OK(y/n)?"
read ans
if [ $ans != "y" ]; then
return
fi


for tract in `cat $1` ; do
echo $tract

cat > batch_$tract.sl <<EOF
#!/bin/bash -l

#SBATCH -N 1
#SBATCH -t 15:00:00
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

sbatch batch_$tract.sl 

done
