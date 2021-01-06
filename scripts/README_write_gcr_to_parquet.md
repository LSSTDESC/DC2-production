# Using write_gcr_to_parquet.py

This README contains the updated instructions for using `write_gcr_to_parquet.py`.
It was last updated by Yao-Yuan Mao on Jan 5th, 2021.

## General environment setup

### Step 1: Create a new working directory in scratch

Most likely you will want to put the parquet files in a scratch area first,
and then copy them to the actual destination when everything is done.

```bash
cd $SCRATCH
mkdir -p desc/a-new-working-dir
cd desc/a-new-working-dir
```

### Step 2: Clone the needed repos

To make sure that you are using the up-to-date scripts and catalog configs,
you should clone the repos.
If you want to test a specific branch, change the following accordingly.

```bash
git clone --depth=1 git@github.com:LSSTDESC/gcr-catalogs.git
git clone --depth=1 git@github.com:LSSTDESC/DC2-production.git
```

### Step 3: Start up DESC Python environment

```bash
source /global/common/software/lsst/common/miniconda/setup_current_python.sh ""
export PYTHONPATH="gcr-catalogs:$PYTHONPATH"
```

### Step 4 (optional but encouraged): Request an interactive session

```bash
salloc -N 1 -C haswell -q interactive -A m1727 -t 04:00:00
```

### Step 5: Generate Parquet files

Generally speaking you can just run:

```bash
CAT="cosmoDC2_v1.1.4"
python ./DC2-production/scripts/write_gcr_to_parquet.py $CAT --partition
```

But for DPDD-only Object Catalog in Parquet, you will want to change the output filename prefix:

```bash
CAT="dc2_object_run2.2i_dr6_v1"
python ./DC2-production/scripts/write_gcr_to_parquet.py $CAT --output-filename=object_dpdd  --partition
```

If you are running on an interactive session or using a batch job,
you can enable checkpoints to parallelize the job.
Here's an example:

```bash
CAT="dc2_object_run2.2i_dr6_v1"
EXEC="python ./DC2-production/scripts/write_gcr_to_parquet.py $CAT --output-filename=object_dpdd  --partition --checkpoint-dir=checkpoints"

$EXEC &
sleep 30
$EXEC &
sleep 30
$EXEC &
sleep 30
$EXEC
```

Here we are running four instances simultaneously.
The `sleep 30` command is needed so that we don't start Python interpretors all at once,
without it we will likely run into I/O issue.

To check if you are done, go to `checkpoints` and count the numbers of `*.done` and `*.lock` files.

```bash
ls -1 *.done | wc -l
ls -1 *.lock | wc -l
```

The first line should give you the number of files (tracts or healpixels) that you are expecting.
The second line should give you 0.

### Step 6: Clean up and copy

You can now clean up the direcotries that you no longer need.

```bash
rm -rf checkpoints gcr-catalogs DC2-production
```

Use Globus or switch to desc collab account to copy the files to the correct destination.
