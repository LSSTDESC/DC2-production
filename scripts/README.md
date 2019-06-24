General scripts need for tasks related to the DC2 production and
validation can be put in this directory.

## How To Generate Object Tables from DM processing outputs
*by Michael Wood-Vasey [@wmwv]*

### Outline
Our goal is to provide an interface to the DESC DC2 static-sky data that resembles the LSST Data Products Definition Document
https://ls.st/dpdd
In addition, we aim to provide access to intermediate quantities that may not appear in the final DPDD.

We start with a data set that has been processed by the LSST DM Science Pipelines through to coadd + forced-photometry.

1. Create Summary files of the set of Objects, their original detection measurements, and their forced-photometry measurements on the coadded stacks in each filter and save in HDF5 files on a per-tract basis.
2. Create a smaller, Trimmed, version of these files that only contains the columns necessary for the DPDD.
3. Provide access to these data through the `GCRCatalogs` framework (https://github.com/yymao/generic-catalog-reader) using `gcr-catalogs`, which is the DESC-customized set of catalog formats (https://github.com/LSSTDESC/gcr-catalogs).
4. Create a Parquet file modeled after the DPDD Object Table.  This Parquet file is meant to be portable and usable without the requirement of any external additional LSST DM or DESC-specific infrastructure.

### Environment configuration
#### Basic dependencies

1. LSST Science Pipelines
    - Reading the LSST DM Science Pipelines data requires an installation of the LSST DM Science Pipelines stack.
2. `pandas`, `astropy`, and `tables`.
    - The first two will already be present with the LSST Science Pipelines install.  The slightly generically named `tables` Python package is necessary to fully use the HDF5 functionality of `pandas` and `h5py`.
3. `GCRCatalogs` and `gcr-catalogs`
    - [GCRCatalogs](https://github.com/yymao/generic-catalog-reader) presents a general abstract API that can be used to access disparate datasets.
    - [gcr-catalogs](https://github.com/LSSTDESC/gcr-catalogs) presents the specific definitions of the catalogs used in the DESC DC2 effort and installed at NESRC.
4. Data
    - You'll need to run these at a place that has all of the data.  Right now that's either DOE's NERSC or the IN2P3 Computing Center.  Perhaps this seems too obvious to need mentioning, but I do to clarify the instructions in this document have been written from the perspective of being at NERSC.

#### Shifter image of DM+DESC environment
I used the same shifter image DESC uses for the NERSC Jupyter Hub `desc-stack` kernel.  I ran `shifter` from the command line

```bash
shifter --image=lsstdesc/stack-jupyter:prod
```

and then ran the configuration and subsequent lines below from that the shifter session.
```bash
. setup_shifter_env.sh
```

Which contains

```bash
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
```

##### Command-line invocation of Jupyter Kernel desc-stack environment
The same behavior was obtained for re-running Run 1.2p using
scripts/start-kernel-cli.py
from the
https://github.com/LSSTDESC/nersc
package (checked out into `~wmwv/local/lsst/nersc`)

```
python ~/local/lsst/nersc/scripts/start-kernel-cli.py desc-stack
```

#### Script directory
The commands run here are from the `DC2-Production/scripts` directory.  The notes in this file using a `SCRIPT_DIR` environment variable to point to this location.  I ran these as I was developing the scripts, so my `SCRIPT_DIR` points to my local checkout:

```bash
SCRIPT_DIR=/global/homes/w/wmwv/local/lsst/DC2-production/scripts
```

### Make Summary Files

#### Run 1.1p

```bash
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.1/output
python "${SCRIPT_DIR}"/merged_tract_cat.py ${REPO} 5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429
```

It took many hours.
This can be trivially parallelized with one invocation per tract.  E.g.,

```bash
python "${SCRIPT_DIR}"/merged_tract_cat.py ${REPO} 5066
python "${SCRIPT_DIR}"/merged_tract_cat.py ${REPO} 5065
...
python "${SCRIPT_DIR}"/merged_tract_cat.py ${REPO} 4429
```

While they are hitting the same filesystem, they are not hitting the same files.

#### Run 1.2p

```bash
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-test5-u
python "${SCRIPT_DIR}"/merged_tract_cat.py ${REPO} [...]
```

#### Run 1.2i
Then run
```bash
REPO=/global/cscratch1/sd/desc/DC2/data/Run1.2i_globus_in2p3_20181217/w_2018_39/rerun/multiband
TRACTS="5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429"

WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/object_catalog_new
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/w/wmwv/local/lsst/DC2-production/scripts
nohup python "${SCRIPT_DIR}"/merge_tract_cat.py "${REPO}" ${TRACTS} > merge_tract_cat.log 2>&1 < /dev/null
```

#### Re-generating Run 1.2p v4

Then run
```bash
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_39/rerun/coadd-v4
TRACTS="5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429"

WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog_new
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/w/wmwv/local/lsst/DC2-production/scripts
nohup python "${SCRIPT_DIR}"/merge_tract_cat.py "${REPO}" ${TRACTS} > merge_tract_cat_${TRACT}.log 2>&1 < /dev/null
```

### Make Trimmed Files

Generate "Trimmed" Object Tables
These files have only the columns necessary to reconstruct the DPDD.
They maintain their original column names.

The respective Object trimmed object tables were generated with

Run 1.1p
```bash
python  "${SCRIPT_DIR}"/trim_tract_cat.py /global/projecta/projectdirs/lsst/global/in2p3/Run1.1/object_catalog/merged_tract_cat_*.hdf5
```

Run 1.2p (v3)
```bash
python "${SCRIPT_DIR}"/trim_tract_cat.py /global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog/merged_tract_cat_*.hdf5
```

Run 1.2i
```bash
python "${SCRIPT_DIR}"/trim_tract_cat.py WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/object_catalog_new/object_tract_*.hdf5
```
(Note that the output HDF5 files are named `object_tract_*` instead of `merged_tract_*` as they were for Run 1.1p and 1.2p)

This trim step uses the Generic Catalog Reader to determine the columns required in the trim catalog.  We can do this before generating the actual catalog reader configuration file because the basic quantities we need are constant across the DC2 Runs.

Run 1.2p v4
```bash
python "${SCRIPT_DIR}"/trim_tract_cat.py /global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog_v4/object_tract_cat_*.hdf5
```


### Update gcr-catalog

Write a `gcr-catalogs` reader for the new catalog.  Generally this will be as easy as creating a new configuration file with a new base_dir and description.  E.g., the catalog config file for Run 1.2i (https://github.com/LSSTDESC/gcr-catalogs/blob/master/GCRCatalogs/catalog_configs/dc2_object_run1.2i.yaml) is:

```yaml
subclass_name: dc2_object.DC2ObjectCatalog
base_dir: /global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/object_catalog_new
schema_filename: trim_schema.yaml
filename_pattern: 'trim_object_tract_\d+\.hdf5$'
description: DC2 Run 1.2i Object Catalog
creators: ['Michael Wood-Vasey']
included_by_default: true
pixel_scale: 0.2
```

Note:
`pixel_scale` above refers to choice of pixel scale for the coadd, not necessarily the native instrument itself.
Make sure to check the pixel scale used by the sky map to generate the coadds.
A quick way to do this is to look at the CD1_1, CD2_2 values in a sample coadded image.
These values are in degrees/pixel, so a value of 5.5555555555e-05 means 0.2"/pixel.

#### Generate Schema files

To save load time, we generate a schema file that tells the GCRCatalog exactly what's in the file.
We can generate this the first time with (for example for Run 1.2i):

```python
import GCRCatalogs

for cat in ('dc2_object_run1.2i', 'dc2_object_run1.2i_all_columns'):
    GCRCatalogs.load_catalog(cat).generate_schema_yaml()
```

When given no schema the reader goes through and figures out all of the available columns.
The schema file is then saved to the same base directory as the catalog files.
If the schema file already exists it won't be overwritten.

After the schema file is created will be picked up by the reader and loading should be much faster.

#### Make DPDD Parquet files

Produce stand-alone files with columns named as in the DPDD.

In addition to renaming columns, this also translates to derived columns that are based on several input columns.
For speed, this is done by default on the already trimmed object tables (from the step just above).  But it would be possible to do it directly from the full Object merged_tract_cat files instead.

```bash
python "${SCRIPT_DIR}"/convert_merged_tract_to_dpdd.py --reader dc2_object_run1.1p
python "${SCRIPT_DIR}"/convert_merged_tract_to_dpdd.py --reader dc2_object_run1.2p
python "${SCRIPT_DIR}"/convert_merged_tract_to_dpdd.py --reader dc2_object_run1.2i
```

This will create individual per-tract Parquet files.  To create a merged Parquet file of all tracts, use the very simple `merge_parquet_files.py` utility.  E.g.,

```bash
python "${SCRIPT_DIR}"/merge_parquet_files.py dpdd_object_tract_????.parquet --output_file dpdd_dc2_object_run1.2i.parquet
```

## How To Generate Source Tables from DM processing outputs
*by Michael Wood-Vasey [@wmwv]*

### Outline
Provide the DESC DC2 individual-image catalog data in the form defined by the LSST Data Products Definition Document
https://ls.st/dpdd

We start with a data set that has been processed by the LSST DM Science Pipelines through to coadd + forced-photometry.

1. Create a summary file of the source catalog for each visit where Sources have been matched to the nearest Object in the Object Table.
2. Provide access to these data through the `GCRCatalogs` framework (https://github.com/yymao/generic-catalog-reader) using `gcr-catalogs`, which is the DESC-customized set of catalog formats (https://github.com/LSSTDESC/gcr-catalogs).
3. Create a Parquet file modeled after the DPDD Object Table.  This Parquet file is meant to be portable and usable without the requirement of any external additional LSST DM or even DESC-specific infrastructure.

### Environment configuration
Same requirements, Shifter, and data requirements as for the Object Tables.

### Make Source Files

#### Run 1.2i

There are 1995 visits in Run 1.2.  The script to extract a source catalog from a visit, match to nearest object (within a matching radius of 1 arcsec), extract the calibration, and write out to a Parquet file is `merge_source_cat.py`.  The shell script `extract_source_table.sh` provides some light wrapping around `merge_source_cat.py` to setup some environment variables.

Each source catalog takes 2-3 minutes, but as there are almost 2000 it takes a long time in aggregate.  The SLURM job script to run all of them uses the `taskfarmer` module that takes a list of tasks and goes through them.

```bash
module load taskfarmer
sbatch extract_source_table_taskfarmer.sl
```

This will create a set of ~2,000 files in `${SCRATCH}/DC2/Run1.2i`.

### Update gcr-catalog

Write a `gcr-catalogs` reader for the new catalog.  Generally this will be as easy as creating a new configuration file with a new base_dir and description.  E.g., the source catalog config file for Run 1.2i (https://github.com/LSSTDESC/gcr-catalogs/blob/master/GCRCatalogs/catalog_configs/dc2_source_run1.2i.yaml) is:

```yaml
subclass_name: dc2_source.DC2SourceCatalog
base_dir: /global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/source_catalog
schema_filename: src_schema.yaml
filename_pattern: 'src_visit_\d+\.parquet$'
description: DC2 Run 1.2i Source Catalog
creators: ['Michael Wood-Vasey']
included_by_default: true
```

#### Generate Schema files

To save load time, we generate a schema file that tells the GCRCatalog exactly what's in the files.
We can generate this the first time with (for example for Run 1.2i):

```python
import GCRCatalogs
import os

base_dir = os.path.join(os.env('SCRATCH'), 'DC2', 'Run1.2i')
for reader in ('dc2_source_run1.2i'):
    cat = GCRCatalogs.load_catalog(reader, config_overwrite={'base_dir': base_dir})
    cat.generate_schema_yaml()
```

The schema file gets created in the `base_dir`.  Since we're creating

#### Copy to central location

The files provided to the collaboration are in a shared space owned by the `desc` user.  Make sure the files you just created are readable by the `lsst` group and then copy in

```bash
chgrp -R lsst ${SCRATCH}/DC2/Run1.2i
collabsu desc

cp -pr /global/cscratch1/sd/wmwv/DC2/Run1.2i/src_visit /global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/source_catalog
```

Where the above `/global/cscratch1/sd/wmwv/DC2/Run1.2i/src_visit` is my `${SCRATCH}/DC2/Run1.2i`.  We have to explicitly spell out the pathname because once we switch to the `desc` user, the `${SCRATCH}` variable will now be that of the `desc` user intead of the user who ran the job to create the files.

### Make Forced Source Files

#### Run 1.2p

There are 1995 visits in Run 1.2.  The script to extract a forced source catalog from a visit and write out to a Parquet file is `merge_forced_source_cat.py`.  The shell script `extract_source_table_run1.2p.sh` provides some light wrapping around `merge_forced_source_cat.py` to set up some environment variables.

It takes 18 minutes on 8 nodes running 16 processes per node.  The SLURM job script to run all of them uses the `taskfarmer` module that takes a list of tasks and goes through them.

```bash
module load taskfarmer
sbatch extract_forced_source_table_taskfarmer_run1.2p.sl
```

This will create a set of ~1,200 files in `${SCRATCH}/DC2/Run1.2p/forced_src_visit`.

### Update gcr-catalog

Write a `gcr-catalogs` reader for the new catalog.  Generally this will be as easy as creating a new configuration file with a new base_dir and description.  E.g., the forced source catalog config file for Run 1.2p (https://github.com/LSSTDESC/gcr-catalogs/blob/master/GCRCatalogs/catalog_configs/dc2_forced_source_run1.2p.yaml) is:

```yaml
subclass_name: dc2_source.DC2ForcedSourceCatalog
base_dir: /global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/source_catalog
schema_filename: forced_src_schema.yaml
filename_pattern: 'forced_src_visit_\d+\.parquet$'
description: DC2 Run 1.2p Forced Source Catalog
creators: ['Michael Wood-Vasey']
included_by_default: true
```

#### Generate Schema files

To save load time, we generate a schema file that tells the GCRCatalog exactly what's in the files.
We can generate this the first time with (for example for Run 1.2p):

```python
import GCRCatalogs
import os

base_dir = os.path.join(os.getenv('SCRATCH'), 'DC2', 'Run1.2p', 'forced_src_visit')
reader = 'dc2_forced_source_run1.2p'
cat = GCRCatalogs.load_catalog(reader, config_overwrite={'base_dir': base_dir})
cat.generate_schema_yaml()
```

The schema file gets created in the `base_dir`.  Since we're creating this, we need permission to write to it so we need to do it know while it's still in our user-controlled directory and before we copy it to a central location.

#### Copy to central location

The files provided to the collaboration are in a shared space owned by the `desc` user.  Make sure the files you just created are readable by the `lsst` group and then copy in

```bash
chgrp -R lsst ${SCRATCH}/DC2/Run1.2p/forced_src_visit
collabsu desc

cp -pr /global/cscratch1/sd/wmwv/DC2/Run1.2p/forced_src_visit /global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/forced_source_catalog
```

Where the above `/global/cscratch1/sd/wmwv/DC2/Run1.2p/forced_src_visit` is my `${SCRATCH}/DC2/Run1.2p/forced_src_visit`.  We have to explicitly spell out the pathname because once we switch to the `desc` user, the `${SCRATCH}` variable will now be that of the `desc` user intead of the user who ran the job to create the files.

## How To Generate DIASource + DIAObject Tables from DM processing outputs
*by Michael Wood-Vasey [@wmwv]*

DIA generation and the relationship between the DIASource and DIAObject tables is fundamentally different than for the standard Source and OBject tables and so we document their creation separately in this section.  DIAObjects are created by association of DIASource detections, whereas the Object table which is created by detections on the coadded images in a process completely separate from the creation of the Source tables.  Thus DIASource and DIAObject are more intimately related, which is reflected by the related processing below.

### Make DIASource Files

#### Run 1.2p test

JUST RUNNING ON A TEST SET OF DATA.

Adapted `merge_source_cat.py` to accept a `--dataset` option to specify the `deepDiff_diaSrc` instead of `src.
Also using `--object_dataset` to match to diaObjectIds directly from the Butler.  We'll create DIA Object Table later.

```
bash ${SCRIPT_DIR}/extract_dia_source_table_run1.2p_test.sh ${SCRIPT_DIR}/run_1.2_visits.txt
```

With one patch of data, the extraction runs in about an hour.  This is quickly enough that we can just simply run it as a script instead of scheduling it as a set of jobs.

### Update gcr-catalog

Write a `gcr-catalogs` reader for the new catalog.  Generally this will be as easy as creating a new configuration file with a new base_dir and description.  E.g., the source catalog config file for Run 1.2p (https://github.com/LSSTDESC/gcr-catalogs/blob/master/GCRCatalogs/catalog_configs/dc2_dia_source_run1.2p_test.yaml) is:

```yaml
subclass_name: dc2_dia_source.DC2DiaSourceCatalog
base_dir: /global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/dia_source_catalog_test
schema_filename: schema.yaml
filename_pattern: 'dia_src_visit_\d+\.parquet$'
description: DC2 Run 1.2p DIA Source Catalog test patch
creators: ['Michael Wood-Vasey']
included_by_default: true
```

#### Generate Schema files

To save load time, we generate a schema file that tells the GCRCatalog exactly what's in the files.
We can generate this the first time with (for example for Run 1.2p):

```python
import GCRCatalogs
import os

base_dir = os.path.join(os.getenv('SCRATCH'), 'DC2', 'Run1.2p', 'dia_src_visit')
reader = 'dc2_dia_source_run1.2p_test'
cat = GCRCatalogs.load_catalog(reader, config_overwrite={'base_dir': base_dir})
cat.generate_schema_yaml()
```

The schema file gets created in the `base_dir`.  Since we're creating those filew they're still owned by us.  Next we'll use the 'desc' user to ihnstall them in a central environment.

#### Copy to central location

The files provided to the collaboration are in a shared space owned by the `desc` user.  Make sure the files you just created are readable by the `lsst` group and then copy in

```bash
chgrp -R lsst ${SCRATCH}/DC2/Run1.2p/dia_src_visit
collabsu desc

cp -pr /global/cscratch1/sd/wmwv/DC2/Run1.2p/dia_src_visit /global/projecta/projectdirs/lsst/production/DC2_PhoSim/Run1.2p/dpdd/dia_source_catalog_test
```

Where the above `/global/cscratch1/sd/wmwv/DC2/Run1.2p/` is my `${SCRATCH}/DC2/Run1.2p`.  We have to explicitly spell out the pathname because once we switch to the `desc` user, the `${SCRATCH}` variable will now be that of the `desc` user intead of the user who ran the job to create the files.

### Make DIAObject Files

Wrote new `merge_dia_object_cat.py`.  Relies on DIA Source table to already exist as a GCR product to calculate summary statistics.
```
bash ${SCRIPT_DIR}/extract_dia_object_table_run1.2p_test.sh
```

*** WARNING *** Due to very inefficient processing of the DIA Source files and the simplistic single-threading, the creation of the DIA Object for just this one patch takes 36 hours!.

### Update gcr-catalog

```yaml
cat GCRCatalogs/catalog_configs/dc2_dia_object_run1.2p_test.yaml
subclass_name: dc2_dia_object.DC2DiaObjectCatalog
base_dir: /global/projecta/projectdirs/lsst/production/DC2_PhoSim/Run1.2p/dpdd/dia_object_catalog_test
schema_filename: schema.yaml
filename_pattern: 'dia_object_tract_\d+\.parquet$'
description: DC2 Run 1.2p DIA Object Catalog test patch
creators: ['Michael Wood-Vasey']
included_by_default: true
```

#### Generate Schema files

To save load time, we generate a schema file that tells the GCRCatalog exactly what's in the files.
We can generate this the first time with (for example for Run 1.2p):

```python
import GCRCatalogs
import os

base_dir = os.path.join(os.getenv('SCRATCH'), 'DC2', 'Run1.2p', 'dia_object')
reader = 'dc2_dia_object_run1.2p_test'
cat = GCRCatalogs.load_catalog(reader, config_overwrite={'base_dir': base_dir})
cat.generate_schema_yaml()
```

The schema file gets created in the `base_dir`.  Since we're creating those filew they're still owned by us.  Next we'll use the 'desc' user to ihnstall them in a central environment.

#### Copy to central location

The files provided to the collaboration are in a shared space owned by the `desc` user.  Make sure the files you just created are readable by the `lsst` group and then copy in

```bash
chgrp -R lsst ${SCRATCH}/DC2/Run1.2p/dia_object
collabsu desc

cp -pr /global/cscratch1/sd/wmwv/DC2/Run1.2p/dia_object /global/projecta/projectdirs/lsst/production/DC2_PhoSim/Run1.2p/dpdd/dia_object_catalog_test
```
