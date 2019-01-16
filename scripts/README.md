General scripts need for tasks related to the DC2 production and
validation can be put in this directory.

### How To Generate Object Tables from DM processing outputs
1. The Run 1.1p Object table summary files were generated with

```
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.1/output
python merged_tract_cat.py ${REPO} 5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429
```

It took many hours.
This can be trivially parallelized with one invocation per tract.  E.g.,

```
python merged_tract_cat.py ${REPO} 5066
python merged_tract_cat.py ${REPO} 5065
...
python merged_tract_cat.py ${REPO} 4429
```

While they are hitting the same filesystem, they are not hitting the same files.

The Run 1.2p files were generated with

```
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-test5-u
python merged_tract_cat.py ${REPO} [...]
```

The Run 1.2i files were generated with by setting up a shifter image:

```
shifter --image=lsstdesc/stack-jupyter:prod
```

Then executing the relevant config file

```
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

Then run
```
REPO=/global/cscratch1/sd/desc/DC2/data/Run1.2i_globus_in2p3_20181217/w_2018_39/rerun/multiband
TRACTS="5066 5065 5064 5063 5062 4852 4851 4850 4849 4848 4640 4639 4638 4637 4636 4433 4432 4431 4430 4429"

WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/object_catalog_new
mkdir -p "${WORKING_DIR}"
cd ${WORKING_DIR}

SCRIPT_DIR=/global/homes/w/wmwv/local/lsst/DC2-production/scripts
nohup python "${SCRIPT_DIR}"/merge_tract_cat.py "${REPO}" "${TRACTS}" > merge_tract_cat.log 2>&1 < /dev/null
```

2. Write a `gcr-catalogs` reader for the new catalog.  Generally this will be as easy as creating a new configuration file with a new base_dir and description.  E.g., the catalog config file for Run 1.2i (https://github.com/LSSTDESC/gcr-catalogs/blob/master/GCRCatalogs/catalog_configs/dc2_object_run1.2i.yaml) is:

```
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

3. Generate "Trimmed" Object Tables
These files have only the columns necessary to reconstruct the DPDD.
They maintain their original column names.  

The respective Object trimmed object tables were generated with

Run 1.1p
```
python trim_tract_cat.py /global/projecta/projectdirs/lsst/global/in2p3/Run1.1/object_catalog/merged_tract_cat_*.hdf5
```

Run 1.2p
```
python trim_tract_cat.py /global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog/merged_tract_cat_*.hdf5
```

Run 1.2i
```
python trim_tract_cat.py WORKING_DIR=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2i/object_catalog_new/object_tract_*.hdf5
```
(Note that the output HDF5 files are named `object_tract_*` instead of `merged_tract_*` as they were for Run 1.1p and 1.2p)

This trim step uses the Generic Catalog Reader to determine the columns required in the trim catalog.

4. Generate DPDD-column only Object Tables in Parquet
Produce stand-alone files with columns named as in the DPDD.
In addition to renaming columns, this also translates to derived columns that are based on several input columns.
For speed, this is done by default on the already trimmed object tables (from the step just above).  But it would be possible to do it directly from the full Object merged_tract_cat files instead.

```
python convert_merged_tract_to_dpdd.py --reader dc2_coadd_run1.1p
python convert_merged_tract_to_dpdd.py --reader dc2_coadd_run1.2p
python convert_merged_tract_to_dpdd.py --reader dc2_coadd_run1.2i
```

This will create individual per-tract Parquet files.  To create a merged Parquet file of all tracts, run the following snippet of code in the relevant `object_catalog` directory:

```
import pandas as pd

file_prefix = 'dpdd_object_tract_'
file_suffix = 'parquet'

out_file = 'dpdd_object.parquet'

tracts = [4429, 4430, 4431, 4432, 4433, 4636, 4637, 4638, 4639, 4640, 4848, 4849, 4850, 4851, 4852, 5062, 5063, 5064, 5065, 5066]

# Write with append for Parquet is not supported as of Pandas 0.23
# So we build up a dataframe from all tracts and then write once
df = None
for tract in tracts:
    data_file = '{}{:d}.{}'.format(file_prefix, tract, file_suffix)
    print("Reading {}".format(data_file))
    this_df = pd.read_parquet(data_file)
    if df is None:
        df = this_df
    else:
        df = df.append(this_df)

df.to_parquet(out_file)
```

