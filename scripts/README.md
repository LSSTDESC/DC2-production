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

In theory, running this for Run 1.2p should be as easy as:

```
REPO=/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/w_2018_30/rerun/coadd-test5-u
python merged_tract_cat.py ${REPO} [...]
```

Where you'll have to look up what the tracts are for Run 1.2.

Except that probably won't be the exact name of the DM output repository.  You'll want the output rerun of the final coadd photometry.

* Once these are generated, to read them with GCR one would have to write a very lightly updated reader for GCR catalogs.  But it ideally should even work by just specifying a different root path to the same GCR reader.

```
config = {'base_dir': '/global/projecta/projectdirs/lsst/global/in2p3/Run1.2p/object_catalog'}
GCRCatalogs.load_catalog('dc2_coadd_run1.1p', config)
```

2. Generate "Trimmed" Object Tables
These files have only the columns necessary to reconstruct the DPDD.
They maintain their original column names.  

The Run 1.1p Object trimmed object tables were generated with

```
python trim_tract_cat.py /global/projecta/projectdirs/lsst/global/in2p3/Run1.1/object_catalog/merged_tract_cat_*.hdf5
```

This operates directly on the HDF5 files, with no dependence on the Generic Catalog Reader or the DM Stack.

3. Generate DPDD-column only Object Tables in HDF, FITS, and Parquet
Produce stand-alone files with columns named as in the DPDD.
In addition to renaming columns, this also translates to derived columns that are based on several input columns.
For speed, this is done by default on the already trimmed object tables (from the step just above).  But it would be possible to do it directly from the full Object merged_tract_cat files instead.

```
python convert_merged_tract_to_dpdd.py --reader dc2_coadd_run1.1p
```

Will generate these files for all tracts available through the `dc2_coadd_run1.1p` reader. 
