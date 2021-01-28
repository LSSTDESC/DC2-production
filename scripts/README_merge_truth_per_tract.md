# Using merge_truth_per_tract.py

This README contains the updated instructions for using `merge_truth_per_tract.py`.
It was last updated by Yao-Yuan Mao on Jan 27th, 2021.

## General environment setup

Please follow Steps 1-4 of [`README_write_gcr_to_parquet.md`](README_write_gcr_to_parquet.md)
to prepare the necessary environment.
The remaining of this README will assume you have followed these steps.

## Generate tract-partitioned truth catalogs

**Important Note:** For Run 2.2i, the tract-partitioned summary truth catalogs
of galaxies, star, and SNe are already available at
`/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/truth/tract_partition/raw`.
So you can skip this section and go to the next section on the truth-match catalog
directly.

To generate tract-partitioned truth catalogs, you will first need another script,
`repartition_into_tracts.py`, also available in this repo, and run the following:

```bash
python ./DC2-production/scripts/repartition_into_tracts.py \
  <original_truth_catalogs> \
  -o <output_dir> \
  --skymap-source-repo <repo_name>
```

For example, for converting Run 2.2i galaxy summary catalogs (which is in healpix partition),
one would run:

```bash
mkdir truth_tract_partition
python ./DC2-production/scripts/repartition_into_tracts.py \
  /global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/truth/galtruth/truth_summary_hp*.parquet \
  -o ./truth_tract_partition \
  --skymap-source-repo 2.2i_dr6
```

The skymap source repo should be a name available in
[`desc-dc2-dm-data`](https://github.com/LSSTDESC/desc-dc2-dm-data/blob/master/desc_dc2_dm_data/repos.py).

When you go to `truth_tract_partition`, you will see many subdirectories, each of which corresponds to one tract.

One would need to repeat this process for all truth types (galaxies, stars, SNe).
Once all are complete, one will then merge the output files for each tract by running
`merge_truth_per_tract.py`.

```bash
python ./DC2-production/scripts/merge_truth_per_tract.py ./truth_tract_partition/<tract>
```

Most likely you will want to repeat this for all tracts, and you can do that by supplying a tract list

```bash
cd truth_tract_partition
ls -1 -d * > ../tract_list.txt
cd ..

mkdir raw

python ./DC2-production/scripts/merge_truth_per_tract.py \
  ./truth_tract_partition/{} \
  --output-dir=raw \
  --tract-list=tract_list.txt \
  --n-cores=8
```

Use `--n-cores` to specify how many cores you want to use.

## Generate a truth-match catalog that matches to a specific object catalog

Once you have tract-partitioned truth catalogs, you can match them to object catalog tract-by-tract by running:

```bash
TRUTH_DIR=/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/truth/tract_partition/raw
OBJ_DIR=/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/dpdd/Run2.2i-dr6-v2/object_dpdd_only

mkdir match_dr6_v2

python ./DC2-production/scripts/merge_truth_per_tract.py \
    $TRUTH_DIR/truth_tract4853.parquet \
    --object-catalog-path=$OBJ_DIR/object_dpdd_tract4853.parquet \
    --matching-only \
    --output-dir=match_dr6_v2
```

Make sure you are using the DPDD-only version of the object catalog.
Also note that you need to supply `--matching-only`,
and that the tract numbers of the truth catalog and the object catalog should match.

In most cases, you will want to run this matching on all tracts.
You can do the following:

```
CWD=$(pwd)
TRUTH_DIR=/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/truth/tract_partition/raw
OBJ_DIR=/global/cfs/cdirs/lsst/shared/DC2-prod/Run2.2i/dpdd/Run2.2i-dr6-v2/object_dpdd_only

cd $OBJ_DIR
ls -1 *.parquet | sed 's/[a-z_]*tract\([0-9]*\).parquet/\1/' > $CWD/tract_list.txt
cd $CWD

mkdir match_dr6_v2

python ./DC2-production/scripts/merge_truth_per_tract.py \
    $TRUTH_DIR/truth_tract{}.parquet \
    --object-catalog-path=$OBJ_DIR/object_dpdd_tract{}.parquet \
    --matching-only \
    --output-dir=match_dr6_v2 \
    --tract-list=tract_list.txt \
    --n-cores=8
```

Use Globus or switch to desc collab account to copy the files to the correct destination.
