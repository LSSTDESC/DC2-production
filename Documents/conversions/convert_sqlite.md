# Converting an SQLite Table to other forms

## From SQLite to PostgreSQL
### Inputs
* path to sqlite file
* table name
* PostgreSQL schema name
* PostgreSQL table name (defaults to sqlite tablename)

### Creating the PostgreSQL schema

From the SQLite table schema make equivalent .sql file to create PostgreSQL table.  For now, do this by hand to ensure type names are correct. If original table has ra,dec columns, add a column of type `earth` (typical for such a column is `coord` or contains that string) to the PostgreSQL schema. All of this could perhaps be automated but if so review output.

Run it from psql:

```
desc_dc2_drp=> \i my_create.sql;
```
### Extract data to csv
Write sql script to extract the data, to be executed from inside sqlite command program. Here is one for summary_truth:

```sql
.headers on
.mode csv
select * from truth_summary;
```
Suppose the path to this file is  `/tool/path/extract.sql`, path to sqlite input file is `/input/path/sqlite_file.db` and path to output csv file is `/output/path/out_table.csv`. Then use it like this:
```
$ sqlite3 /input/path/sqlite_file.db
sqlite> .output /output/path/out_table.csv
sqlite> .read /tool/path/extract.sql
```

Alternatively, embed in shell script, e.g., assuming variables have been appropriately defined earlier in the script or in the environment before it was invoked

```
sqlite3 -header -csv ${SRC_FILE} "select * from table_name;" > ${DEST_CSV}
```
### Ingest csv file into PostgreSQL
Create .sql file to ingest the data from the created .csv file. For each table, need a command something like
```
\copy pg-schema-name.pg-table-name (list-of-column-names) from '/output/path/out_table.csv' with (FORMAT 'csv' 'header');
```
Run it from psql

### Update
For tables with ra, dec columns (for truth tables, only truth_summary) update any values of type `earth` with an sql command something like
```
update schema-name.table-name set coord=public.radec_to_coord(ra-col, dec-col);
```
e.g. for star truth
```
update star_truth.truth_summary set coord=public.radec_to_coord(ra, dec);
```
### Create indices
`id` is a unique index for all but variability tables. Any columns of type `earth` should also be indexed. For variability tables, index at least `id` and `obsHistID`. (**NOTE:** PostgreSQL ignores case so the column may appear as `obshistid`)

### Update permissions
Finally, for all schemas, all tables give read permission to user desc_dc2_drp_user

## From SQLite to Parquet
See the [module-level script parquet-utils](https://github.com/LSSTDESC/sims_TruthCatalog/blob/master/python/desc/sims_truthcatalog/parquet_utils.py) in repo `sims_TruthCatalog`.  Here is the command-line help:

```
usage: parquet_utils.py [-h] [--pqfile PQFILE] [--table TABLE]
                        [--n-group N_GROUP]
                        [--max-group-gbyte MAX_GROUP_GBYTE] [--check] [--dry]
                        [--verbose] [--id-column ID_COLUMN]
                        [--n-check N_CHECK]
                        dbfile

Write parquet with content from sqlite or compare existing files

positional arguments:
  dbfile                input sqlite file; required

optional arguments:
  -h, --help            show this help message and exit
  --pqfile PQFILE       parquet filepath. Defaults to test.parquet if output
  --table TABLE         table to be written or compared parquet
  --n-group N_GROUP     number of row groups. By default compute from
                        max_group_gbyte
  --max-group-gbyte MAX_GROUP_GBYTE
                        max allowed (sqlite input) size in gbytes of a row
                        group
  --check               If set, compare sqlite and parquet
  --dry                 If set describe output without creating it
  --verbose             Print more; may be useful for debugging
  --id-column ID_COLUMN
  --n-check N_CHECK     Number of rows from sqlite to check. Ignored in no
                        check or id_colume is None
```

A typical invocation looks like:

```
python parquet_utils.py agn_truth_cat.db --pqfile agn_truth_summary.parquet --table truth_summary
```

## Validation
Output should be identical to input except for differences in floating point numbers within tolerance.

* Confirm count is identical
* For a small sample check (handful) that floating point value differences are within tolerance and all other values are identical
* For tables with ra,dec compare plots
* For other numerical quantities - not necessarily all, but a selection - compare histograms

Some of these checks are done in the notebook verify_truth in this repo.
