# Converting an SQLite Table to other forms

## From SQLite to PostgreSQL
### Inputs
* path to sqlite file
* table name
* PostgreSQL schema name
* PostgreSQL table name (defaults to sqlite tablename)

### PostgreSQL prerequisites

The database is called `desc_dc2_drp`.  In order to make changes to the database you must have access to the account `desc_dc2_drp_admin`.  By default PostgreSQL applications like the command-line tool psql will look for account information  in the file `.pgpass` in your home directory, readable (and writable) only by you.  Then you can invoke psql as follows:
```
$ psql desc_dc2_drp desc_dc2_drp_admin
psql (10.15, server 9.6.19)
Type "help" for help.

desc_dc2_drp=>
```
For more information about psql see [https://www.postgresql.org/docs/9.6/app-psql.html](https://www.postgresql.org/docs/9.6/app-psql.html).

See also [Getting Started with PostgreSQL at NERSC](https://confluence.slac.stanford.edu/x/s4joE), but note that details for authentication in that document pertain to the read-only user account and will have to be adjusted to log in as `desc_dc2_drp_admin`.

### Creating the PostgreSQL schema

From the SQLite table schema make an equivalent .sql file, say called `my_create.sql`, to create the PostgreSQL table. (For the time being, "make" probably means "create in a text editor".) You can see an SQLite schema from within the SQLite command line program by typing
```
sqlite> .schema
```
Here is the correspondence between SQLite type names and PostgreSQL type names for those types occuring in truth catalogs:

| SQLite         | PostgreSQL       |
|----------------|------------------|
| TEXT           | TEXT             |
| INTEGER or INT | INTEGER          |
| BIGINT         | BIGINT           |
| FLOAT          | REAL             |
| DOUBLE         | DOUBLE PRECISION |

If the original table has ra,dec columns, add a column of type `earth` (a typical name for such a column is `coord` or something containing that string) to the PostgreSQL table definition. Normally the table should be created within a schema other than the default `public` schema.
**NOTE:** For PostgreSQL, "schema" has another special meaning in addition to the usual meaning of "description of structure of database tables".  The keyword `SCHEMA` refers to something which acts like a namespace.  It qualifies the table name.

Here is a typical file, used to create the truth summary table for agn:

```
CREATE SCHEMA IF NOT EXISTS agn_truth;
CREATE TABLE agn_truth.truth_summary
        (id TEXT, host_galaxy BIGINT, ra DOUBLE PRECISION, dec DOUBLE PRECISION,
        redshift REAL, is_variable INTEGER, is_pointsource INTEGER,
        flux_u REAL, flux_g REAL, flux_r REAL,
        flux_i REAL, flux_z REAL, flux_y REAL,
        flux_u_noMW REAL, flux_g_noMW REAL, flux_r_noMW REAL,
        flux_i_noMW REAL, flux_z_noMW REAL, flux_y_noMW REAL,
	coord earth);
```

All of this (generation of file `my_create.sql`) could perhaps be automated but if so, review output.

Run it from psql (that is, execute the lines in the file as if entered at the psql prompt) like this:

```
desc_dc2_drp=> \i my_create.sql;
```
### Extract data to csv
Write an sql script to extract the data, to be executed from inside the sqlite command program. Here is one for summary_truth:

```sql
.headers on
.mode csv
select * from truth_summary;
```
Suppose the path to this file is  `/tool/path/extract.sql`, The path to sqlite input file is `/input/path/sqlite_file.db` and the path to output csv file is `/output/path/out_table.csv`. Then use it like this:
```
$ sqlite3 /input/path/sqlite_file.db
sqlite> .output /output/path/out_table.csv
sqlite> .read /tool/path/extract.sql
```

Alternatively, embed a line like the following in a shell script, e.g., assuming variables have been appropriately defined earlier in the script or in the environment before it was invoked

```
sqlite3 -header -csv ${SRC_FILE} "select * from table_name;" > ${DEST_CSV}
```
### Ingest csv file into PostgreSQL
Create an .sql file to ingest the data from the created .csv file. For each table, you'll need a command something like
```
\copy pg-schema-name.pg-table-name (list-of-column-names) from '/output/path/out_table.csv' with (FORMAT 'csv' 'header');
```
where `list-of-column-names` should be those names in the SQLite schema for the table. Run it from psql.

### Update
For tables with ra, dec columns (for truth tables, only truth_summary) update any values of type `earth` (can be computed from ra, dec) with an sql command something like
```sql
update schema-name.table-name set coord=public.radec_to_coord(ra-col, dec-col);
```
e.g. for star truth
```sql
update star_truth.truth_summary set coord=public.radec_to_coord(ra, dec);
```
### Create indices
`id` is a unique index for all but variability tables. Any columns of type `earth` should also be indexed. For variability tables, index at least `id` and `obsHistID`. (**NOTE:** PostgreSQL ignores case so the column may appear as `obshistid`)

### Update permissions
Finally, for all schemas, all tables, give read permission to user desc_dc2_drp_user. For a schema named `the_schema` this can be done from psql with these two commands:

```
desc_dc2_drp=> grant usage on schema the_schema to desc_dc2_drp_user;
desc_dc2_drp=> grant select on all tables in schema the_schema to desc_dc2_drp_user;
```
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
$ python parquet_utils.py agn_truth_cat.db --pqfile agn_truth_summary.parquet --table truth_summary
```

**Note:** That the repo `sims_TruthCatalog` requires the stack, but the script does not.  You can copy the files `parquet_utils.py` and `script_utils.py` from that repo to another area, modify the statement

```python
from desc.sims_truthcatalog.script_utils import print_callinfo
```
in the first file appropriately, and then run it using any of the standard desc kernels.

## Validation
Output should be identical to input except for differences in floating point numbers within tolerance.

* Confirm count is identical
* For a small sample check (handful) that floating point value differences are within tolerance and all other values are identical
* For tables with ra,dec compare plots
* For other numerical quantities - not necessarily all, but a selection - compare histograms

Some of these checks are done in the notebook verify_truth in this repo.
