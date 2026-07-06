# pudl.scripts.dbt_helper

A basic CLI to autogenerate dbt data test configurations.

## Attributes

| [`logger`](#pudl.scripts.dbt_helper.logger)         |    |
|-----------------------------------------------------|----|
| [`ALL_TABLES`](#pudl.scripts.dbt_helper.ALL_TABLES) |    |
| [`dbt_helper`](#pudl.scripts.dbt_helper.dbt_helper) |    |

## Classes

| [`UpdateResult`](#pudl.scripts.dbt_helper.UpdateResult)       |                                                                          |
|---------------------------------------------------------------|--------------------------------------------------------------------------|
| [`TableUpdateArgs`](#pudl.scripts.dbt_helper.TableUpdateArgs) | Define a single class to collect the args for all table update commands. |

## Functions

| [`insert_data_source`](#pudl.scripts.dbt_helper.insert_data_source)(→ pathlib.Path)                           | Convert parent_dir and table_name to parent_dir/data_source/table_name.             |
|---------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| [`_get_row_count_csv_path`](#pudl.scripts.dbt_helper._get_row_count_csv_path)(→ pathlib.Path)                 |                                                                                     |
| [`_get_existing_row_counts`](#pudl.scripts.dbt_helper._get_existing_row_counts)(→ pandas.DataFrame)           |                                                                                     |
| [`_calculate_row_counts`](#pudl.scripts.dbt_helper._calculate_row_counts)(→ pandas.DataFrame)                 |                                                                                     |
| [`_combine_row_counts`](#pudl.scripts.dbt_helper._combine_row_counts)(→ pandas.DataFrame)                     |                                                                                     |
| [`_write_row_counts`](#pudl.scripts.dbt_helper._write_row_counts)(row_counts)                                 |                                                                                     |
| [`update_row_counts`](#pudl.scripts.dbt_helper.update_row_counts)(→ UpdateResult)                             | Generate updated row counts per partition and write to csv file within dbt project. |
| [`maybe_schema_from_path`](#pudl.scripts.dbt_helper.maybe_schema_from_path)(→ pudl.dbt_schema.DbtSchema)      | Load DbtSchema that may or may not be located at specified path.                    |
| [`update_table_schema`](#pudl.scripts.dbt_helper.update_table_schema)(→ UpdateResult)                         | Generate and write out a schema.yaml file defining a new or updated table for dbt.  |
| [`_log_update_result`](#pudl.scripts.dbt_helper._log_update_result)(result)                                   |                                                                                     |
| [`_extract_row_count_partitions`](#pudl.scripts.dbt_helper._extract_row_count_partitions)(→ list[str | None]) | Extract partition columns from check_row_counts_per_partition tests in a DbtTable.  |
| [`update_tables`](#pudl.scripts.dbt_helper.update_tables)(tables, clobber, schema, row_counts)                | Add or update dbt schema configs and row count expectations for PUDL tables.        |
| [`validate`](#pudl.scripts.dbt_helper.validate)(→ None)                                                       | Validate a selection of dbt nodes.                                                  |
| [`main`](#pudl.scripts.dbt_helper.main)(→ int)                                                                | Script for auto-generating dbt configuration and migrating existing tests.          |

## Module Contents

### pudl.scripts.dbt_helper.logger

### pudl.scripts.dbt_helper.ALL_TABLES

### pudl.scripts.dbt_helper.insert_data_source(parent_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Convert parent_dir and table_name to parent_dir/data_source/table_name.

Table name must have <layer>_<source>_\_<…> format.

### *class* pudl.scripts.dbt_helper.UpdateResult

Bases: [`tuple`](https://docs.python.org/3/library/stdtypes.html#tuple)

#### success

#### message

### pudl.scripts.dbt_helper.\_get_row_count_csv_path() → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

### pudl.scripts.dbt_helper.\_get_existing_row_counts() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.scripts.dbt_helper.\_calculate_row_counts(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), partition_expr: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.scripts.dbt_helper.\_combine_row_counts(existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), new: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.scripts.dbt_helper.\_write_row_counts(row_counts: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

### pudl.scripts.dbt_helper.update_row_counts(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), dbt_root: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [UpdateResult](#pudl.scripts.dbt_helper.UpdateResult)

Generate updated row counts per partition and write to csv file within dbt project.

### pudl.scripts.dbt_helper.maybe_schema_from_path(path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [pudl.dbt_schema.DbtSchema](../../dbt_schema/index.md#pudl.dbt_schema.DbtSchema)

Load DbtSchema that may or may not be located at specified path.

If file is empty or does not exist, returns blank schema.

### pudl.scripts.dbt_helper.update_table_schema(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), dbt_root: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [UpdateResult](#pudl.scripts.dbt_helper.UpdateResult)

Generate and write out a schema.yaml file defining a new or updated table for dbt.

Incorporates handwritten models and data tests in corresponding schema.human.yml files,
if present.

### pudl.scripts.dbt_helper.\_log_update_result(result: [UpdateResult](#pudl.scripts.dbt_helper.UpdateResult))

### pudl.scripts.dbt_helper.\_extract_row_count_partitions(table: [pudl.dbt_schema.DbtTable](../../dbt_schema/index.md#pudl.dbt_schema.DbtTable)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)]

Extract partition columns from check_row_counts_per_partition tests in a DbtTable.

### *class* pudl.scripts.dbt_helper.TableUpdateArgs

Define a single class to collect the args for all table update commands.

#### tables *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

#### schema *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

#### row_counts *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

#### clobber *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= False*

### pudl.scripts.dbt_helper.update_tables(tables: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], clobber: [bool](https://docs.python.org/3/library/functions.html#bool), schema: [bool](https://docs.python.org/3/library/functions.html#bool), row_counts: [bool](https://docs.python.org/3/library/functions.html#bool))

Add or update dbt schema configs and row count expectations for PUDL tables.

The `tables` argument can be a single table name, a list of table names, or
‘all’. If ‘all’ the script will update configurations for for all PUDL tables.

If `--clobber` is set, existing configurations for tables will be overwritten.
if this does not result in deletions.

### pudl.scripts.dbt_helper.validate(select: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, asset_select: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, exclude: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, dry_run: [bool](https://docs.python.org/3/library/functions.html#bool) = False, override_target: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [None](https://docs.python.org/3/library/constants.html#None)

Validate a selection of dbt nodes.

Wraps the `dbt build` command line so we can annotate the result with the
actual data that was returned from the test query.

Understands how to translate Dagster asset selection syntax into dbt node
selections via the –asset-select flag.

Default behavior if you do not pass –asset-select or –select is to
validate everything.

Usage examples:

Run all the checks for one asset:

> $ dbt_helper validate –asset-select “key:out_eia_\_yearly_generators”

Run the checks for one specific dbt node:

> $ dbt_helper validate –select “source:pudl_dbt.pudl.out_eia_\_yearly_generators”

Run checks for an asset and all its upstream dependencies:

> $ dbt_helper validate –asset-select “+key:out_eia_\_yearly_generators”

Exclude the row count tests:

> $ dbt_helper validate –asset-select “+key:out_eia_\_yearly_generators” –exclude “*check_row_counts*”

### pudl.scripts.dbt_helper.main() → [int](https://docs.python.org/3/library/functions.html#int)

Script for auto-generating dbt configuration and migrating existing tests.

This CLI currently provides the following sub-commands:

update-tables: which can update or create a dbt table (model) schema.yml file under
the `dbt/models` repo. These configuration files tell dbt about the structure of
the table and what data tests are specified for it. The script can also generate or
update the expected row counts for existing tables, assuming they have been
materialized to parquet files and are sitting in your $PUDL_OUTPUT directory.

validate: run validation tests for a selection of dbt nodes.

Run `dbt_helper {command} --help` for detailed usage on each command.

### pudl.scripts.dbt_helper.dbt_helper
