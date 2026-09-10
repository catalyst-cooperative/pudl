# pudl.dagster.assets.output.databases

Dagster assets that assemble pudl.sqlite and pudl.duckdb from the Parquet outputs.

Once the ETL has written every table to Parquet, [`build_pudl_sqlite_asset()`](#pudl.dagster.assets.output.databases.build_pudl_sqlite_asset) and
[`build_pudl_duckdb_asset()`](#pudl.dagster.assets.output.databases.build_pudl_duckdb_asset) each define a Dagster asset that rebuilds one all-in-one
relational database from those files. `_write_pudl_sqlite()` and
`_write_pudl_duckdb()` do the actual work. They are deliberately two separate
functions rather than one parametrized one: the formats differ in schema, connection
handling, and which constraints get enforced on write, and the `pudl.sqlite` path is
expected to be deleted wholesale once its deprecation period ends.

## Classes

| [`TableWriteErrorInfo`](#pudl.dagster.assets.output.databases.TableWriteErrorInfo)   | A single table's failed write: which table, and the exception that stopped it.   |
|------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`TableWriteReport`](#pudl.dagster.assets.output.databases.TableWriteReport)      | Outcome of building a database: where it landed, and any error reports.          |

## Functions

| [`build_pudl_sqlite_asset`](#pudl.dagster.assets.output.databases.build_pudl_sqlite_asset)(→ dagster.AssetsDefinition)   | Build the `pudl_sqlite` asset. Delete this once `pudl.sqlite` is retired.   |
|--------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`build_pudl_duckdb_asset`](#pudl.dagster.assets.output.databases.build_pudl_duckdb_asset)(→ dagster.AssetsDefinition)   | Build the `pudl_duckdb` asset.                                              |

## Module Contents

### *class* pudl.dagster.assets.output.databases.TableWriteErrorInfo

A single table’s failed write: which table, and the exception that stopped it.

#### table_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Name of the table whose write failed.

#### exception *: [Exception](https://docs.python.org/3/library/exceptions.html#Exception)*

The exception raised while validating or writing the table.

#### \_\_str_\_() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Render as `table_name: ExceptionType: message` for logs and reports.

### *class* pudl.dagster.assets.output.databases.TableWriteReport

Outcome of building a database: where it landed, and any error reports.

#### db_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Path of the database file that was built.

#### row_counts *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

Mapping of table name to number of rows written, per successfully written table.

#### errors *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[TableWriteErrorInfo](#pudl.dagster.assets.output.databases.TableWriteErrorInfo)]* *= []*

One [`TableWriteErrorInfo`](#pudl.dagster.assets.output.databases.TableWriteErrorInfo) per failed table, in the order they failed.

#### *property* failed_tables *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Names of the tables that failed to write.

* **Returns:**
  The `table_name` of every recorded error, in failure order.

#### summary() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Human-readable report of every table that failed to write, and why.

* **Returns:**
  a single `"Wrote all N table(s)."` line when
  nothing failed, otherwise a header line plus one indented line per
  failed table.
* **Return type:**
  A multi-line string

### pudl.dagster.assets.output.databases.build_pudl_sqlite_asset(asset_keys: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)]) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Build the `pudl_sqlite` asset. Delete this once `pudl.sqlite` is retired.

### pudl.dagster.assets.output.databases.build_pudl_duckdb_asset(asset_keys: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)]) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Build the `pudl_duckdb` asset.
