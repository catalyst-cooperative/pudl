# pudl.transform.ferceqr

Transform FERC Electric Quarterly Report (EQR) data.

This module implements the transformation stage of the PUDL pipeline for FERC EQR data.
Raw EQR data is ingested as quarterly-partitioned Apache Parquet files and transformed
into clean, typed core tables that are written back out as Parquet.

The module is structured in two layers:

Reusable DuckDB transformation helpers operate on [`duckdb.DuckDBPyRelation`](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation)
objects and are composed together inside each Dagster asset definition.

Private expression factory functions that accept a column name (and optional parameters)
and return a [`duckdb.Expression`](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression) suitable for use inside
[`apply_column_transforms()`](#pudl.transform.ferceqr.apply_column_transforms).

Dagster assets apply these helpers to produce four core FERC EQR tables, each of which
is partitioned by `year_quarter`:

- [core_ferceqr_\_quarterly_identity](../../../../data_dictionaries/pudl_db.md#core-ferceqr-quarterly-identity)
- [core_ferceqr_\_transactions](../../../../data_dictionaries/pudl_db.md#core-ferceqr-transactions)
- [core_ferceqr_\_contracts](../../../../data_dictionaries/pudl_db.md#core-ferceqr-contracts)
- [core_ferceqr_\_quarterly_index_pub](../../../../data_dictionaries/pudl_db.md#core-ferceqr-quarterly-index-pub)

## Attributes

| [`logger`](#pudl.transform.ferceqr.logger)   |    |
|----------------------------------------------|----|

## Functions

| [`apply_duckdb_dtypes`](#pudl.transform.ferceqr.apply_duckdb_dtypes)(table_data, table_name, conn)             | Cast each column to the dtype declared in the PUDL metadata schema for the table.      |
|----------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| [`rename_duckdb_columns`](#pudl.transform.ferceqr.rename_duckdb_columns)(→ duckdb.DuckDBPyRelation)            | Rename one or more columns in a DuckDB relation, passing all others through unchanged. |
| [`apply_column_transforms`](#pudl.transform.ferceqr.apply_column_transforms)(→ duckdb.DuckDBPyRelation)        | Apply a DuckDB expression factory to a set of columns, replacing each in place.        |
| [`_yn_to_bool`](#pudl.transform.ferceqr._yn_to_bool)(→ duckdb.Expression)                                      | Return a DuckDB expression that converts `'Y'`/`'N'` strings to booleans.              |
| [`_na_to_null`](#pudl.transform.ferceqr._na_to_null)(→ duckdb.Expression)                                      | Return a DuckDB expression that converts `'N/A'` or `'NA'` strings to NULL.            |
| [`_parse_datetimes`](#pudl.transform.ferceqr._parse_datetimes)(→ duckdb.Expression)                            | Return a DuckDB expression that parses a datetime string column using `fmt`.           |
| [`_recode_categoricals`](#pudl.transform.ferceqr._recode_categoricals)(→ duckdb.Expression)                    | Return a DuckDB expression that replaces exact categorical values in a column.         |
| [`core_ferceqr__quarterly_identity`](#pudl.transform.ferceqr.core_ferceqr__quarterly_identity)(context, ...)   | Transform the raw FERC EQR filer identity table.                                       |
| [`core_ferceqr__transactions`](#pudl.transform.ferceqr.core_ferceqr__transactions)(context, ...)               | Transform the raw FERC EQR electricity transactions table.                             |
| [`core_ferceqr__contracts`](#pudl.transform.ferceqr.core_ferceqr__contracts)(context, raw_ferceqr_\_contracts) | Transform the raw FERC EQR electricity contracts table.                                |
| [`core_ferceqr__quarterly_index_pub`](#pudl.transform.ferceqr.core_ferceqr__quarterly_index_pub)(context, ...) | Transform the raw FERC EQR index price publisher table.                                |

## Module Contents

### pudl.transform.ferceqr.logger

### pudl.transform.ferceqr.apply_duckdb_dtypes(table_data: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection))

Cast each column to the dtype declared in the PUDL metadata schema for the table.

Column types are looked up from the [`pudl.metadata.classes.Resource`](../../metadata/classes/index.md#pudl.metadata.classes.Resource) for
`table_name`. Any custom enum types required by the schema are created in `conn`
before the cast is applied.

* **Parameters:**
  * **table_data** – DuckDB relation whose columns will be cast.
  * **table_name** – PUDL table name used to look up the schema from the metadata.
  * **conn** – DuckDB connection used to register custom enum types as needed.

### pudl.transform.ferceqr.rename_duckdb_columns(table_data: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), mapping: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]) → [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation)

Rename one or more columns in a DuckDB relation, passing all others through unchanged.

* **Parameters:**
  * **table_data** – DuckDB relation containing the columns to rename.
  * **mapping** – Maps existing column names to their new names.

### pudl.transform.ferceqr.apply_column_transforms(table_data: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], transform: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[str](https://docs.python.org/3/library/stdtypes.html#str)], [duckdb.Expression](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression)]) → [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation)

Apply a DuckDB expression factory to a set of columns, replacing each in place.

The `transform` callable is invoked once per column name and must return a
[`duckdb.Expression`](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression) whose result will be aliased back to the original column
name. All columns not listed in *columns* are passed through unchanged.

* **Parameters:**
  * **table_data** – DuckDB relation containing the columns to transform.
  * **columns** – Names of the columns to which `transform` will be applied.
  * **transform** – Callable that accepts a column name and returns a DuckDB Expression
    defining the transformation for that column.

### pudl.transform.ferceqr.\_yn_to_bool(col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [duckdb.Expression](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression)

Return a DuckDB expression that converts `'Y'`/`'N'` strings to booleans.

The comparison is case-insensitive. Any value other than `'Y'` or `'N'` is
mapped to `NULL`.

### pudl.transform.ferceqr.\_na_to_null(col_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [duckdb.Expression](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression)

Return a DuckDB expression that converts `'N/A'` or `'NA'` strings to NULL.

The comparison is case-insensitive. All other values are uppercased and returned
unchanged.

### pudl.transform.ferceqr.\_parse_datetimes(col_name: [str](https://docs.python.org/3/library/stdtypes.html#str), fmt: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [duckdb.Expression](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression)

Return a DuckDB expression that parses a datetime string column using `fmt`.

Uses DuckDB’s `TRY_STRPTIME`, so values that cannot be parsed return `NULL`
rather than raising an error.

### pudl.transform.ferceqr.\_recode_categoricals(col_name: [str](https://docs.python.org/3/library/stdtypes.html#str), replace_mapping: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]) → [duckdb.Expression](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.Expression)

Return a DuckDB expression that replaces exact categorical values in a column.

Generates a `CASE WHEN` expression with one equality branch per entry in
`replace_mapping`. Keys not in the mapping are passed through unchanged via the
`ELSE` clause.

* **Parameters:**
  * **col_name** – Name of the DuckDB column whose values will be recoded.
  * **replace_mapping** – Maps each observed bad value (key) to its correct
    canonical replacement (value).

### pudl.transform.ferceqr.core_ferceqr_\_quarterly_identity(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), raw_ferceqr_\_ident: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR filer identity table.

### pudl.transform.ferceqr.core_ferceqr_\_transactions(context, raw_ferceqr_\_transactions: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR electricity transactions table.

### pudl.transform.ferceqr.core_ferceqr_\_contracts(context, raw_ferceqr_\_contracts: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR electricity contracts table.

### pudl.transform.ferceqr.core_ferceqr_\_quarterly_index_pub(context, raw_ferceqr_\_index_pub: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR index price publisher table.
