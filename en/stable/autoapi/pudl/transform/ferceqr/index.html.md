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

- [core_ferceqr_\_quarterly_identity](../../../../data_dictionaries/pudl_db.html.md#core-ferceqr-quarterly-identity)
- [core_ferceqr_\_transactions](../../../../data_dictionaries/pudl_db.html.md#core-ferceqr-transactions)
- [core_ferceqr_\_contracts](../../../../data_dictionaries/pudl_db.html.md#core-ferceqr-contracts)
- [core_ferceqr_\_quarterly_index_pub](../../../../data_dictionaries/pudl_db.html.md#core-ferceqr-quarterly-index-pub)

## Attributes

| [`logger`](#pudl.transform.ferceqr.logger)                        |                                                                            |
|--------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| [`_EXTRACTION_STATS_ASSET_KEY`](#pudl.transform.ferceqr._EXTRACTION_STATS_ASSET_KEY)   |                                                                            |
| [`_EXTRACTION_STATS_FETCH_LIMIT`](#pudl.transform.ferceqr._EXTRACTION_STATS_FETCH_LIMIT) | Comfortably more than the number of ferceqr quarters, even with some       |
| [`_CORE_FERCEQR_TABLE_LABELS`](#pudl.transform.ferceqr._CORE_FERCEQR_TABLE_LABELS)    |                                                                            |
| [`_CHECK_EVALUATION_FETCH_LIMIT`](#pudl.transform.ferceqr._CHECK_EVALUATION_FETCH_LIMIT) | 4 core ferceqr tables x ~52 quarters, with headroom for re-run partitions. |

## Functions

| [`apply_duckdb_dtypes`](#pudl.transform.ferceqr.apply_duckdb_dtypes)(table_data, table_name, conn)         | Cast each column to the dtype declared in the PUDL metadata schema for the table.      |
|------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| [`rename_duckdb_columns`](#pudl.transform.ferceqr.rename_duckdb_columns)(→ duckdb.DuckDBPyRelation)          | Rename one or more columns in a DuckDB relation, passing all others through unchanged. |
| [`apply_column_transforms`](#pudl.transform.ferceqr.apply_column_transforms)(→ duckdb.DuckDBPyRelation)        | Apply a DuckDB expression factory to a set of columns, replacing each in place.        |
| [`_yn_to_bool`](#pudl.transform.ferceqr._yn_to_bool)(→ duckdb.Expression)                          | Return a DuckDB expression that converts `'Y'`/`'N'` strings to booleans.              |
| [`_na_to_null`](#pudl.transform.ferceqr._na_to_null)(→ duckdb.Expression)                          | Return a DuckDB expression that converts `'N/A'` or `'NA'` strings to NULL.            |
| [`_parse_datetimes`](#pudl.transform.ferceqr._parse_datetimes)(→ duckdb.Expression)                     | Return a DuckDB expression that parses a datetime string column using `fmt`.           |
| [`_recode_categoricals`](#pudl.transform.ferceqr._recode_categoricals)(→ duckdb.Expression)                 | Return a DuckDB expression that replaces exact categorical values in a column.         |
| [`core_ferceqr__quarterly_identity`](#pudl.transform.ferceqr.core_ferceqr__quarterly_identity)(context, ...)            | Transform the raw FERC EQR filer identity table.                                       |
| [`core_ferceqr__transactions`](#pudl.transform.ferceqr.core_ferceqr__transactions)(context, ...)                  | Transform the raw FERC EQR electricity transactions table.                             |
| [`core_ferceqr__contracts`](#pudl.transform.ferceqr.core_ferceqr__contracts)(context, raw_ferceqr_\_contracts) | Transform the raw FERC EQR electricity contracts table.                                |
| [`core_ferceqr__quarterly_index_pub`](#pudl.transform.ferceqr.core_ferceqr__quarterly_index_pub)(context, ...)           | Transform the raw FERC EQR index price publisher table.                                |
| [`_latest_extraction_stats_by_quarter`](#pudl.transform.ferceqr._latest_extraction_stats_by_quarter)(→ dict[str, ...)      | Return `{year_quarter: extraction_stats}` from each partition's latest run.            |
| [`_latest_check_evaluations_by_quarter`](#pudl.transform.ferceqr._latest_check_evaluations_by_quarter)(→ dict[str, ...)     | Return `{year_quarter: {table_label: evaluation}}` from each check's latest run.       |
| [`_build_ferceqr_diagnostics_rows`](#pudl.transform.ferceqr._build_ferceqr_diagnostics_rows)(→ list[dict[str, Any]])   | Flatten per-quarter extraction stats and check results into one wide table.            |
| [`ferceqr_pipeline_diagnostics`](#pudl.transform.ferceqr.ferceqr_pipeline_diagnostics)(→ dagster.MaterializeResult) | Compile a cross-quarter summary of ferceqr extraction and schema-check anomalies.      |

## Module Contents

### pudl.transform.ferceqr.logger

### pudl.transform.ferceqr.\_EXTRACTION_STATS_ASSET_KEY

### pudl.transform.ferceqr.\_EXTRACTION_STATS_FETCH_LIMIT *= 500*

Comfortably more than the number of ferceqr quarters, even with some
re-materialized more than once – `fetch_materializations` returns
most-recent-first, so the first record seen for each partition is already the
one we want.

### pudl.transform.ferceqr.\_CORE_FERCEQR_TABLE_LABELS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

### pudl.transform.ferceqr.\_CHECK_EVALUATION_FETCH_LIMIT *= 2000*

4 core ferceqr tables x ~52 quarters, with headroom for re-run partitions.

### pudl.transform.ferceqr.apply_duckdb_dtypes(table_data: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection))

Cast each column to the dtype declared in the PUDL metadata schema for the table.

Column types are looked up from the [`pudl.metadata.classes.Resource`](../../metadata/classes/index.html.md#pudl.metadata.classes.Resource) for
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

### pudl.transform.ferceqr.core_ferceqr_\_quarterly_identity(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), raw_ferceqr_\_ident: [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR filer identity table.

### pudl.transform.ferceqr.core_ferceqr_\_transactions(context, raw_ferceqr_\_transactions: [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR electricity transactions table.

### pudl.transform.ferceqr.core_ferceqr_\_contracts(context, raw_ferceqr_\_contracts: [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR electricity contracts table.

### pudl.transform.ferceqr.core_ferceqr_\_quarterly_index_pub(context, raw_ferceqr_\_index_pub: [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData))

Transform the raw FERC EQR index price publisher table.

### pudl.transform.ferceqr.\_latest_extraction_stats_by_quarter(instance: [dagster.DagsterInstance](https://docs.dagster.io/api/dagster/internals/#dagster.DagsterInstance)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]

Return `{year_quarter: extraction_stats}` from each partition’s latest run.

Reads the `extraction_stats` JSON metadata that `pudl.extract.ferceqr.
extract_ferceqr()` attaches to `raw_ferceqr__extract_errors`, directly from this
Dagster instance’s event log – no Parquet data is read.

### pudl.transform.ferceqr.\_latest_check_evaluations_by_quarter(instance: [dagster.DagsterInstance](https://docs.dagster.io/api/dagster/internals/#dagster.DagsterInstance)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), dagster.AssetCheckEvaluation]]

Return `{year_quarter: {table_label: evaluation}}` from each check’s latest run.

Reads `pandera_schema_check` asset check evaluations for the four
`core_ferceqr__*` tables directly from this Dagster instance’s event log –
no Parquet data is read.

Unlike asset materializations, check evaluation events don’t carry a
`partition_key` field on the record itself – the partition instead lives
on the `AssetCheckEvaluation` payload, so this reads events generically
(across *all* assets and checks in the instance, hence the high
`_CHECK_EVALUATION_FETCH_LIMIT`) and filters down to the four
`core_ferceqr__*` tables’ `pandera_schema_check` results itself, rather
than being able to ask the instance for exactly those upfront.

### pudl.transform.ferceqr.\_build_ferceqr_diagnostics_rows(extraction_stats_by_quarter: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]], check_evaluations_by_quarter: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), dagster.AssetCheckEvaluation]]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]

Flatten per-quarter extraction stats and check results into one wide table.

Each input dict is keyed by `year_quarter` but has a different, and
possibly incomplete, set of keys underneath (e.g. a quarter can have
extraction stats but no check evaluations yet, or vice versa). This
produces one row per quarter seen in *either* input, with a fixed,
predictable set of columns – computed upfront from the *union* of
per-quarter table/reject-reason keys actually observed – so every row has
the same shape and missing values show up as `0`/`None` rather than
a missing column.

### pudl.transform.ferceqr.ferceqr_pipeline_diagnostics(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext)) → [dagster.MaterializeResult](https://docs.dagster.io/api/dagster/assets/#dagster.MaterializeResult)

Compile a cross-quarter summary of ferceqr extraction and schema-check anomalies.

This asset produces no data of its own – it exists purely to compile
diagnostics that other ferceqr assets already record, into a single table
visible in the Dagster UI without opening each quarter’s materialization one
at a time: the per-quarter `extraction_stats` metadata attached to
`raw_ferceqr__extract_errors` (filing counts, corrupt archives, rejected
records by reason), and the `pandera_schema_check` asset check results for
the four `core_ferceqr__*` tables (row counts, primary-key violations, and
other schema check failures).

Depending on *all* partitions of several partitioned upstream assets via
[`dagster.AllPartitionMapping`](https://docs.dagster.io/api/dagster/partitions/#dagster.AllPartitionMapping) means materializing this asset re-scans
the full history already recorded in this Dagster instance’s event log each
time – cheap, since it only reads metadata, never the underlying Parquet
data.
