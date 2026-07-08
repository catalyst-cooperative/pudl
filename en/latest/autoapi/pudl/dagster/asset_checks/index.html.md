# pudl.dagster.asset_checks

Programmatically defined Dagster asset checks for PUDL.

This module should contain Dagster asset-check definitions and helper functions that
evaluate the quality or structural correctness of already-materialized assets. Put
checks here when they belong in the Dagster asset graph and should run as blocking or
reporting validations attached to specific assets, especially when they can be derived
from metadata or shared validation patterns. Keep business transformations and dbt-only
data tests out of this module so it remains focused on Dagster-native asset validation.

For the underlying Dagster concept see [https://docs.dagster.io/guides/test/asset-checks](https://docs.dagster.io/guides/test/asset-checks)

For data validation we almost entirely rely on `dbt` data tests defined using SQL
and executed across our Parquet outputs using `duckdb`.

We primarily use Dagster asset checks to validate the schemas of PUDL tables throughout
the pipeline. We use `pandera` to programmatically define dataframe schemas based
on the PUDL metadata with the asset check factory [`asset_check_from_schema()`](#pudl.dagster.asset_checks.asset_check_from_schema)
defined below. A handful of asset checks that were particularly difficult to translate
to SQL/dbt data tests are also defined here, but in general all data validation tests
should go in dbt.

## Attributes

| [`default_asset_checks`](#pudl.dagster.asset_checks.default_asset_checks)   |    |
|-----------------------------------------------------------------------------|----|
| [`duckdb_assets`](#pudl.dagster.asset_checks.duckdb_assets)                 |    |
| [`high_memory_assets`](#pudl.dagster.asset_checks.high_memory_assets)       |    |

## Functions

| [`group_mean_continuity_check`](#pudl.dagster.asset_checks.group_mean_continuity_check)(→ dagster.AssetCheckResult)   | Check that certain variables don't vary too much on average between groups.           |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`asset_check_from_schema`](#pudl.dagster.asset_checks.asset_check_from_schema)(...)                                  | Create a Dagster asset check based on the resource schema, if defined.                |
| [`valid_datapackage_check`](#pudl.dagster.asset_checks.valid_datapackage_check)(→ dagster.AssetChecksDefinition)      | Return a Dagster asset check that validates a frictionless datapackage descriptor.    |
| [`valid_datapackage_unit_strings_check`](#pudl.dagster.asset_checks.valid_datapackage_unit_strings_check)(...)        | Return a Dagster asset check that validates unit strings in a datapackage descriptor. |

## Module Contents

### pudl.dagster.asset_checks.group_mean_continuity_check(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), thresholds: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [float](https://docs.python.org/3/library/functions.html#float)], groupby_col: [str](https://docs.python.org/3/library/stdtypes.html#str), n_outliers_allowed: [int](https://docs.python.org/3/library/functions.html#int) = 0) → [dagster.AssetCheckResult](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetCheckResult)

Check that certain variables don’t vary too much on average between groups.

Groups and sorts the data by `groupby_col`, then takes the mean across
each group. Useful for saying something like “the average water usage of
cooling systems didn’t jump by 10x from 2012-2013.”

* **Parameters:**
  * **df** – the df with the actual data
  * **thresholds** – a mapping from column names to the ratio by which those
    columns are allowed to fluctuate from one group to the next.
  * **groupby_col** – the column by which we will group the data.
  * **n_outliers_allowed** – how many data points are allowed to be above the
    threshold.

### pudl.dagster.asset_checks.asset_check_from_schema(asset_key: [dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey), package: [pudl.metadata.classes.Package](../../metadata/classes/index.md#pudl.metadata.classes.Package), duckdb_asset: [bool](https://docs.python.org/3/library/functions.html#bool), high_memory_asset: [bool](https://docs.python.org/3/library/functions.html#bool)) → [dagster.AssetChecksDefinition](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetChecksDefinition) | [None](https://docs.python.org/3/library/constants.html#None)

Create a Dagster asset check based on the resource schema, if defined.

The vast majority of assets will be loaded as Polars LazyFrames directly using
the `PudlParquetIOManager` and validated with Pandera’s Polars backend, but
there are two exceptions to this. The first exception are assets which contain
a geometry data type. These assets will all be loaded as geopandas GeoDataFrames
and use Pandera’s Pandas backend as Polars does not support geometry data types.
The second exception are assets produced entirely using DuckDB. These assets
return `ParquetData` objects, which are handled by the default io-manager. In
this case, the resulting parquet file(s) will be scanned with Polars to produce
a LazyFrame, then handled exactly the same as a typical asset.

### pudl.dagster.asset_checks.valid_datapackage_check(asset_key: [dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey) | [str](https://docs.python.org/3/library/stdtypes.html#str), , description: [str](https://docs.python.org/3/library/stdtypes.html#str), blocking: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [dagster.AssetChecksDefinition](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetChecksDefinition)

Return a Dagster asset check that validates a frictionless datapackage descriptor.

The check reads `$PUDL_OUTPUT/parquet/datapackage.json` from the injected
`pudl_paths` Dagster resource at run time and validates it recursively through
resources, schemas, and fields against the frictionless spec using
`frictionless.Package.metadata_validate()`.

* **Parameters:**
  * **asset_key** – Key of the asset that produces the datapackage descriptor.
  * **description** – Human-readable description attached to the check in the
    Dagster UI.
  * **blocking** – Whether the check is blocking (default `True`).

### pudl.dagster.asset_checks.default_asset_checks

### pudl.dagster.asset_checks.duckdb_assets *= ['core_ferceqr_\_quarterly_identity', 'core_ferceqr_\_contracts',...*

### pudl.dagster.asset_checks.high_memory_assets *= ['out_vcerare_\_hourly_available_capacity_factor', 'core_epacems_\_hourly_emissions',...*

### pudl.dagster.asset_checks.valid_datapackage_unit_strings_check(asset_key: [dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey) | [str](https://docs.python.org/3/library/stdtypes.html#str), , description: [str](https://docs.python.org/3/library/stdtypes.html#str), blocking: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [dagster.AssetChecksDefinition](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetChecksDefinition)

Return a Dagster asset check that validates unit strings in a datapackage descriptor.

Reads the descriptor from `$PUDL_OUTPUT/parquet/datapackage.json`, builds a
Pint unit registry from the `unit_registry` field embedded in the descriptor,
and attempts to parse every `unit` field value with that registry.  All
failures are collected before the check reports so a single run surfaces every
bad unit string.

* **Parameters:**
  * **asset_key** – Key of the asset that produces the datapackage descriptor.
  * **description** – Human-readable description attached to the check in the
    Dagster UI.
  * **blocking** – Whether the check is blocking (default `True`).
