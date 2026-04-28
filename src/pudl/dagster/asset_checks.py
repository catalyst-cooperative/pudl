"""Programmatically defined Dagster asset checks for PUDL.

This module should contain Dagster asset-check definitions and helper functions that
evaluate the quality or structural correctness of already-materialized assets. Put
checks here when they belong in the Dagster asset graph and should run as blocking or
reporting validations attached to specific assets, especially when they can be derived
from metadata or shared validation patterns. Keep business transformations and dbt-only
data tests out of this module so it remains focused on Dagster-native asset validation.

For the underlying Dagster concept see https://docs.dagster.io/guides/test/asset-checks

For data validation we almost entirely rely on :mod:`dbt` data tests defined using SQL
and executed across our Parquet outputs using :mod:`duckdb`.

We primarily use Dagster asset checks to validate the schemas of PUDL tables throughout
the pipeline. We use :mod:`pandera` to programmatically define dataframe schemas based
on the PUDL metadata with the asset check factory :func:`asset_check_from_schema`
defined below. A handful of asset checks that were particularly difficult to translate
to SQL/dbt data tests are also defined here, but in general all data validation tests
should go in dbt.
"""

import itertools
from typing import Any

import dagster as dg
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pandera.pandas as pr_pandas
import pandera.polars as pr_polars
import polars as pl
from pandera.errors import SchemaErrors

from pudl.dagster.assets import all_asset_modules, asset_keys
from pudl.dagster.partitions import ferceqr_year_quarters
from pudl.helpers import ParquetData, get_parquet_table_polars
from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import Package, Resource


def _collect_asset_metadata(asset_value) -> dict[str, Any]:
    """Collect basic metadata about the asset."""
    if isinstance(asset_value, pl.LazyFrame):
        shape = (
            asset_value.select(pl.len()).collect(engine="streaming").item(),
            asset_value.collect_schema().len(),
        )
    else:
        shape = asset_value.shape
    return {
        "asset_type": str(type(asset_value)),
        "asset_shape": list(shape),
    }


def _extract_actual_columns_and_dtypes(
    asset_value: pl.LazyFrame | pd.DataFrame,
) -> tuple[list[str], dict[str, str], bool]:
    """Extract actual column names and dtypes from supported dataframe objects."""
    use_pandas_backend = False

    if isinstance(asset_value, pl.LazyFrame):
        schema = asset_value.collect_schema()
        actual_columns = schema.names()
        actual_dtypes = {
            col: str(dtype)
            for col, dtype in zip(actual_columns, schema.dtypes(), strict=True)
        }
        return actual_columns, actual_dtypes, use_pandas_backend

    if isinstance(asset_value, pd.DataFrame):
        use_pandas_backend = True
        actual_columns = list(asset_value.columns)
        actual_dtypes = {
            str(col): str(dtype) for col, dtype in asset_value.dtypes.items()
        }
        return actual_columns, actual_dtypes, use_pandas_backend

    raise ValueError(
        f"Unsupported asset type for dtype collection: {type(asset_value)}"
    )


def _collect_dtype_metadata(
    asset_value: pl.LazyFrame | pd.DataFrame,
    resource: Resource,
) -> dict[str, Any]:
    """Build metadata comparing actual dataframe dtypes to metadata-driven expectations.

    Args:
        asset_value: Asset output to introspect. Supported types are
            :class:`pandas.DataFrame` and :class:`polars.LazyFrame`.
        resource: PUDL metadata resource whose schema fields define expected columns and
            dtypes.

    Returns:
        A metadata dictionary with:
        - ``field_details``: per-column expected and actual dtype details.
        - ``column_comparison``: expected/actual column counts and optional missing
          or extra column lists.
        - ``type_mismatches``: only present when common columns have differing dtype
          strings.

    Raises:
        ValueError: If ``asset_value`` is not a supported dataframe type.

    Notes:
        Expected dtypes are captured as strings from ``field.to_pandera_column()``.
        Any errors while computing expected dtypes are recorded inline as
        ``"Error: ..."`` values rather than raised.
    """
    dtype_errors: dict[str, str] = {}
    actual_columns, actual_dtypes, use_pandas_backend = (
        _extract_actual_columns_and_dtypes(asset_value)
    )

    expected_columns = [field.name for field in resource.schema.fields]

    pandera_dtypes = {}
    for field in resource.schema.fields:
        try:
            pandera_dtypes[field.name] = str(
                field.to_pandera_column(use_pandas_backend=use_pandas_backend).dtype
            )
        except Exception as exc:
            error_text = str(exc)
            pandera_dtypes[field.name] = f"Error: {error_text}"
            dtype_errors[field.name] = error_text

    field_details = {
        field.name: {
            "pudl_field_dtype": field.type,
            "expected_pandera_dtype": pandera_dtypes.get(field.name, "Unknown"),
            "actual_dtype": actual_dtypes.get(field.name, "Column not present"),
        }
        for field in resource.schema.fields
    }

    missing_columns = sorted(set(expected_columns) - set(actual_columns))
    extra_columns = sorted(set(actual_columns) - set(expected_columns))
    column_comparison: dict[str, Any] = {
        "expected_count": len(expected_columns),
        "actual_count": len(actual_columns),
    }
    if missing_columns:
        column_comparison["missing_columns"] = missing_columns
    if extra_columns:
        column_comparison["extra_columns"] = extra_columns

    common_columns = sorted(set(expected_columns) & set(actual_columns))
    type_mismatches = {}
    for column in common_columns:
        expected_type = pandera_dtypes.get(column, "Unknown")
        actual_type = actual_dtypes.get(column, "Unknown")
        if expected_type != actual_type and expected_type != "Unknown":
            type_mismatches[column] = {
                "expected": expected_type,
                "actual": actual_type,
            }

    metadata = {
        "field_details": field_details,
        "column_comparison": column_comparison,
    }
    if dtype_errors:
        metadata["expected_dtype_errors"] = dtype_errors
    if type_mismatches:
        metadata["type_mismatches"] = type_mismatches

    return metadata


def _collect_geometry_metadata(asset_value) -> dict[str, Any]:
    """Collect GeoPandas-specific metadata."""
    if not isinstance(asset_value, gpd.GeoDataFrame):
        return {}

    metadata = {
        "geometry_column": (
            asset_value.geometry.name
            if hasattr(asset_value, "geometry")
            else "No geometry attribute"
        )
    }

    if hasattr(asset_value, "geometry") and hasattr(asset_value.geometry, "dtype"):
        metadata["geometry_dtype"] = str(asset_value.geometry.dtype)

    return metadata


def _process_schema_errors(schema_errors: SchemaErrors) -> dict[str, Any]:
    """Process Pandera schema errors into structured metadata."""
    detailed_errors = []

    for err in schema_errors.schema_errors:
        error_info = {
            "error_type": type(err).__name__,
            "error_message": str(err),
            "failure_cases": str(err.failure_cases)
            if hasattr(err, "failure_cases")
            else "No failure_cases",
            "data": str(err.data) if hasattr(err, "data") else "No data",
        }

        # Add optional error attributes
        for attr in ["schema", "check", "args"]:
            if hasattr(err, attr):
                error_info[f"{attr}_info"] = str(getattr(err, attr))

        detailed_errors.append(error_info)

    return {
        "detailed_errors": detailed_errors,
        "num_errors": len(schema_errors.schema_errors),
    }


def group_mean_continuity_check(
    df: pd.DataFrame,
    thresholds: dict[str, float],
    groupby_col: str,
    n_outliers_allowed: int = 0,
) -> dg.AssetCheckResult:
    """Check that certain variables don't vary too much on average between groups.

    Groups and sorts the data by ``groupby_col``, then takes the mean across
    each group. Useful for saying something like "the average water usage of
    cooling systems didn't jump by 10x from 2012-2013."

    Args:
        df: the df with the actual data
        thresholds: a mapping from column names to the ratio by which those
            columns are allowed to fluctuate from one group to the next.
        groupby_col: the column by which we will group the data.
        n_outliers_allowed: how many data points are allowed to be above the
            threshold.
    """
    pct_change = (
        df.loc[:, [groupby_col] + list(thresholds.keys())]
        .groupby(groupby_col, sort=True)
        .mean()
        .pct_change()
        .abs()
        .dropna()
    )
    discontinuity = pct_change >= thresholds
    metadata = {
        col: {
            "top5": list(pct_change[col][discontinuity[col]].nlargest(n=5)),
            "threshold": thresholds[col],
        }
        for col in thresholds
        if discontinuity[col].sum() > 0
    }
    if (discontinuity.sum() > n_outliers_allowed).any():
        return dg.AssetCheckResult(passed=False, metadata=metadata)

    return dg.AssetCheckResult(passed=True, metadata=metadata)


def asset_check_from_schema(  # noqa: C901
    asset_key: dg.AssetKey,
    package: Package,
    duckdb_asset: bool,
    high_memory_asset: bool,
) -> dg.AssetChecksDefinition | None:
    """Create a Dagster asset check based on the resource schema, if defined.

    The vast majority of assets will be loaded as Polars LazyFrames directly using
    the ``PudlParquetIOManager`` and validated with Pandera's Polars backend, but
    there are two exceptions to this. The first exception are assets which contain
    a geometry data type. These assets will all be loaded as geopandas GeoDataFrames
    and use Pandera's Pandas backend as Polars does not support geometry data types.
    The second exception are assets produced entirely using DuckDB. These assets
    return ``ParquetData`` objects, which are handled by the default io-manager. In
    this case, the resulting parquet file(s) will be scanned with Polars to produce
    a LazyFrame, then handled exactly the same as a typical asset.
    """
    resource_id = asset_key.to_user_string()
    try:
        resource = package.get_resource(resource_id)
    except ValueError:
        return None

    pandera_schema = resource.schema.to_pandera()
    partitions = ferceqr_year_quarters if "ferceqr" in resource_id else None
    if duckdb_asset:
        asset_type = ParquetData
    elif isinstance(pandera_schema, pr_polars.DataFrameSchema):
        asset_type = pl.LazyFrame
    elif isinstance(pandera_schema, pr_pandas.DataFrameSchema):
        asset_type = gpd.GeoDataFrame
    else:
        raise ValueError(
            "Unexpected return type from `Resource.schema.to_pandera()`."
            f"Expected a pandera `DataFrameSchema`, but got: `{type(pandera_schema)}`"
        )

    @dg.asset_check(asset=asset_key, blocking=True, partitions_def=partitions)
    # Dagster uses this runtime annotation to select the correct IO manager load type,
    # but static type checkers may reject the computed local variable in a type expression.
    def pandera_schema_check(
        asset_value: asset_type,  # type: ignore[valid-type]
    ) -> dg.AssetCheckResult:
        if isinstance(asset_value, ParquetData):
            asset_value = get_parquet_table_polars(
                table_name=resource_id,
                partitions=asset_value.partitions,
            )

        # Collect all metadata
        metadata = (
            _collect_asset_metadata(asset_value)
            | _collect_dtype_metadata(asset_value, resource)
            | _collect_geometry_metadata(asset_value)
        )

        try:
            if isinstance(asset_value, pl.LazyFrame):
                validated_schema = asset_value.pipe(pandera_schema.validate, lazy=True)
                # Only validate data contents if asset is not marked as high memory
                if not high_memory_asset:
                    validated_schema.collect(engine="streaming")
            else:
                pandera_schema.validate(asset_value, lazy=True)
            return dg.AssetCheckResult(passed=True, metadata=metadata)

        except SchemaErrors as schema_errors:
            metadata.update(_process_schema_errors(schema_errors))
            return dg.AssetCheckResult(passed=False, metadata=metadata)

        except Exception as exc:
            metadata["unexpected_error"] = {
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "error_args": str(exc.args) if hasattr(exc, "args") else "No args",
            }
            return dg.AssetCheckResult(passed=False, metadata=metadata)

    return pandera_schema_check


default_asset_checks = list(
    itertools.chain.from_iterable(
        dg.load_asset_checks_from_modules(modules)
        for modules in all_asset_modules.values()
    )
)

duckdb_assets = [
    "core_ferceqr__quarterly_identity",
    "core_ferceqr__contracts",
    "core_ferceqr__quarterly_index_pub",
    "core_ferceqr__transactions",
]
high_memory_assets = [
    "out_vcerare__hourly_available_capacity_factor",
    "core_epacems__hourly_emissions",
]

default_asset_checks += [
    check
    for check in (
        asset_check_from_schema(
            asset_key,
            PUDL_PACKAGE,
            duckdb_asset=asset_key.to_user_string() in duckdb_assets,
            high_memory_asset=asset_key.to_user_string() in high_memory_assets,
        )
        for asset_key in asset_keys
    )
    if check is not None
]

__all__ = [
    "asset_check_from_schema",
    "group_mean_continuity_check",
    "default_asset_checks",
    "duckdb_assets",
    "high_memory_assets",
]
