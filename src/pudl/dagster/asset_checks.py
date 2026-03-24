"""Programmatically defined Dagster asset checks for PUDL.

We primarily use Dagster asset checks to validate the schemas of PUDL tables. We use
Pandera to programmatically define dataframe schemas based on the PUDL metadata with the
asset check factory :func:`asset_check_from_schema` defined below.

For data validation we almost entirely rely on dbt data tests.
"""

import itertools
from typing import Any

import dagster as dg
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import pandera.pandas as pr_pandas
import pandera.polars as pr_polars
import polars as pl
from dagster import AssetCheckResult, AssetChecksDefinition, AssetKey, asset_check
from pandera.errors import SchemaErrors

from pudl.dagster.assets import all_asset_modules, asset_keys
from pudl.helpers import ParquetData, get_parquet_table_polars
from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import Package, Resource
from pudl.settings import ferceqr_year_quarters


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
    """Build metadata comparing actual dataframe dtypes to metadata-driven expectations."""
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


def asset_check_from_schema(  # noqa: C901
    asset_key: AssetKey,
    package: Package,
    duckdb_asset: bool,
    high_memory_asset: bool,
) -> AssetChecksDefinition | None:
    """Create a Dagster asset check based on the resource schema, if defined.

    The majority of assets are validated as Polars ``LazyFrame`` objects loaded from
    parquet by the IO manager. Geometry-backed assets are validated as GeoPandas data
    frames, and DuckDB-backed assets use ``ParquetData`` placeholders which are read
    back as Polars before validation.
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

    @asset_check(asset=asset_key, blocking=True, partitions_def=partitions)
    def pandera_schema_check(asset_value: asset_type) -> AssetCheckResult:
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
            return AssetCheckResult(passed=True, metadata=metadata)

        except SchemaErrors as schema_errors:
            metadata.update(_process_schema_errors(schema_errors))
            return AssetCheckResult(passed=False, metadata=metadata)

        except Exception as exc:
            metadata["unexpected_error"] = {
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "error_args": str(exc.args) if hasattr(exc, "args") else "No args",
            }
            return AssetCheckResult(passed=False, metadata=metadata)

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
    "default_asset_checks",
    "duckdb_assets",
    "high_memory_assets",
]
