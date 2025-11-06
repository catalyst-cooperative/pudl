"""Programmatically defined Dagster asset checks for PUDL.

We primarily use Dagster asset checks to validate the schemas of PUDL tables. We use
Pandera to programmatically define dataframe schemas based on the PUDL metadata with the
asset check factory :func:`asset_check_from_schema` defined below.

For data validation we almost entirely rely on dbt data tests.
"""

from typing import Any

import geopandas as gpd
import pandas as pd
import pandera as pr
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    asset_check,
)

from pudl.metadata.classes import Package, Resource


def _collect_asset_metadata(asset_value) -> dict[str, Any]:
    """Collect basic metadata about the asset."""
    return {
        "asset_type": str(type(asset_value)),
        "asset_shape": list(getattr(asset_value, "shape", "No shape attribute")),
    }


def _collect_dtype_metadata(asset_value, resource: Resource) -> dict[str, Any]:
    """Collect comprehensive column and data type information for comparison."""
    metadata = {}

    # Get actual columns and types
    actual_columns = (
        list(asset_value.columns) if hasattr(asset_value, "columns") else []
    )
    actual_dtypes = {}
    if hasattr(asset_value, "dtypes"):
        actual_dtypes = {col: str(dtype) for col, dtype in asset_value.dtypes.items()}

    # Get expected columns and types
    expected_columns = [field.name for field in resource.schema.fields]
    pandera_dtypes = {}

    for field in resource.schema.fields:
        try:
            pandera_dtypes[field.name] = str(field.to_pandera_column().dtype)
        except Exception as e:
            pandera_dtypes[field.name] = f"Error: {str(e)}"

    # Detailed field information
    field_details = {}
    for field in resource.schema.fields:
        field_details[field.name] = {
            "pudl_field_dtype": field.type,
            "expected_pandera_dtype": pandera_dtypes.get(field.name, "Unknown"),
            "actual_dtype": actual_dtypes.get(field.name, "Column not present"),
        }

    metadata["field_details"] = field_details

    # Column comparison summary
    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)

    metadata["column_comparison"] = {
        "expected_count": len(expected_columns),
        "actual_count": len(actual_columns),
    }

    if missing_columns:
        metadata["column_comparison"]["missing_columns"] = list(missing_columns)
    if extra_columns:
        metadata["column_comparison"]["extra_columns"] = list(extra_columns)

    # Type mismatches for common columns
    common_columns = set(expected_columns) & set(actual_columns)
    type_mismatches = {}

    for col in common_columns:
        expected_type = pandera_dtypes.get(col, "Unknown")
        actual_type = actual_dtypes.get(col, "Unknown")
        if expected_type != actual_type and expected_type != "Unknown":
            type_mismatches[col] = {"expected": expected_type, "actual": actual_type}

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


def _process_schema_errors(schema_errors: pr.errors.SchemaErrors) -> dict[str, Any]:
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


def asset_check_from_schema(
    asset_key: AssetKey,
    package: Package,
) -> AssetChecksDefinition | None:
    """Create a dagster asset check based on the resource schema, if defined."""
    resource_id = asset_key.to_user_string()
    try:
        resource = package.get_resource(resource_id)
    except ValueError:
        return None

    pandera_schema = resource.schema.to_pandera()

    @asset_check(asset=asset_key, blocking=True)
    def pandera_schema_check(asset_value: pd.DataFrame) -> AssetCheckResult:
        # Collect all metadata
        metadata = (
            _collect_asset_metadata(asset_value)
            | _collect_dtype_metadata(asset_value, resource)
            | _collect_geometry_metadata(asset_value)
        )

        try:
            pandera_schema.validate(asset_value, lazy=True)
            return AssetCheckResult(passed=True, metadata=metadata)

        except pr.errors.SchemaErrors as schema_errors:
            metadata.update(_process_schema_errors(schema_errors))
            return AssetCheckResult(passed=False, metadata=metadata)

        except Exception as e:
            metadata["unexpected_error"] = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "error_args": str(e.args) if hasattr(e, "args") else "No args",
            }
            return AssetCheckResult(passed=False, metadata=metadata)

    return pandera_schema_check
