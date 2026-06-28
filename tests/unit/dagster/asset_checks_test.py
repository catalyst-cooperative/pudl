"""Regression tests for Dagster asset-check input typing.

:func:`~pudl.dagster.asset_checks.asset_check_from_schema` is a factory that generates
one Dagster asset check per PUDL table. Each generated check function has a single
parameter, ``asset_value``, whose type annotation Dagster inspects **at runtime** to
decide which IO manager to use when loading the asset. The annotation must be the exact
type object appropriate for that specific asset:

* ``pl.LazyFrame`` for normal Parquet-backed assets
* ``gpd.GeoDataFrame`` for assets containing geometry columns
* ``ParquetData`` for DuckDB-produced assets

The factory stores this in a local variable (``asset_type``) and uses it directly as
the annotation, which requires a ``# type: ignore[valid-type]`` comment to silence the
static type checker.

The tempting "cleanup" is to replace that computed annotation with a tidy static union::

    def pandera_schema_check(
        asset_value: pl.LazyFrame | gpd.GeoDataFrame | ParquetData,  # looks fine!
    ) -> dg.AssetCheckResult:

This satisfies the type checker and removes the ``type: ignore``, but it silently breaks
every generated check: Dagster sees the union and can no longer determine the correct IO
manager, so it will attempt to load every asset through the wrong path.

The tests below guard against both classes of "cleanup" by asserting that the annotation
on each generated check is the *exact* expected type object (using ``is``, not ``==``).
"""

import io

import dagster as dg
import pandas as pd
import pint
import polars as pl
import pytest
from dagster._core.definitions.asset_checks.asset_checks_definition import (
    AssetChecksDefinition,
)

from pudl.dagster.asset_checks import (
    _build_registry_from_descriptor,
    _validate_datapackage_unit_strings,
    asset_check_from_schema,
    group_mean_continuity_check,
)
from pudl.helpers import ParquetData
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.metadata.units import PUDL_UNIT_DEFINITIONS


@pytest.mark.parametrize(
    ("asset_name", "duckdb_asset", "expected_type"),
    [
        ("core_pudl__codes_subdivisions", False, pl.LazyFrame),
        ("core_ferceqr__contracts", True, ParquetData),
    ],
)
def test_asset_checks_preserve_runtime_input_types(
    asset_name: str, duckdb_asset: bool, expected_type: type
) -> None:
    """Generated checks should advertise the IO-manager input type they expect."""
    check: AssetChecksDefinition | None = asset_check_from_schema(
        dg.AssetKey([asset_name]),
        PUDL_PACKAGE,
        duckdb_asset=duckdb_asset,
        high_memory_asset=False,
    )

    assert check is not None
    assert (
        check.node_def.compute_fn.decorated_fn.__annotations__["asset_value"]
        is expected_type
    )


@pytest.mark.parametrize(
    "column,threshold,n_outliers_allowed,expected_pass",
    [
        # Test cases that should PASS
        ("stable_metric", 0.1, 0, True),
        ("gradual_growth", 0.2, 0, True),
        ("sudden_jump", 5.0, 0, True),
        ("volatile_metric", 0.2, 1, True),
        ("negative_change", 0.2, 0, True),
        # Test cases that should FAIL
        ("sudden_jump", 0.1, 0, False),
        ("volatile_metric", 0.1, 0, False),
        ("sudden_jump", 1.0, 0, False),
        ("gradual_growth", 0.05, 0, False),
    ],
)
def test_group_mean_continuity_check(
    column, threshold, n_outliers_allowed, expected_pass
):
    """Test the group_mean_continuity_check function with various scenarios.

    Uses a test dataframe with different column patterns:
    - stable_metric: Values around 100 with minimal variation
    - gradual_growth: Fixed growth of 10 per year
    - sudden_jump: Large 5x jump from 2022 to 2023
    - volatile_metric: Random fluctuations around 100, one of which is larger
    - negative_change: Fixed decline of 100 per year
    """
    # Test data for group_mean_continuity_check function
    # This dataframe contains various patterns for testing different scenarios
    mean_continuity_df = pd.read_csv(
        io.StringIO(
            """year,stable_metric,gradual_growth,sudden_jump,volatile_metric,negative_change
    2020,100,100,100,100,1000
    2021,101,110,102,95,900
    2022,99,120,105,130,800
    2023,102,130,500,105,700
    2024,98,140,510,90,600
    """
        )
    )

    result = group_mean_continuity_check(
        df=mean_continuity_df,
        thresholds={column: threshold},
        groupby_col="year",
        n_outliers_allowed=n_outliers_allowed,
    )

    assert result.passed == expected_pass

    # Verify metadata structure
    assert hasattr(result, "metadata")
    assert isinstance(result.metadata, dict)

    # If test failed, metadata should contain information about the failing column
    if not expected_pass:
        assert column in result.metadata
        # The metadata values are wrapped in JsonMetadataValue objects
        # Access the underlying data using the .data attribute
        column_metadata = result.metadata[column].data
        assert isinstance(column_metadata, dict)
        assert "threshold" in column_metadata
        assert column_metadata["threshold"] == threshold
        assert "top5" in column_metadata


# ---------------------------------------------------------------------------
# Tests for descriptor-embedded unit registry helpers
# ---------------------------------------------------------------------------

_MINIMAL_DESCRIPTOR = {
    "unit_registry": {"format": "pint", "definitions": PUDL_UNIT_DEFINITIONS},
    "resources": [
        {
            "name": "test_resource",
            "schema": {
                "fields": [
                    {"name": "energy_mwh", "unit": "MWh"},
                    {"name": "heat_mmbtu", "unit": "MMBtu"},
                    {"name": "cost_usd_per_mwh", "unit": "USD / MWh"},
                    {"name": "no_unit_field"},
                ]
            },
        }
    ],
}


def test_build_registry_from_descriptor_parses_custom_units() -> None:
    """Registry built from descriptor definitions must parse PUDL custom units."""
    ureg = _build_registry_from_descriptor(_MINIMAL_DESCRIPTOR)
    assert isinstance(ureg, pint.UnitRegistry)
    ureg.parse_units("MMBtu")
    ureg.parse_units("VAr")
    ureg.parse_units("USD / MWh")


def test_build_registry_from_descriptor_missing_key() -> None:
    """KeyError is raised when the descriptor has no unit_registry field."""
    with pytest.raises(KeyError):
        _build_registry_from_descriptor({})


def test_validate_datapackage_unit_strings_all_valid() -> None:
    """No errors are returned for a descriptor with only valid unit strings."""
    errors = _validate_datapackage_unit_strings(_MINIMAL_DESCRIPTOR)
    assert errors == []


def test_validate_datapackage_unit_strings_bad_unit() -> None:
    """An unparseable unit string produces exactly one error entry."""
    descriptor = {
        "unit_registry": {"format": "pint", "definitions": PUDL_UNIT_DEFINITIONS},
        "resources": [
            {
                "name": "bad_resource",
                "schema": {"fields": [{"name": "col", "unit": "not_a_unit"}]},
            }
        ],
    }
    errors = _validate_datapackage_unit_strings(descriptor)
    assert len(errors) == 1
    assert "bad_resource.col" in errors[0]
    assert "not_a_unit" in errors[0]


def test_validate_datapackage_unit_strings_missing_registry() -> None:
    """A descriptor without unit_registry produces an error rather than crashing."""
    errors = _validate_datapackage_unit_strings({"resources": []})
    assert len(errors) == 1
    assert "Could not build unit registry" in errors[0]
