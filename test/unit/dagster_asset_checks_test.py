"""Regression tests for Dagster asset-check input typing."""

import dagster as dg
import polars as pl
import pytest

from pudl.dagster.asset_checks import asset_check_from_schema
from pudl.helpers import ParquetData
from pudl.metadata import PUDL_PACKAGE


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
    check = asset_check_from_schema(
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
