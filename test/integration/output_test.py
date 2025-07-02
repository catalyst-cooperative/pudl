"""PyTest cases related to generating derived outputs."""

import logging

import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.helpers import get_parquet_table

logger = logging.getLogger(__name__)


def nuke_gen_fraction(df: pd.DataFrame) -> float:
    """Calculate the nuclear fraction of net generation."""
    total_gen = df.net_generation_mwh.sum()
    nuke_gen = df[df.fuel_type_code_pudl == "nuclear"].net_generation_mwh.sum()
    return nuke_gen / total_gen


@pytest.mark.parametrize(
    "table_name,expected_nuke_fraction,tolerance",
    [
        ("out_eia923__monthly_generation_fuel_combined", 0.2, 0.02),
        ("out_eia__monthly_generators", 0.2, 0.02),
    ],
)
def test_nuclear_fraction(
    table_name: str,
    expected_nuke_fraction: float,
    tolerance: float,
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
):
    """Ensure that overall nuclear generation fractions are as expected."""
    df = get_parquet_table(
        table_name, columns=["fuel_type_code_pudl", "net_generation_mwh"]
    )
    actual_nuke_fraction = nuke_gen_fraction(df)
    assert abs(actual_nuke_fraction - expected_nuke_fraction) <= tolerance
