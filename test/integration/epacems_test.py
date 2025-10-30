"""Tests for pudl/output/epacems.py loading functions."""

from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.output.epacems import epacems as epacems_output
from pudl.settings import EpaCemsSettings, EtlSettings
from pudl.workspace.setup import PudlPaths


@pytest.fixture(scope="module")
def epacems_settings(etl_settings: EtlSettings) -> EpaCemsSettings:
    """Find the year and quarter defined in pudl/package_data/settings/etl_*.yml."""
    # the etl_settings data structure alternates dicts and lists so indexing is a pain.
    return etl_settings.datasets.epacems


@pytest.fixture(scope="session")
def epacems_parquet_path(pudl_engine: sa.Engine) -> Path:
    """Get path to the directory of EPA CEMS .parquet data.

    Args:
        pudl_engine: An implicit dependency that ensures the .parquet files exist.
    """
    return PudlPaths().parquet_path("core_epacems__hourly_emissions")


def test_epacems_output(epacems_settings: EpaCemsSettings, epacems_parquet_path: Path):
    """Check that epacems output routine retrieves a non-empty dataframe."""
    if not epacems_settings:
        pytest.skip("EPA CEMS not in settings file and so is not being tested.")
    path = epacems_parquet_path
    years = [yq.year for yq in pd.to_datetime(epacems_settings.year_quarters)]
    states = ["ID"]  # Test on Idaho, one of the smallest states
    actual = epacems_output(
        columns=["year", "state", "gross_load_mw"],
        epacems_path=path,
        years=years,
        states=states,
    )
    assert isinstance(actual, pd.DataFrame)
    assert "gross_load_mw" in actual.columns
    assert "state" in actual.columns
    assert "year" in actual.columns
    assert actual.state.unique().tolist() == states
    assert set(actual.year.unique().tolist()).issubset(set(years))
    assert len(actual) > 0


# TODO: Add tests for expected behavior with bad partitions: 1994q4, 2051q4
