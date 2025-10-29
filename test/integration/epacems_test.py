"""Tests for pudl/output/epacems.py loading functions."""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.output.epacems import epacems, year_state_filter
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


def test_epacems_subset(epacems_settings: EpaCemsSettings, epacems_parquet_path: Path):
    """Check that epacems output retrieves a non-empty dataframe."""
    if not epacems_settings:
        pytest.skip("EPA CEMS not in settings file and so is not being tested.")
    path = epacems_parquet_path
    years = [yq.year for yq in pd.to_datetime(epacems_settings.year_quarters)]
    states = ["ID"]  # Test on Idaho, one of the smallest states
    actual = epacems(
        columns=["year", "state", "gross_load_mw"],
        epacems_path=path,
        years=years,
        states=states,
    )
    assert isinstance(actual, dd.DataFrame)
    assert "gross_load_mw" in actual.columns
    assert "state" in actual.columns
    assert "year" in actual.columns
    assert actual.shape[0].compute() > 0


def test_epacems_parallel(pudl_engine, epacems_parquet_path):
    """Test that we can run the EPA CEMS ETL in parallel."""
    # We need a temporary output directory to avoid dropping the ID/ME 2019/2020
    # parallel outputs in the real output directory and interfering with the normal
    # monolithic outputs.
    df = dd.read_parquet(
        epacems_parquet_path,
        filters=year_state_filter(years=[2023], states=["ME"]),
        index=False,
        engine="pyarrow",
        split_row_groups=True,
    ).compute()
    assert df.year.unique() == [2023]
    assert df.state.unique() == ["ME"]


# TODO: Add tests for expected behavior with bad partitions: 1994q4, 2051q4
