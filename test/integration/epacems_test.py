"""Tests for pudl/output/epacems.py loading functions."""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest
import sqlalchemy as sa
from dagster import build_init_resource_context
from pydantic import ValidationError

from pudl.extract.epacems import extract
from pudl.io_managers import epacems_io_manager
from pudl.output.epacems import epacems, year_state_filter
from pudl.settings import EpaCemsSettings, EtlSettings


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
    context = build_init_resource_context()
    return (
        epacems_io_manager(context)._base_path
        / "core_epacems__hourly_emissions.parquet"
    )


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


def test_epacems_missing_partition(pudl_datastore_fixture):
    """Check that trying to extract a non-working partition raises ValidationError"""
    with pytest.raises(ValidationError):
        _ = extract(year_quarter="1994Q4", ds=pudl_datastore_fixture)


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


def test_missing_cems_partition(pudl_datastore_fixture):
    """Test that the extract step returns an empty df.

    Note: This could be a unit test, but it interacts with zenodo which is sometimes
    slow. It must retrieve the datapacakge.json associated with the archive.
    """
    assert extract("2051q4", ds=pudl_datastore_fixture).empty
