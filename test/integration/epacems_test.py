"""Tests for pudl/output/epacems.py loading functions."""
import dask.dataframe as dd
import pytest
from dagster import build_init_resource_context

from pudl.extract.epacems import extract
from pudl.io_managers import epacems_io_manager
from pudl.metadata.classes import Resource
from pudl.output.epacems import epacems, year_state_filter


@pytest.fixture(scope="module")
def epacems_year_and_state(etl_settings):
    """Find the year and state defined in pudl/package_data/settings/etl_*.yml."""
    # the etl_settings data structure alternates dicts and lists so indexing is a pain.
    return etl_settings.datasets.epacems


@pytest.fixture(scope="session")
def epacems_parquet_path(
    pudl_engine,  # implicit dependency; ensures .parquet files exist
):
    """Get path to the directory of EPA CEMS .parquet data."""
    context = build_init_resource_context()
    return epacems_io_manager(context)._base_path / "hourly_emissions_epacems.parquet"


def test_epacems_subset(epacems_year_and_state, epacems_parquet_path):
    """Minimal integration test of epacems().

    Check if it returns a DataFrame.
    """
    if not epacems_year_and_state:
        pytest.skip("EPA CEMS not in settings file and so is not being tested.")
    path = epacems_parquet_path
    years = epacems_year_and_state.years
    # Use only Idaho if multiple states are given
    states = (
        epacems_year_and_state.states
        if len(epacems_year_and_state.states) == 1
        else ["ID"]
    )
    actual = epacems(
        columns=["gross_load_mw"], epacems_path=path, years=years, states=states
    )
    assert isinstance(actual, dd.DataFrame)  # nosec: B101
    assert actual.shape[0].compute() > 0  # nosec: B101  n rows


def test_epacems_missing_partition(pudl_datastore_fixture):
    """Check that missing partitions return an empty data frame.

    Note that this should pass for both the Fast and Full ETL because the behavior
    towards a missing file is identical."""
    df = extract(year=1996, state="UT", ds=pudl_datastore_fixture)
    epacems_res = Resource.from_id("hourly_emissions_epacems")
    expected_cols = list(epacems_res.get_field_names())
    assert df.shape[0] == 0  # Check that no rows of data are there
    # Check that all columns expected of EPACEMS data are present.
    assert sorted(df.columns) == sorted(expected_cols)


def test_epacems_subset_input_validation(epacems_year_and_state, epacems_parquet_path):
    """Check if invalid inputs raise exceptions."""
    if not epacems_year_and_state:
        pytest.skip("EPA CEMS not in settings file and so is not being tested.")
    path = epacems_parquet_path
    valid_year = epacems_year_and_state.years[-1]
    valid_state = epacems_year_and_state.states[-1]
    valid_column = "gross_load_mw"

    invalid_state = "confederacy"
    invalid_year = 1775
    invalid_column = "clean_coal"
    combos = [
        {"years": [valid_year], "states": [valid_state], "columns": [invalid_column]},
        {"years": [valid_year], "states": [invalid_state], "columns": [valid_column]},
        {"years": [invalid_year], "states": [valid_state], "columns": [valid_column]},
    ]
    for combo in combos:
        with pytest.raises(ValueError):
            epacems(epacems_path=path, **combo)


def test_epacems_parallel(pudl_engine, epacems_parquet_path):
    """Test that we can run the EPA CEMS ETL in parallel."""
    # We need a temporary output directory to avoid dropping the ID/ME 2019/2020
    # parallel outputs in the real output directory and interfering with the normal
    # monolithic outputs.
    df = dd.read_parquet(
        epacems_parquet_path,
        filters=year_state_filter(years=[2020], states=["ME"]),
        index=False,
        engine="pyarrow",
        split_row_groups=True,
    ).compute()
    assert df.shape == (96_624, 16)  # nosec: B101
