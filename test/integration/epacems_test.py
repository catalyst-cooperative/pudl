"""tests for pudl/output/epacems.py loading functions."""
from pathlib import Path

import dask.dataframe as dd
import pytest

from pudl.etl import etl_epacems
from pudl.output.epacems import epacems, year_state_filter
from pudl.settings import EpaCemsSettings


@pytest.fixture(scope="module")
def epacems_year_and_state(etl_settings):
    """Find the year and state defined in pudl/package_data/settings/etl_*.yml."""
    # the etl_settings data structure alternates dicts and lists so indexing is a pain.
    return etl_settings.datasets.epacems


@pytest.fixture(scope="session")
def epacems_parquet_path(
    pudl_settings_fixture,
    pudl_engine,  # implicit dependency; ensures .parquet files exist
):
    """Get path to the directory of EPA CEMS .parquet data."""
    out_dir = Path(pudl_settings_fixture["parquet_dir"], "epacems")
    return out_dir


def test_epacems_subset(epacems_year_and_state, epacems_parquet_path):
    """Minimal integration test of epacems(). Check if it returns a DataFrame."""
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
        dict(years=[valid_year], states=[valid_state], columns=[invalid_column]),
        dict(years=[valid_year], states=[invalid_state], columns=[valid_column]),
        dict(years=[invalid_year], states=[valid_state], columns=[valid_column]),
    ]
    for combo in combos:
        with pytest.raises(ValueError):
            epacems(epacems_path=path, **combo)


def test_epacems_parallel(pudl_settings_fixture, pudl_ds_kwargs, tmpdir_factory):
    """Test that we can run the EPA CEMS ETL in parallel."""
    epacems_settings = EpaCemsSettings(
        years=[2019, 2020, 2021], states=["ID", "ME"], partition=True
    )
    # We need a temporary output directory to avoid dropping the ID/ME 2019/2020
    # parallel outputs in the real output directory and interfering with the normal
    # monolithic outputs.
    parquet_tmp = tmpdir_factory.mktemp("parquet_tmp")
    epacems_tmp = Path(parquet_tmp, "epacems")
    epacems_tmp.mkdir()
    settings_tmp = {k: v for k, v in pudl_settings_fixture.items()}
    settings_tmp["parquet_dir"] = parquet_tmp
    etl_epacems(epacems_settings, settings_tmp, pudl_ds_kwargs)
    df = dd.read_parquet(
        epacems_tmp,
        filters=year_state_filter(years=[2019], states=["ME"]),
        index=False,
        engine="pyarrow",
        split_row_groups=True,
    ).compute()
    assert df.shape == (96_360, 19)  # nosec: B101
