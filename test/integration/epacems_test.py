"""tests for pudl/output/epacems.py loading functions."""
from pathlib import Path

import dask.dataframe as dd
import pytest

from pudl.output.epacems import epacems


@pytest.fixture(scope='module')
def epacems_year_and_state(etl_params):
    """Find the year and state defined in pudl/package_data/settings/etl_*.yml."""
    # the etl_params data structure alternates dicts and lists so indexing is a pain.
    epacems = [item for item in etl_params['datapkg_bundle_settings']
               [0]['datasets'] if 'epacems' in item.keys()]
    epacems = epacems[0]['epacems']
    return {'years': epacems['epacems_years'], 'states': epacems['epacems_states']}


@pytest.fixture(scope='session')
def epacems_parquet_path(
    pudl_settings_fixture,
    pudl_engine,  # implicit dependency; ensures .parquet files exist
):
    """Get path to the directory of EPA CEMS .parquet data."""
    out_dir = Path(pudl_settings_fixture['parquet_dir'], 'epacems')
    return out_dir


def test_epacems_subset(epacems_year_and_state, epacems_parquet_path):
    """Minimal integration test of epacems(). Check if it returns a DataFrame."""
    path = epacems_parquet_path
    years = epacems_year_and_state['years']
    # Use only Idaho if multiple states are given
    states = epacems_year_and_state['states'] if len(
        epacems_year_and_state['states']) == 1 else ['ID']
    actual = epacems(columns=["gross_load_mw"],
                     epacems_path=path,
                     years=years,
                     states=states)
    assert isinstance(actual, dd.DataFrame)
    assert actual.shape[0].compute() > 0  # n rows
