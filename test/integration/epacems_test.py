"""tests for pudl/output/epacems.py loading functions."""
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pudl.output.epacems import epacems


@pytest.fixture(scope='module')
def cems_year_and_state(etl_params):
    """Find the year and state defined in pudl/package_data/settings/etl_fast.yml."""
    # the etl_params data structure alternates dicts and lists so indexing is a pain.
    eia_epa = [item['datasets']
               for item in etl_params['datapkg_bundle_settings'] if 'epacems' in item['name']]
    cems = [item for item in eia_epa[0] if 'epacems' in item.keys()]
    cems = cems[0]['epacems']
    return {'years': cems['epacems_years'], 'states': cems['epacems_states']}


def test_epacems_empty_frame(cems_year_and_state):
    """Test that empty partitions return empty dataframes instead of an exception."""
    [-1]
    empty_state = ['AK']  # no data for any year, as of 2019 data
    year = cems_year_and_state['years']
    cols = ["plant_id_eia", "unitid", "operating_datetime_utc",
            "gross_load_mw", "unit_id_epa"]
    actual = epacems(states=empty_state, years=year, columns=cols)
    expected = pd.DataFrame(columns=cols)
    assert_frame_equal(actual, expected)


def test_epacems_subset(cems_year_and_state):
    """Minimal integration test of epacems(). Check if it returns a DataFrame."""
    # initially I checked len() exactly, but that had to be hardcoded for a specific year/state.
    # This is less strict, but because etl_fast only tests a single year/state, I think just as effective.
    actual = epacems(columns=["gross_load_mw"], **cems_year_and_state)
    assert isinstance(actual, pd.DataFrame)
    assert len(actual) > 0
