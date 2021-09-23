"""tests for pudl/output/epacems.py loading functions."""
from pathlib import Path

import dask.dataframe as dd
import pytest

from pudl.convert.epacems_to_parquet import epacems_to_parquet
from pudl.etl import get_flattened_etl_parameters
from pudl.output.epacems import epacems


@pytest.fixture(scope='module')
def epacems_year_and_state(etl_params):
    """Find the year and state defined in pudl/package_data/settings/etl_fast.yml."""
    # the etl_params data structure alternates dicts and lists so indexing is a pain.
    eia_epa = [item['datasets']
               for item in etl_params['datapkg_bundle_settings'] if 'epacems' in item['name']]
    epacems = [item for item in eia_epa[0] if 'epacems' in item.keys()]
    epacems = epacems[0]['epacems']
    return {'years': epacems['epacems_years'], 'states': epacems['epacems_states']}


@pytest.fixture(scope='session')
def epacems_parquet_path(
    datapkg_bundle,
    pudl_settings_fixture,
    pudl_etl_params,
    request,
    live_dbs,
):
    """Get CEMS path and convert a small amount of EPA CEMS data to parquet format."""
    epacems_datapkg_json = Path(
        pudl_settings_fixture['datapkg_dir'],
        pudl_etl_params['datapkg_bundle_name'],
        'epacems-eia',
        "datapackage.json"
    )
    flat = get_flattened_etl_parameters(
        pudl_etl_params["datapkg_bundle_settings"]
    )
    out_dir = Path(pudl_settings_fixture['parquet_dir'], 'epacems')
    if not live_dbs:
        # The test .parquet files are created by etl_test.py.
        # But because pytest runs the tests in alphabetical order, this module runs before etl_test.py
        # So I have to create the .parquet files here in order to run tests on them.
        # Surely there is a better way; this doesn't belong here.
        epacems_to_parquet(
            datapkg_path=epacems_datapkg_json,
            epacems_years=flat["epacems_years"],
            epacems_states=flat["epacems_states"],
            out_dir=out_dir,
            compression='snappy',
            clobber=False,
        )
    return out_dir


def test_epacems_subset(epacems_year_and_state, epacems_parquet_path):
    """Minimal integration test of epacems(). Check if it returns a DataFrame."""
    path = epacems_parquet_path
    # initially I checked len() exactly, but that had to be hardcoded for a specific year/state.
    # This is less strict, but because etl_fast only tests a single year/state, I think just as effective.
    actual = epacems(columns=["gross_load_mw"],
                     epacems_path=path, **epacems_year_and_state)
    assert isinstance(actual, dd.DataFrame)
    assert len(actual.compute()) > 0
