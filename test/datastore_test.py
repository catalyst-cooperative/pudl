"""
Exercise the functionality in the datastore management module.

The local datastore is managed by a script that uses the datastore management
module to pull raw data from public sources, and organize it prior to ETL.
However, that process is time consuming because the data is large and far away.
Because of that, we don't pull down new data frequently, which means if the
datastore management infrastructure breaks, we often don't find out about it
for a long time. These tests are meant to help avoid that problem by
continuosly exercising this functionality.
"""

import logging
import os.path

import pytest

import pudl
import pudl.constants as pc
import pudl.datastore.datastore as datastore

logger = logging.getLogger(__name__)


@pytest.mark.pre_etl
def test_datastore(tmp_path, pudl_settings_fixture):
    """Attempt to download the most recent year of FERC Form 1 data."""
    sources = ['eia860', 'eia923', 'epacems', 'ferc1', 'epaipm']
    years_by_source = {
        'eia860': [min(pc.working_years['eia860']), 2017],
        'eia923': [min(pc.working_years['eia923']), 2017],
        'epacems': [min(pc.working_years['epacems']), 2017],
        'ferc1': [min(pc.working_years['ferc1']), 2017],
        'epaipm': [None, ],
    }
    states = ['id']  # Idaho has the least data of any CEMS state.

    datastore.parallel_update(
        sources=sources,
        years_by_source=years_by_source,
        states=states,
        pudl_settings=pudl_settings_fixture,
    )

    pudl.helpers.verify_input_files(
        ferc1_years=years_by_source['ferc1'],
        eia923_years=years_by_source['eia923'],
        eia860_years=years_by_source['eia860'],
        epacems_years=years_by_source['epacems'],
        epacems_states=states,
        data_dir=pudl_settings_fixture['data_dir'],
    )
