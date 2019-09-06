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
import os

import pudl
import pudl.workspace.datastore as datastore

logger = logging.getLogger(__name__)


def test_datastore(pudl_settings_fixture, data_scope):
    """Download sample data for each available data source."""
    sources_to_update = ['eia860', 'eia923', 'epaipm']
    years_by_source = {
        'eia860': data_scope['eia860_years'],
        'eia923': data_scope['eia923_years'],
        'epacems': [],
        'epaipm': [None, ],
        'ferc1': [],
    }
    # Sadly, FERC & EPA only provide access to their data via FTP, and it's
    # not possible to use FTP from within the Travis CI environment:
    if os.getenv('TRAVIS'):
        states = []
    else:
        # Idaho has the least data of any CEMS state.
        states = data_scope['epacems_states']
        sources_to_update.extend(['ferc1', 'epacems'])
        years_by_source['ferc1'] = data_scope['ferc1_years']
        years_by_source['epacems'] = data_scope['epacems_years']

    datastore.parallel_update(
        sources=sources_to_update,
        years_by_source=years_by_source,
        states=states,
        data_dir=pudl_settings_fixture['data_dir'],
    )

    pudl.helpers.verify_input_files(
        ferc1_years=years_by_source['ferc1'],
        eia923_years=years_by_source['eia923'],
        eia860_years=years_by_source['eia860'],
        epacems_years=years_by_source['epacems'],
        epacems_states=states,
        # Currently no mechanism for automatically verifying EPA IPM files...
        pudl_settings=pudl_settings_fixture,
    )
