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

logger = logging.getLogger(__name__)


@pytest.mark.pre_etl
@pytest.mark.parametrize(
    "source,year", [
        ("eia860", min(pc.data_years["eia860"])),
        ("eia860", max(pc.data_years["eia860"])),
        ("eia923", min(pc.data_years["eia923"])),
        ("eia923", max(pc.data_years["eia923"])),
        ("epacems", min(pc.data_years["epacems"])),
        ("epacems", max(pc.data_years["epacems"])),
        ("epaipm", None),
        ("ferc1", min(pc.data_years["ferc1"])),
        ("ferc1", max(pc.data_years["ferc1"]))
    ]
)
def test_datastore(tmpdir, source, year):
    """Attempt to download the most recent year of FERC Form 1 data."""
    states = ["id"]  # Idaho has the least data of any CEMS state.
    # Attempt to update our temporary/test datastore:
    pudl.datastore.datastore.update(source, year, states, datadir=tmpdir)
    # Generate a list of paths where we think there should be files now:
    paths_to_check = pudl.datastore.datastore.paths_for_year(
        source=source, year=year, states=states, datadir=tmpdir)
    # Loop over those paths and poke at the files a bit:
    for path in paths_to_check:
        # Check that the path exists at all:
        if not os.path.exists(path):
            raise AssertionError(f"Missing expected file at {path}")
        filesize_kb = round(os.path.getsize(path) / 1024)
        logger.info(
            f"Found {filesize_kb} kB file at {path}")
        # Verify that recent files are big enough to be real:
        if (source != 'epacems') and (filesize_kb < 10000) and (year > 2010):
            raise AssertionError(
                f"Suspiciously small {source} data for {year} found at {path}"
            )
