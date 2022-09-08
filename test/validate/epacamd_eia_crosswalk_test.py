"""Validate post-ETL EPACAMD-EIA Crosswalk data."""
import logging

import pytest

from pudl.validate import check_unique_rows

logger = logging.getLogger(__name__)


def test_unique_ids(pudl_out_eia, live_dbs):
    """Test whether the EIA plants and EPA unit pairings are unique."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test should only run on un-aggregated data.")
    # Should I add these args to the pudl.validate module?
    check_unique_rows(
        pudl_out_eia.epacamd_eia,
        ["plant_id_eia", "emissions_unit_id_epa"],
        "epacamd_eia",
    )
