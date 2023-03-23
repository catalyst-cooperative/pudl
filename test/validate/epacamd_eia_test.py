"""Validate post-ETL EPACAMD-EIA Crosswalk data."""
import logging

import pytest

from pudl.validate import check_max_rows, check_min_rows

logger = logging.getLogger(__name__)


def test_minmax_rows(pudl_out_orig, live_dbs):
    """Test whether the EIA plants and EPA unit pairings are unique."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")

    pudl_out_orig.epacamd_eia().pipe(
        check_min_rows, expected_rows=6432, margin=0.0, df_name="epacamd_eia"
    ).pipe(check_max_rows, expected_rows=6432, margin=0.0, df_name="epacamd_eia")
