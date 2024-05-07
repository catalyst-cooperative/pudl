"""Validate post-ETL state demand analysis output."""

import logging

import pyarrow.parquet as pq
import pytest

from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "resource_id,expected_rows",
    [("out_ferc714__hourly_estimated_state_demand", 6_706_318)],
)
def test_minmax_rows(live_dbs: bool, resource_id: str, expected_rows: int):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        live_dbs: wether we're using a live or testing DB)
        expected_rows: Expected number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        resource_id: ID of the resource (table) whose rows we're going to count.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    parquet_path = PudlPaths().parquet_path(resource_id)
    meta = pq.read_metadata(parquet_path)
    assert meta.num_rows == expected_rows
