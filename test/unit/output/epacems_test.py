"""Test helper functions associated with the EPA CEMS outputs."""

import io
import logging
from unittest.mock import patch

import pandas as pd
import pytest

from pudl.output.epacems import get_plant_states, get_plant_years, year_state_filter

logger = logging.getLogger(__name__)


# Test data containing plants across multiple states and years
TEST_PLANT_DATA = pd.read_csv(
    io.StringIO(
        """plant_id_eia,state,report_date
1001,CA,2019-01-01
1001,CA,2020-01-01
1002,TX,2020-01-01
1002,TX,2021-06-01
1003,NY,2022-01-01
1004,FL,2020-07-01
1004,FL,2021-01-01
1005,CA,2020-12-01"""
    ),
    parse_dates=["report_date"],
)


@pytest.mark.parametrize(
    "years,states,expected_filter",
    [
        ([2020], ["ID"], [[("year", "=", 2020), ("state", "=", "ID")]]),
        (None, ["ID"], [[("state", "=", "ID")]]),
        ([2020], None, [[("year", "=", 2020)]]),
        (None, None, None),
    ],
)
def test_year_state_filter(years, states, expected_filter):
    """Test the generation of DNF pushdown filters for Parquet files."""
    assert (  # nosec: B101
        year_state_filter(years=years, states=states) == expected_filter
    )


@pytest.mark.parametrize(
    "plant_ids,expected_states",
    [
        # Multiple plants across different states
        ([1001, 1002, 1003], {"CA", "TX", "NY"}),
        # Single plant in one state
        ([1001], {"CA"}),
        # Plant not in dataset (empty result)
        ([9999], set()),
        # Plants with duplicate states should be deduplicated
        ([1001, 1004, 1005], {"CA", "FL"}),
        # All plants
        ([1001, 1002, 1003, 1004, 1005], {"CA", "TX", "NY", "FL"}),
    ],
)
@patch("pudl.output.epacems.get_parquet_table")
def test_get_plant_states(mock_get_parquet_table, plant_ids, expected_states):
    """Test get_plant_states function with various plant ID inputs."""
    # Filter test data to only include requested plant IDs
    filtered_data = TEST_PLANT_DATA[TEST_PLANT_DATA["plant_id_eia"].isin(plant_ids)]
    mock_get_parquet_table.return_value = filtered_data

    # Test the function
    result = get_plant_states(plant_ids)

    # Verify the call to get_parquet_table
    mock_get_parquet_table.assert_called_once_with(
        "out_eia__yearly_plants",
        columns=["plant_id_eia", "state"],
        filters=[("plant_id_eia", "in", plant_ids)],
    )

    # Verify the result
    assert set(result) == expected_states


@pytest.mark.parametrize(
    "plant_ids,expected_years",
    [
        # Multiple plants across different years
        ([1001, 1002, 1003], {2019, 2020, 2021, 2022}),
        # Single plant across multiple years
        ([1001], {2019, 2020}),
        # Plant not in dataset (empty result)
        ([9999], set()),
        # Plants with overlapping years should be deduplicated
        ([1002, 1004], {2020, 2021}),
        # All plants
        ([1001, 1002, 1003, 1004, 1005], {2019, 2020, 2021, 2022}),
    ],
)
@patch("pudl.output.epacems.get_parquet_table")
def test_get_plant_years(mock_get_parquet_table, plant_ids, expected_years):
    """Test get_plant_years function with various plant ID inputs."""
    # Filter test data to only include requested plant IDs
    filtered_data = TEST_PLANT_DATA[TEST_PLANT_DATA["plant_id_eia"].isin(plant_ids)]
    mock_get_parquet_table.return_value = filtered_data

    # Test the function
    result = get_plant_years(plant_ids)

    # Verify the call to get_parquet_table
    mock_get_parquet_table.assert_called_once_with(
        "out_eia__yearly_plants",
        columns=["plant_id_eia", "report_date"],
        filters=[("plant_id_eia", "in", plant_ids)],
    )

    # Verify the result
    assert set(result) == expected_years
