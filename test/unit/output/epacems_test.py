"""Test helper functions associated with the EPA CEMS outputs."""
import logging

import pytest

from pudl.output.epacems import year_state_filter

logger = logging.getLogger(__name__)


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
