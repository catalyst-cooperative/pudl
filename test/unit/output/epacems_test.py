"""tests for pudl/output/epacems.py loading functions."""
import pandas as pd
from pandas.testing import assert_frame_equal

from pudl.output.epacems import epacems


def test_epacems_empty_frame():
    """Test that empty partitions return empty dataframes instead of an exception."""
    empty_state = ['AK']
    year = [2019]
    cols = ["plant_id_eia", "unitid", "operating_datetime_utc",
            "gross_load_mw", "unit_id_epa"]
    actual = epacems(states=empty_state, years=year, columns=cols)
    expected = pd.DataFrame(columns=cols)
    assert_frame_equal(actual, expected)


def test_epacems_subset():
    """Minimal integration test of epacems()."""
    # smallest single state-year, just check len()
    state = ['DC']
    year = [1997]
    actual = len(epacems(states=state, years=year, columns=["gross_load_mw"]))
    expected = 17520
    assert actual == expected, f"Length mismatch for state {state} in {year}. Expected {expected}, got {actual}"
