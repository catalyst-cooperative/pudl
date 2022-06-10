"""Tests for timeseries anomalies detection and imputation."""
import numpy as np
import pandas as pd
import pytest

from pudl.analysis.state_demand import lookup_state

AK_FIPS = {"name": "Alaska", "code": "AK", "fips": "02"}


@pytest.mark.parametrize(
    "state,expected",
    [
        ("Alaska", AK_FIPS),
        ("alaska", AK_FIPS),
        ("ALASKA", AK_FIPS),
        ("AK", AK_FIPS),
        ("ak", AK_FIPS),
        (2, AK_FIPS),
        ("02", AK_FIPS),
        ("2", AK_FIPS),
        (2.0, AK_FIPS),
        (np.int64(2), AK_FIPS),
        pytest.param(99, {}, marks=pytest.mark.xfail),
        pytest.param("Oaxaca", {}, marks=pytest.mark.xfail),
        pytest.param("MX", {}, marks=pytest.mark.xfail),
        pytest.param(np.nan, {}, marks=pytest.mark.xfail),
        pytest.param(pd.NA, {}, marks=pytest.mark.xfail),
        pytest.param("", {}, marks=pytest.mark.xfail),
        pytest.param(None, {}, marks=pytest.mark.xfail),
    ],
)
def test_lookup_state(state: str | int, expected: dict[str, str | int]) -> None:
    """Check that various kinds of state lookups work."""
    assert lookup_state(state) == expected
