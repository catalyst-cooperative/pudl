"""Unit tests for pudl.validate.quality."""

import numpy as np
import pandas as pd
import pytest

from pudl.validate import quality as pv


@pytest.mark.parametrize(
    "data,weights,quantile,expected",
    [
        # Basic test case - single non-zero weight
        ([1, 2, 3, 4, 5], [0, 0, 0, 1, 0], 0.5, 4),
        # Uniform weights - should behave like regular quantile
        ([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], 0.5, 3),
        ([1, 2, 3, 4], [1, 1, 1, 1], 0.5, 2.5),
        # Edge cases for quantiles
        ([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], 0.0, 1),
        ([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], 1.0, 5),
        # Non-uniform weights
        ([1, 2, 3], [1, 1, 3], 0.5, 2.5),
        ([10, 20, 40], [1, 1, 3], 0.5, 30),
        # Single data point
        ([5], [1], 0.5, 5),
        ([5], [1], 0.0, 5),
        ([5], [1], 1.0, 5),
        # Two data points
        ([1, 3], [1, 1], 0.5, 2),
        ([1, 3], [1, 3], 0.5, 2.5),
        ([1, 3], [3, 1], 0.5, 1.5),
        # Repeated values (tests the groupby logic)
        ([1, 1, 2, 2], [1, 1, 1, 1], 0.5, 1.5),
        ([1, 1, 3, 3], [1, 1, 1, 1], 0.5, 2),
    ],
)
def test_weighted_quantile(data, weights, quantile, expected):
    """Test the weighted quantile function with various inputs."""
    data_series = pd.Series(data)
    weights_series = pd.Series(weights)
    result = pv.weighted_quantile(data_series, weights_series, quantile)
    assert np.isclose(result, expected)


@pytest.mark.parametrize(
    "data,weights,quantile,expected_error",
    [
        # Invalid quantile values
        ([1, 2, 3], [1, 1, 1], -0.1, ValueError),
        ([1, 2, 3], [1, 1, 1], 1.1, ValueError),
        # Mismatched lengths
        ([1, 2, 3], [1, 1], 0.5, ValueError),
        ([1, 2], [1, 1, 1], 0.5, ValueError),
    ],
)
def test_weighted_quantile_errors(data, weights, quantile, expected_error):
    """Test that weighted_quantile raises appropriate errors for invalid inputs."""
    data_series = pd.Series(data)
    weights_series = pd.Series(weights)
    with pytest.raises(expected_error):
        pv.weighted_quantile(data_series, weights_series, quantile)


@pytest.mark.parametrize(
    "data,weights,quantile",
    [
        # All NaN data
        ([np.nan, np.nan, np.nan], [1, 1, 1], 0.5),
        # All zero weights
        ([1, 2, 3], [0, 0, 0], 0.5),
        # Empty series
        ([], [], 0.5),
    ],
)
def test_weighted_quantile_nan_cases(data, weights, quantile):
    """Test that weighted_quantile returns NaN for degenerate cases."""
    data_series = pd.Series(data)
    weights_series = pd.Series(weights)
    result = pv.weighted_quantile(data_series, weights_series, quantile)
    assert np.isnan(result)


@pytest.mark.parametrize(
    "data,weights,quantile,expected",
    [
        # Data with inf values - function filters out inf but processes remaining data
        ([1, np.inf, 3], [1, 1, 1], 0.5, 2.0),
        ([1, -np.inf, 3], [1, 1, 1], 0.5, 2.0),
        # NaN weights - function filters out rows with NaN weights
        ([1, 2, 3], [1, np.nan, 1], 0.5, 2.0),
        # Mix of NaN and inf - function filters out problematic rows
        ([1, np.nan, np.inf, 4], [1, 1, 1, 1], 0.5, 2.5),
    ],
)
def test_weighted_quantile_filtering(data, weights, quantile, expected):
    """Test that weighted_quantile filters out problematic rows of data and weights."""
    data_series = pd.Series(data)
    weights_series = pd.Series(weights)
    result = pv.weighted_quantile(data_series, weights_series, quantile)
    assert result == expected


@pytest.mark.parametrize(
    "rows,cols,max_null_fraction",
    [
        # All values present — never null
        ({"a": [1, 2], "b": [3, 4]}, "all", 0.9),
        # One null in a two-column row: fraction is 0.5, below threshold of 0.6
        ({"a": [None, 2], "b": [3, 4]}, "all", 0.6),
        # Subset cols — nulls in unchecked column are ignored
        ({"a": [None, 2], "b": [3, 4]}, ["b"], 0.9),
        # Threshold of exactly 1.0 — any fraction passes
        ({"a": [None, None], "b": [None, None]}, "all", 1.0),
    ],
)
def test_no_null_rows_passes(rows, cols, max_null_fraction):
    """no_null_rows returns the input DataFrame unchanged when no row is too null."""
    df = pd.DataFrame(rows)
    result = pv.no_null_rows(df, cols=cols, max_null_fraction=max_null_fraction)
    pd.testing.assert_frame_equal(result, df)


@pytest.mark.parametrize(
    "rows,cols,max_null_fraction,expected_null_count",
    [
        # Every column null in first row: fraction 1.0 > 0.9 threshold
        ({"a": [None, 1], "b": [None, 2]}, "all", 0.9, 1),
        # Both rows are fully null
        ({"a": [None, None], "b": [None, None]}, "all", 0.9, 2),
        # Only the checked column is null; unchecked col has values
        ({"a": [None, 1], "b": [3, 4]}, ["a"], 0.9, 1),
        # Three-column row with two nulls: fraction 0.67 > 0.6 threshold
        ({"a": [None, 1], "b": [None, 2], "c": [3, 4]}, "all", 0.6, 1),
    ],
)
def test_no_null_rows_raises(rows, cols, max_null_fraction, expected_null_count):
    """no_null_rows raises ExcessiveNullRowsError when rows exceed the null threshold."""
    df = pd.DataFrame(rows)
    with pytest.raises(pv.ExcessiveNullRowsError) as exc_info:
        pv.no_null_rows(df, cols=cols, max_null_fraction=max_null_fraction)
    assert len(exc_info.value.null_rows) == expected_null_count
