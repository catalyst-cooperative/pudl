"""Unit tests for the functions in the pudl.validate module."""

import numpy as np
import pandas as pd
import pytest

import pudl.validate as pv


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
        ([1, 2, 3], [1, 2, 1], 0.5, 2),
        ([10, 20, 30], [1, 3, 1], 0.5, 20),
        # Different quantiles with same data
        ([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], 0.25, 1.75),
        ([1, 2, 3, 4, 5], [1, 1, 1, 1, 1], 0.75, 4.25),
        # Single data point
        ([5], [1], 0.5, 5),
        ([5], [1], 0.0, 5),
        ([5], [1], 1.0, 5),
        # Two data points - corrected expected values
        ([1, 3], [1, 1], 0.5, 2),
        ([1, 3], [1, 3], 0.5, 2.5),
        ([1, 3], [3, 1], 0.5, 1.5),
        # Repeated values (tests the groupby logic)
        ([1, 1, 2, 2], [1, 1, 1, 1], 0.5, 1.5),
        ([1, 1, 3, 3], [1, 1, 1, 1], 0.5, 2),
        # Zero weights mixed with non-zero
        ([1, 2, 3, 4, 5], [0, 1, 0, 0, 0], 0.5, 2),
        ([1, 2, 3, 4, 5], [0, 0, 0, 0, 1], 0.5, 5),
    ],
)
def test_weighted_quantile_parametrized(data, weights, quantile, expected):
    """Test the weighted quantile function with various inputs."""
    data_series = pd.Series(data)
    weights_series = pd.Series(weights)
    result = pv.weighted_quantile(data_series, weights_series, quantile)
    assert result == expected


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
def test_weighted_quantile_with_invalid_data(data, weights, quantile, expected):
    """Test that weighted_quantile handles invalid data by filtering it out."""
    data_series = pd.Series(data)
    weights_series = pd.Series(weights)
    result = pv.weighted_quantile(data_series, weights_series, quantile)
    assert result == expected
