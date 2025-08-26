"""Unit tests for the functions in the pudl.validate module."""

import io

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
    "column,threshold,n_outliers_allowed,expected_pass",
    [
        # Test cases that should PASS
        ("stable_metric", 0.1, 0, True),
        ("gradual_growth", 0.2, 0, True),
        ("sudden_jump", 5.0, 0, True),
        ("volatile_metric", 0.2, 1, True),
        ("negative_change", 0.2, 0, True),
        # Test cases that should FAIL
        ("sudden_jump", 0.1, 0, False),
        ("volatile_metric", 0.1, 0, False),
        ("sudden_jump", 1.0, 0, False),
        ("gradual_growth", 0.05, 0, False),
    ],
)
def test_group_mean_continuity_check(
    column, threshold, n_outliers_allowed, expected_pass
):
    """Test the group_mean_continuity_check function with various scenarios.

    Uses a test dataframe with different column patterns:
    - stable_metric: Values around 100 with minimal variation
    - gradual_growth: Fixed growth of 10 per year
    - sudden_jump: Large 5x jump from 2022 to 2023
    - volatile_metric: Random fluctuations around 100, one of which is larger
    - negative_change: Fixed decline of 100 per year
    """
    # Test data for group_mean_continuity_check function
    # This dataframe contains various patterns for testing different scenarios
    mean_continuity_df = pd.read_csv(
        io.StringIO(
            """year,stable_metric,gradual_growth,sudden_jump,volatile_metric,negative_change
    2020,100,100,100,100,1000
    2021,101,110,102,95,900
    2022,99,120,105,130,800
    2023,102,130,500,105,700
    2024,98,140,510,90,600
    """
        )
    )

    result = pv.group_mean_continuity_check(
        df=mean_continuity_df,
        thresholds={column: threshold},
        groupby_col="year",
        n_outliers_allowed=n_outliers_allowed,
    )

    assert result.passed == expected_pass

    # Verify metadata structure
    assert hasattr(result, "metadata")
    assert isinstance(result.metadata, dict)

    # If test failed, metadata should contain information about the failing column
    if not expected_pass:
        assert column in result.metadata
        # The metadata values are wrapped in JsonMetadataValue objects
        # Access the underlying data using the .data attribute
        column_metadata = result.metadata[column].data
        assert isinstance(column_metadata, dict)
        assert "threshold" in column_metadata
        assert column_metadata["threshold"] == threshold
        assert "top5" in column_metadata
