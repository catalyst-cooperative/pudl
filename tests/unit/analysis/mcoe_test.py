"""Unit tests for mcoe_asset_check_factory in pudl.analysis.mcoe.

The factory wraps :func:`~pudl.validate.quality.no_null_rows` inside a Dagster
``@asset_check`` and converts the :exc:`~pudl.validate.quality.ExcessiveNullRowsError`
exception into an :class:`~dagster.AssetCheckResult`. These tests exercise that
error-handling conversion directly by extracting the decorated function from the
generated :class:`~dagster.AssetChecksDefinition` and calling it with a plain
:class:`~pandas.DataFrame`.
"""

import pandas as pd
import pytest

from pudl.analysis.mcoe import McoeCheckSpec, mcoe_asset_check_factory


def _call_check(spec: McoeCheckSpec, df: pd.DataFrame):
    """Extract and directly call the underlying check function.

    ``mcoe_asset_check_factory`` returns an ``AssetChecksDefinition``. We reach
    through Dagster's wrapping layers to retrieve the original Python function so
    we can call it with a plain DataFrame in a unit-test context, without needing
    a running Dagster instance.
    """
    check = mcoe_asset_check_factory(spec)
    return check.node_def.compute_fn.decorated_fn(df)


@pytest.mark.parametrize(
    "rows,max_null_fraction",
    [
        # No nulls at all
        ({"a": [1, 2], "b": [3, 4]}, 0.9),
        # One null in a two-column row: fraction 0.5, below threshold of 0.6
        ({"a": [None, 2], "b": [3, 4]}, 0.6),
    ],
)
def test_mcoe_check_passes_with_clean_data(rows, max_null_fraction):
    """Check returns passed=True and count=0 when null fraction is within threshold."""
    spec = McoeCheckSpec(asset="test_asset", max_null_fraction=max_null_fraction)
    result = _call_check(spec, pd.DataFrame(rows))
    assert result.passed is True
    assert result.metadata["excessively_null_row_count"].value == 0


@pytest.mark.parametrize(
    "rows,max_null_fraction,expected_count",
    [
        # Both columns null in first row: fraction 1.0 > 0.9 threshold
        ({"a": [None, 1], "b": [None, 2]}, 0.9, 1),
        # Both rows fully null
        ({"a": [None, None], "b": [None, None]}, 0.9, 2),
        # Three-column row with two nulls: fraction 0.67 > 0.6 threshold
        ({"a": [None, 1], "b": [None, 2], "c": [3, 4]}, 0.6, 1),
    ],
)
def test_mcoe_check_fails_with_null_rows(rows, max_null_fraction, expected_count):
    """Check returns passed=False with the correct null row count in metadata."""
    spec = McoeCheckSpec(asset="test_asset", max_null_fraction=max_null_fraction)
    result = _call_check(spec, pd.DataFrame(rows))
    assert result.passed is False
    assert result.metadata["excessively_null_row_count"].value == expected_count
