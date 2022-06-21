"""Unit tests for functions in the :mod:`pudl.analysis.fuel_price` module."""
import numpy as np
import pandas as pd
import pytest

from pudl.analysis.fuel_price import weighted_median


@pytest.mark.parametrize(
    "data,weights,answer,dropna",
    [
        (
            [7, 1, 2, 4, 10],
            [1, 1 / 3, 1 / 3, 1 / 3, 1],
            7,
            True,
        ),
        (
            [7, 1, 2, 4, 10],
            [1, 1, 1, 1, 1],
            4,
            True,
        ),
        (
            [7, 1, 2, 4, 10, 15],
            [1, 1 / 3, 1 / 3, 1 / 3, 1, 1],
            8.5,
            True,
        ),
        (
            [1, 2, 4, 7, 10, 15],
            [1 / 3, 1 / 3, 1 / 3, 1, 1, 1],
            8.5,
            True,
        ),
        (
            [0, 10, 20, 30],
            [30, 191, 9, 0],
            10,
            True,
        ),
        (
            [1, 2, 3, 4, 5],
            [10, 1, 1, 1, 9],
            2.5,
            True,
        ),
        (
            [30, 40, 50, 60, 35],
            [1, 3, 5, 4, 2],
            50,
            True,
        ),
        (
            [2, 0.6, 1.3, 0.3, 0.3, 1.7, 0.7, 1.7, 0.4],
            [2, 2, 0, 1, 2, 2, 1, 6, 0],
            1.7,
            True,
        ),
        (
            [2, 0.6, 1.3, 0.3, 0.3, 1.7, 0.7, 1.7, 0.4],
            [2, 2, 0, 1, 2, 2, 1, 6, 0],
            1.7,
            False,
        ),
        (
            [1.0, 2.0, 400.0, np.nan],
            [np.nan, 2.0, 1.0, 1.0],
            2.0,
            True,
        ),
        (
            [1.0, 2.0, 400.0, np.nan],
            [np.nan, 2.0, 1.0, 1.0],
            np.nan,
            False,
        ),
    ],
)
def test_weighted_median(data, weights, answer, dropna):
    """Test the calculation of a weighted median."""
    df = pd.DataFrame({"data": data, "weights": weights})
    wm = weighted_median(df, data="data", weights="weights", dropna=dropna)

    if not dropna and wm is np.nan:
        assert wm is answer  # nosec: B101
    else:
        assert wm == answer  # nosec: B101
