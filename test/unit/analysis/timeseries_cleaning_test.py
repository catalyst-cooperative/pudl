"""Tests for timeseries anomalies detection and imputation."""

import numpy as np
import pandas as pd
import pandera as pa
import pytest
from pandera.typing import DataFrame

from pudl.analysis import timeseries_cleaning


def simulate_series(
    n: int = 10,
    periods: int = 20,
    frequency: int = 24,
    amplitude_range: tuple[float, float] = (0.0, 1.0),
    offset_range: tuple[float, float] = (1.0, 2.0),
    shift_range: tuple[int, int] = (-3, 3),
    seed=None,
) -> np.ndarray:
    """Generate synthetic multivariate series from sin functions.

    Args:
        n: Number of variables.
        periods: Number of periods.
        frequency: Number of values in each period.
        amplitude_range: Range of amplitudes.
        offset_range: Range of offsets.
        shift_range: Range of phase shifts (by number of values).
        seed: Random number generator seed to ensure deterministic tests.

    Returns:
        Multivariate series with shape (`periods * frequency`, `n`).
    """
    rng = np.random.default_rng(seed=seed)
    t = np.arange(periods * frequency) * (2 * np.pi / frequency)
    amplitudes = rng.uniform(*amplitude_range, size=n)
    offsets = rng.uniform(*offset_range, size=n)
    shifts = rng.integers(*shift_range, size=n)
    return np.column_stack(
        [
            offset + np.roll(amplitude * np.sin(t), shift)
            for amplitude, offset, shift in zip(
                amplitudes, offsets, shifts, strict=True
            )
        ]
    )


@pa.check_types
def _to_timeseries_matrix(
    x: np.ndarray,
    n: int = 10,
    periods: int = 20,
    frequency: int = 24,
) -> DataFrame[timeseries_cleaning.TimeseriesMatrix]:
    start_date = "2025-03-20 00:00:00"
    return pd.DataFrame(
        x,
        columns=pd.Index(range(n), name="id_col"),
        index=pd.DatetimeIndex(
            pd.date_range(periods=periods * frequency, freq="h", start=start_date),
            name="datetime",
        ),
    )


def simulate_anomalies(
    x: np.ndarray,
    n: int = 100,
    sigma: float = 1,
    seed=None,
) -> tuple[np.ndarray, np.ndarray]:
    """Simulate anomalies in series.

    Args:
        x: Multivariate series with shape (m observations, n variables).
        n: Total number of anomalies to simulate.
        sigma: Standard deviation of the anomalous deviations from `x`.

    Returns:
        Values and flat indices in `x` of the simulated anomalies.
    """
    # nrows, ncols = x.shape
    rng = np.random.default_rng(seed=seed)
    indices = rng.choice(x.size, size=n, replace=False)
    values = rng.normal(scale=sigma, size=n)
    return x.flat[indices] + values, indices


@pytest.mark.parametrize(
    "series_seed,anomalies_seed",
    [
        (16662093832, 741013840),
        (7088438834, 382046123),
        (11357816575, 18413484987),
        (5150844305, 5634704703),
        (5248964137, 8991153078),
        (2654087352, 8105685070),
        (18949329570, 5605034834),
        (16844944928, 11661181582),
        (5473292783, 5189943010),
        (7173817266, 19937484751),
    ],
)
def test_flags_and_imputes_anomalies(series_seed, anomalies_seed) -> None:
    """Flags and imputes anomalies within modest thresholds of success."""
    x = simulate_series(seed=series_seed)

    # Insert anomalies
    values, indices = simulate_anomalies(x, seed=anomalies_seed)
    x.flat[indices] = values

    # Convert to timeseries matrix
    matrix = _to_timeseries_matrix(x)

    # Flag anomalies
    matrix, flags = timeseries_cleaning.flag_ruggles(matrix)
    flagged_df = timeseries_cleaning.melt_imputed_timeseries_matrix(matrix, flags)

    # Flag summary table has the right flag count
    assert (
        timeseries_cleaning.summarize_flags(
            flagged_df, id_col="id_col", value_col="value_col", flag_col="flags"
        )["count"].sum()
        == flagged_df["flags"].notnull().sum()
    )

    # Flagged values are 90%+ inserted anomalous values
    flag_indices = np.where(flags.notnull().to_numpy().flatten())[0]
    assert np.isin(flag_indices, indices).sum() > 0.9 * flag_indices.size

    # Add additional null values alongside nulled anomalies
    mask = timeseries_cleaning.simulate_nulls(matrix.to_numpy())
    for method in "tubal", "tnn":
        # Impute null values
        imputed0 = timeseries_cleaning.impute(
            matrix, mask=mask, method=method, rho0=1, maxiter=1
        )
        imputed = timeseries_cleaning.impute(
            matrix, mask=mask, method=method, rho0=1, maxiter=100
        )
        # Deviations between original and imputed values
        fit0 = timeseries_cleaning.summarize_imputed(matrix, imputed0, mask)
        fit = timeseries_cleaning.summarize_imputed(matrix, imputed, mask)
        # Mean MAPE (mean absolute percent error) is converging
        assert fit["mape"].mean() < fit0["mape"].mean()
