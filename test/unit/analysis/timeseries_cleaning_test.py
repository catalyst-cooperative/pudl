"""Tests for timeseries anomalies detection and imputation."""
from typing import Tuple

import numpy as np

import pudl.analysis.timeseries_cleaning


def simulate_series(
    n: int = 10,
    periods: int = 20,
    frequency: int = 24,
    amplitude_range: Tuple[float, float] = (0.0, 1.0),
    offset_range: Tuple[float, float] = (1.0, 2.0),
    shift_range: Tuple[int, int] = (-3, 3),
) -> np.ndarray:
    """
    Generate synthetic multivariate series from sin functions.

    Args:
        n: Number of variables.
        periods: Number of periods.
        frequency: Number of values in each period.
        amplitude_range: Range of amplitudes.
        offset_range: Range of offsets.
        shift_range: Range of phase shifts (by number of values).

    Returns:
        Multivariate series with shape (`periods * frequency`, `n`).
    """
    rng = np.random.default_rng()
    t = np.arange(periods * frequency) * (2 * np.pi / frequency)
    amplitudes = rng.uniform(*amplitude_range, size=n)
    offsets = rng.uniform(*offset_range, size=n)
    shifts = rng.integers(*shift_range, size=n)
    return np.column_stack([
        offset + np.roll(amplitude * np.sin(t), shift)
        for amplitude, offset, shift in zip(amplitudes, offsets, shifts)
    ])


def simulate_anomalies(
    x: np.ndarray, n: int = 100, sigma: float = 1
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Simulate anomalies in series.

    Args:
        x: Multivariate series with shape (m observations, n variables).
        n: Total number of anomalies to simulate.
        sigma: Standard deviation of the anomalous deviations from `x`.

    Returns:
        Values and flat indices in `x` of the simulated anomalies.
    """
    # nrows, ncols = x.shape
    rng = np.random.default_rng()
    indices = rng.choice(x.size, size=n, replace=False)
    values = rng.normal(scale=sigma, size=n)
    return x.flat[indices] + values, indices


def test_flags_and_imputes_anomalies() -> None:
    """Flags and imputes anomalies within modest thresholds of success."""
    x = simulate_series()
    # Insert anomalies
    values, indices = simulate_anomalies(x)
    x.flat[indices] = values
    # Flag anomalies
    s = pudl.analysis.timeseries_cleaning.Timeseries(x)
    s.flag_ruggles()
    flag_indices = np.flatnonzero(~np.equal(s.flags, None))
    # Flag summary table has the right flag count
    assert s.summarize_flags()['count'].sum() == flag_indices.size
    # Flagged values are 90%+ inserted anomalous values
    assert np.isin(flag_indices, indices).sum() > 0.9 * flag_indices.size
    # Add additional null values alongside nulled anomalies
    mask = s.simulate_nulls()
    for method in 'tubal', 'tnn':
        # Impute null values
        imputed0 = s.impute(mask=mask, method=method, rho0=1, maxiter=1)
        imputed = s.impute(mask=mask, method=method, rho0=1, maxiter=10)
        # Deviations between original and imputed values
        fit0 = s.summarize_imputed(imputed0, mask)
        fit = s.summarize_imputed(imputed, mask)
        # Mean MAPE (mean absolute percent error) is converging
        assert fit['mape'].mean() < fit0['mape'].mean()
