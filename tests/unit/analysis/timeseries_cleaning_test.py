"""Tests for timeseries anomalies detection and imputation."""

import numpy as np
import pandas as pd
import pandera.pandas as pa
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


def test_impute_latc_tubal_deterministic_below_subsample_threshold() -> None:
    """Imputation is bitwise reproducible for series lengths that used to subsample.

    A `dim_time` of ~7,200 (300 days * 24 hours) sat above the old 5,000-step
    subsampling threshold but below the new 10,000-step threshold (PUDL issue
    #5649). It's also representative of a full year of hourly data (<= 8,784
    steps), our actual production use case.
    """
    x = simulate_series(n=3, periods=300, frequency=24, seed=20260922)
    tensor = timeseries_cleaning.fold_tensor(x, periods=24)
    assert tensor.shape[1] * tensor.shape[2] == 7200

    result1 = timeseries_cleaning.impute_latc_tubal(tensor.copy(), maxiter=5, rho0=1)
    result2 = timeseries_cleaning.impute_latc_tubal(tensor.copy(), maxiter=5, rho0=1)

    np.testing.assert_array_equal(result1, result2)


def test_impute_latc_tubal_still_stochastic_above_subsample_threshold() -> None:
    """Subsampling still kicks in, and is still stochastic, above the new threshold.

    Guards against a future edit silently raising the threshold further (or
    deleting the subsampling branch) without anyone noticing: the branch
    should still exist, and still be unseeded, for series long enough that a
    future higher-resolution dataset might need it. See PUDL issue #5649.
    """
    x = simulate_series(n=2, periods=500, frequency=24, seed=20260922)
    # A pure sinusoid repeats exactly every 24 hours, so a linear fit is
    # insensitive to *which* 20% of timesteps get subsampled -- add a little
    # noise so different subsamples actually produce different fits, the same
    # way real, non-perfectly-periodic demand data would.
    x = x + np.random.default_rng(20260922).normal(scale=0.05, size=x.shape)
    # The subsampled fit is only used to fill missing (nulled) values, so
    # without any actual gaps to fill, different subsamples are a no-op.
    # Null out a chunk of values to give the subsampling something to do.
    x[100:150, :] = np.nan
    tensor = timeseries_cleaning.fold_tensor(x, periods=24)
    assert tensor.shape[1] * tensor.shape[2] == 12000

    result1 = timeseries_cleaning.impute_latc_tubal(tensor.copy(), maxiter=2, rho0=1)
    result2 = timeseries_cleaning.impute_latc_tubal(tensor.copy(), maxiter=2, rho0=1)

    assert not np.array_equal(result1, result2)


def test_merge_imputed_preserves_reported_values_for_unflagged_cells() -> None:
    """Unflagged cells keep the reported value, not the model's reconstruction."""
    datetimes = pd.date_range("2025-01-01", periods=5, freq="h", name="datetime")

    @pa.check_types
    def _aligned_df() -> DataFrame[timeseries_cleaning.AlignedTimeseriesDataFrame]:
        return pd.DataFrame(  # type: ignore[bad-return]
            {
                "id_col": ["A"] * 5,
                "datetime": datetimes,
                "value_col": pd.array([10.0, 11.0, 12.0, 13.0, 14.0], dtype="Float64"),
            }
        )

    @pa.check_types
    def _matrix() -> DataFrame[timeseries_cleaning.TimeseriesMatrix]:
        # Simulate an imputation model that reconstructs every cell, including
        # unflagged ones, with small deviations from the reported value.
        matrix = pd.DataFrame(
            {"A": [10.1, 50.0, 51.0, 13.05, 14.02]},
            index=datetimes,
        )
        matrix.columns.name = "id_col"
        return matrix

    @pa.check_types
    def _flags() -> DataFrame[timeseries_cleaning.TimeseriesMatrix]:
        flags = pd.DataFrame(
            {"A": [None, "missing_value", "missing_value", None, None]},
            index=datetimes,
        )
        flags.columns.name = "id_col"
        return flags

    merged = timeseries_cleaning._merge_imputed(_aligned_df(), _matrix(), _flags())
    merged = merged.set_index("datetime")["imputed_value_col"]

    # Unflagged cells: reported value preserved, not the reconstruction.
    assert merged.loc[datetimes[0]] == 10.0
    assert merged.loc[datetimes[3]] == 13.0
    assert merged.loc[datetimes[4]] == 14.0
    # Flagged cells: reconstruction kept, since there's no reported value to
    # fall back on.
    assert merged.loc[datetimes[1]] == 50.0
    assert merged.loc[datetimes[2]] == 51.0


def test_splice_does_not_introduce_large_discontinuities() -> None:
    """Splicing reported values back in shouldn't create visible boundary jumps.

    Mirrors the boundary-jump check run against a production FERC-714 build
    while evaluating PUDL issue #5649: compare the jump at a flagged/unflagged
    boundary with and without splicing reported values back in for unflagged
    cells, and confirm splicing adds only a small fraction of the series'
    normal hour-to-hour variability.
    """
    x = simulate_series(n=5, periods=60, frequency=24, seed=20260922)
    matrix = _to_timeseries_matrix(x, n=5, periods=60, frequency=24)
    reported = matrix.copy()

    # Flag a 48-hour run in the interior of one series.
    start, end = 400, 448
    flagged_matrix = matrix.copy()
    flagged_matrix.iloc[start:end, 0] = np.nan

    imputed = timeseries_cleaning.impute(
        flagged_matrix, method="tubal", rho0=1, maxiter=100
    )
    unflagged_mask = flagged_matrix.notna()
    spliced = imputed.where(~unflagged_mask, reported)

    col = matrix.columns[0]
    baseline = reported[col].diff().abs().median()

    for boundary_current, boundary_reported in [(start - 1, start), (end - 1, end)]:
        current_jump = abs(
            imputed[col].iloc[boundary_current] - imputed[col].iloc[boundary_reported]
        )
        spliced_jump = abs(
            spliced[col].iloc[boundary_current] - spliced[col].iloc[boundary_reported]
        )
        extra_jump = abs(spliced_jump - current_jump)
        assert extra_jump < 2 * baseline


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
