"""Routines for evaluating and visualizing timeseries data.

Some types of evaluations we'd like to enable:

Scatter plot colored and/or down-selected by an ID or categorical column, comparing two
highly correlated time series. Allow user to select time range or ID/categorical to
plot.

Weighted histogram of a given timeseries variable. Allow selection by ID or categorical
column.

Line plot showing two different timeseries overlaid on each other. Allow zooming and
panning along the whole series. Color code values by a categorical column (imputation
codes).

Static reported vs. imputed values with color coded points for the imputations

"""

import zipfile
from collections.abc import Sequence
from io import BytesIO
from typing import Any

import matplotlib.cm as cm
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

from pudl.metadata.enums import IMPUTATION_CODES


def _filter_df(
    df: pd.DataFrame,
    idx_cols: list[str],
    idx_vals: tuple[Any],
    start_date: str | None = None,
    end_date: str | None = None,
    time_col: str = "datetime_utc",
) -> pd.DataFrame:
    """Filter a dataframe based on index columns and date range."""
    if start_date is None:
        start_date = df[time_col].min()
    if end_date is None:
        end_date = df[time_col].max()
    return (
        df.set_index(idx_cols + [time_col])
        .sort_index()
        .loc[idx_vals]
        .loc[start_date:end_date]
    )


def extract_baseline_eia930_imputation() -> pd.DataFrame:  # pragma: no cover
    """Download and extract an existing imputation of the EIA-930 demand data.

    Useful as a baseline for evaluating our imputation results in development.
    Originally by Tyler Ruggles, Alicia Wongel, and David Farnham (2025). See:
    https://doi.org/10.5281/zenodo.14768167 (data)
    and https://doi.org/10.5281/zenodo.14768152 (code).
    """
    r = requests.get(
        "https://zenodo.org/records/14768167/files/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Data-v1.4.zip?download=1",
        timeout=30,
    )
    f = BytesIO(r.content)
    subregions = []
    base_path = "truggles-EIA_Cleaned_Hourly_Electricity_Demand_Data-5c959df/data/"
    archive = zipfile.Path(f, at=base_path)
    for release in [
        "release_2020_Oct_include_subregions",
        "release_2025_Jan_include_subregions",
    ]:
        for path in (
            archive / release / "subregions_and_balancing_authorities"
        ).iterdir():
            if path.suffix != ".csv":
                continue
            df = pd.read_csv(path.open())

            # Get subregion/ba
            name = path.stem.split("-")
            ba = name[0]
            subregion = None if len(name) == 1 else name[1]
            df["balancing_authority_code_eia"] = ba
            df["balancing_authority_subregion_code_eia"] = subregion

            subregions.append(df)
    df = pd.concat(subregions).rename(
        columns={"cleaned demand (MW)": "baseline_demand_mwh"}
    )
    df["datetime_utc"] = pd.to_datetime(df["date_time"])
    return df[
        [
            "datetime_utc",
            "baseline_demand_mwh",
            "balancing_authority_code_eia",
            "balancing_authority_subregion_code_eia",
        ]
    ]


def plot_correlation(
    df: pd.DataFrame,
    timeseries_x: str,
    timeseries_y: str,
    idx_cols: list[str],
    idx_vals: list[tuple[Any] | str] | None = None,
    xylim: tuple[float] | None = None,
    xlabel: str = "",
    ylabel: str = "",
    title: str = "",
    time_col: str = "datetime_utc",
    start_date: str | None = None,
    end_date: str | None = None,
    log: bool = True,
    legend: bool = True,
    alpha: float = 0.1,
):
    """Plot the correlation between two analogous time series."""
    plt.figure(figsize=(12, 12))

    # Generate a color palette using Matplotlib
    # Use the "tab10" colormap with the required number of colors
    cmap = cm.get_cmap("tab10", len(idx_vals))
    # Convert colors to hex format
    palette = [mcolors.to_hex(cmap(i)) for i in range(len(idx_vals))]
    color_map = {group: palette[i] for i, group in enumerate(idx_vals)}

    for idx in idx_vals:
        filtered = _filter_df(
            df,
            idx_cols=idx_cols,
            idx_vals=idx,
            start_date=start_date,
            end_date=end_date,
            time_col=time_col,
        )
        label = "-".join(str(idx)) if isinstance(idx, tuple) else str(idx)
        plt.scatter(
            filtered[timeseries_x],
            filtered[timeseries_y],
            s=0.1,
            alpha=alpha,
            label=label,
            color=color_map[idx],
        )

    if xylim is not None:
        plt.xlim(xylim)
        plt.ylim(xylim)
    if log:
        plt.xscale("log")
        plt.yscale("log")

    # Add some gridlines
    plt.grid(True, which="both", ls="--", lw=0.5, alpha=0.5)
    # Show line for perfect correlation where x == y
    xlim = plt.gca().get_xlim()
    ylim = plt.gca().get_ylim()
    min_val = max(min(xlim), min(ylim))
    max_val = min(max(xlim), max(ylim))
    plt.plot(
        [min_val, max_val],
        [min_val, max_val],
        linestyle="--",
        color="gray",
        linewidth=0.5,
    )

    if legend:
        leg = plt.legend(
            loc="upper left",
            scatterpoints=3,
            markerscale=10,
            labelspacing=1.2,
        )
        for lh in leg.legend_handles:
            lh.set_alpha(1.0)

    # Label the plot
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)

    plt.tight_layout()
    plt.show()


def plot_imputation(
    df: pd.DataFrame,
    idx_cols: list[str],
    idx_vals: tuple[Any],
    start_date: str,
    end_date: str,
    reported_col: str,
    imputed_col: str,
    time_col: str = "datetime_utc",
    ylabel: str = "Demand [MWh]",
):
    """Compare reported values with imputed values visually.

    Select a particular time series based on the ID columns and limit the data displayed
    based on the provided start and end dates. Plot both the reported and imputed
    values, color coding imputed values based on the reason for imputation.

    """
    # Set the dataframe index to the ID columns and the time column
    # Select specified index values that fall within the specified date range:
    filtered = _filter_df(df, idx_cols, idx_vals, start_date, end_date, time_col)
    plt.figure(figsize=(12, 6))
    plt.plot(
        filtered.index,
        filtered[reported_col],
        lw=1,
        color="gray",
        label="reported",
        alpha=0.5,
    )
    plt.plot(filtered.index, filtered[imputed_col], lw=1, label="imputed")
    for code in IMPUTATION_CODES | {"simulated"}:
        mask = filtered[imputed_col + "_imputation_code"] == code
        plt.scatter(
            filtered.index[mask],
            filtered[imputed_col][mask],
            label=code,
            s=3,
            alpha=0.9,
        )
    plt.title(f"Reported vs Imputed Values for {idx_vals}")
    plt.ylabel(ylabel)
    plt.legend(
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
        borderaxespad=0.0,
        markerscale=2,  # Scale up the marker size in the legend
    )
    plt.tight_layout()  # Adjust layout to make room for the legend
    plt.show()


def plot_compare_imputation(
    df: pd.DataFrame,
    idx_cols: list[str],
    idx_vals: tuple[Any],
    start_date: str,
    end_date: str,
    reported_col: str,
    timeseries_a: str,
    timeseries_b: str,
    time_col: str = "datetime_utc",
    ylabel: str = "Demand [MWh]",
):  # pragma: no cover
    """Plot two timeseries of the same information like demand for comparison."""
    filtered = _filter_df(df, idx_cols, idx_vals, start_date, end_date, time_col)

    # Plot reported
    plt.figure(figsize=(12, 6))
    plt.plot(
        filtered.index,
        filtered[reported_col],
        lw=1,
        color="gray",
        label="reported",
        alpha=0.5,
    )

    plt.plot(filtered.index, filtered[timeseries_a], lw=1, label=timeseries_a)
    plt.plot(filtered.index, filtered[timeseries_b], lw=1, label=timeseries_b, ls="--")
    plt.title(f"{timeseries_a} vs {timeseries_b} for {idx_vals}")
    plt.ylabel(ylabel)
    plt.legend(
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
        borderaxespad=0.0,
    )
    plt.tight_layout()  # Adjust layout to make room for the legend
    plt.show()


###############################################################################
### Old helper functions to adapt or repurpose
###############################################################################


def encode_run_length(
    x: Sequence | np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:  # pragma: no cover
    """Encode vector with run-length encoding.

    Args:
        x: Vector to encode.

    Returns:
        Values and their run lengths.

    Examples:
        >>> x = np.array([0, 1, 1, 0, 1])
        >>> encode_run_length(x)
        (array([0, 1, 0, 1]), array([1, 2, 1, 1]))
        >>> encode_run_length(x.astype('bool'))
        (array([False,  True, False,  True]), array([1, 2, 1, 1]))
        >>> encode_run_length(x.astype('<U1'))
        (array(['0', '1', '0', '1'], dtype='<U1'), array([1, 2, 1, 1]))
        >>> encode_run_length(np.where(x == 0, np.nan, x))
        (array([nan,  1., nan,  1.]), array([1, 2, 1, 1]))
    """
    # Inspired by https://stackoverflow.com/a/32681075
    x = np.asarray(x)
    n = len(x)
    if not n:
        return x, np.array([], dtype=int)
    # Pairwise unequal (string safe)
    y = np.array(x[1:] != x[:-1])
    # Must include last element position
    i = np.append(np.where(y), n - 1)
    lengths = np.diff(np.append(-1, i))
    # starts = np.cumsum(np.append(0, lengths))[:-1]
    return x[i], lengths


def insert_run_length(  # noqa: C901
    x: Sequence | np.ndarray,
    values: Sequence | np.ndarray,
    lengths: Sequence[int],
    mask: Sequence[bool] = None,
    padding: int = 0,
    intersect: bool = False,
) -> np.ndarray:  # pragma: no cover
    """Insert run-length encoded values into a vector.

    Args:
        x: Vector to insert values into.
        values: Values to insert.
        lengths: Length of run to insert for each value in `values`.
        mask: Boolean mask, of the same length as `x`, where values can be inserted.
            By default, values can be inserted anywhere in `x`.
        padding: Minimum space between inserted runs and,
            if `mask` is provided, the edges of masked-out areas.
        intersect: Whether to allow inserted runs to intersect each other.

    Raises:
        ValueError: Padding must zero or greater.
        ValueError: Run length must be greater than zero.
        ValueError: Could not find space for run of length {length}.

    Returns:
        Copy of array `x` with values inserted.

    Example:
        >>> x = [0, 0, 0, 0]
        >>> mask = [True, False, True, True]
        >>> insert_run_length(x, values=[1, 2], lengths=[1, 2], mask=mask)
        array([1, 0, 2, 2])

        If we use unique values for the background and each inserted run,
        the run length encoding of the result (ignoring the background)
        is the same as the inserted run, albeit in a different order.

        >>> x = np.zeros(10, dtype=int)
        >>> values = [1, 2, 3]
        >>> lengths = [1, 2, 3]
        >>> x = insert_run_length(x, values=values, lengths=lengths)
        >>> rvalues, rlengths = encode_run_length(x[x != 0])
        >>> order = np.argsort(rvalues)
        >>> all(rvalues[order] == values) and all(rlengths[order] == lengths)
        True

        Null values can be inserted into a vector such that the new null runs
        match the run length encoding of the existing null runs.

        >>> x = [1, 2, np.nan, np.nan, 5, 6, 7, 8, np.nan]
        >>> is_nan = np.isnan(x)
        >>> rvalues, rlengths = encode_run_length(is_nan)
        >>> xi = insert_run_length(
        ...     x,
        ...     values=[np.nan] * rvalues.sum(),
        ...     lengths=rlengths[rvalues],
        ...     mask=~is_nan
        ... )
        >>> np.isnan(xi).sum() == 2 * is_nan.sum()
        np.True_

        The same as above, with non-zero `padding`, yields a unique solution:

        >>> insert_run_length(
        ...     x,
        ...     values=[np.nan] * rvalues.sum(),
        ...     lengths=rlengths[rvalues],
        ...     mask=~is_nan,
        ...     padding=1
        ... )
        array([nan,  2., nan, nan,  5., nan, nan,  8., nan])
    """
    if padding < 0:
        raise ValueError("Padding must zero or greater")
    # Make a new array to modify in place
    x = np.array(x)
    # Compute runs available for insertions
    if mask is None:
        run_starts = np.array([0])
        run_lengths = np.array([len(x)])
    else:
        mask_values, mask_lengths = encode_run_length(mask)
        run_starts = np.cumsum(np.append(0, mask_lengths))[:-1][mask_values]
        run_lengths = mask_lengths[mask_values]
    if padding:
        # Constrict runs
        run_ends = run_starts + run_lengths
        # Move run starts forward, unless endpoint
        moved = slice(int(run_starts[0] == 0), None)
        run_starts[moved] += padding
        # Move run ends backward, unless endpoint
        moved = slice(None, -1 if run_ends[-1] == len(x) else None)
        run_ends[moved] -= padding
        # Recalculate run lengths and keep runs with positive length
        run_lengths = run_ends - run_starts
        keep = run_lengths > 0
        run_starts = run_starts[keep]
        run_lengths = run_lengths[keep]
    # Grow runs by maximum number of insertions (for speed)
    n_runs = len(run_starts)
    if not intersect:
        buffer = np.zeros(len(values), dtype=int)
        run_starts = np.concatenate((run_starts, buffer))
        run_lengths = np.concatenate((run_lengths, buffer))
    # Initialize random number generator
    rng = np.random.default_rng()
    # Sort insertions from longest to shortest
    order = np.argsort(lengths)[::-1]
    values = np.asarray(values)[order]
    lengths = np.asarray(lengths)[order]
    for value, length in zip(values, lengths, strict=True):
        if length < 1:
            raise ValueError("Run length must be greater than zero")
        # Choose runs of adequate length
        choices = np.nonzero(run_lengths[:n_runs] >= length)[0]
        if not choices.size:
            raise ValueError(f"Could not find space for run of length {length}")
        idx = rng.choice(choices)
        # Choose adequate start position in run
        offset = rng.integers(0, run_lengths[idx] - length, endpoint=True)
        start = run_starts[idx] + offset
        # Insert value
        x[start : start + length] = value
        if intersect:
            continue
        # Update runs
        padded_length = length + padding
        if offset:
            tail = run_lengths[idx] - offset - padded_length
            if tail > 0:
                # Insert run
                run_starts[n_runs] = start + padded_length
                run_lengths[n_runs] = tail
                n_runs += 1
            # Shorten run
            run_lengths[idx] = offset - padding
        else:
            # Shift and shorten run
            run_starts[idx] += padded_length
            run_lengths[idx] -= padded_length
    return x


def summarize_flags(self) -> pd.DataFrame:  # pragma: no cover
    """Summarize flagged values by flag, count and median."""
    stats = {}
    for col in range(self.xi.shape[1]):
        stats[self.columns[col]] = (
            pd.Series(self.xi[:, col])
            .groupby(self.flags[:, col])
            .agg(["count", "median"])
        )
    df = pd.concat(stats, names=["column", "flag"]).reset_index()
    # Sort flags by flagged order
    ordered = df["flag"].astype(pd.CategoricalDtype(set(self.flagged)))
    return df.assign(flag=ordered).sort_values(["column", "flag"])


def plot_flags(self, name: Any = 0) -> None:  # pragma: no cover
    """Plot cleaned series and anomalous values colored by flag.

    Args:
        name: Series to plot, as either an integer index or name in :attr:`columns`.
    """
    if name not in self.columns:
        name = self.columns[name]
    col = list(self.columns).index(name)
    plt.plot(self.index, self.x[:, col], color="lightgrey", marker=".", zorder=1)
    colors = {
        "NEGATIVE_OR_ZERO": "pink",
        "IDENTICAL_RUN": "blue",
        "GLOBAL_OUTLIER": "brown",
        "GLOBAL_OUTLIER_NEIGHBOR": "brown",
        "LOCAL_OUTLIER_HIGH": "purple",
        "LOCAL_OUTLIER_LOW": "purple",
        "DOUBLE_DELTA": "green",
        "SINGLE_DELTA": "red",
        "ANOMALOUS_REGION": "orange",
    }
    for flag in colors:
        mask = self.flags[:, col] == flag
        x, y = self.index[mask], self.xi[mask, col]
        # Set zorder manually to ensure flagged points are drawn on top
        plt.scatter(x, y, c=colors[flag], label=flag, zorder=2)
    plt.legend()


def simulate_nulls(  # pragma: no cover
    self,
    lengths: Sequence[int] = None,
    padding: int = 1,
    intersect: bool = False,
    overlap: bool = False,
) -> np.ndarray:
    """Find non-null values to null to match a run-length distribution.

    Args:
        length: Length of null runs to simulate for each series.
            By default, uses the run lengths of null values in each series.
        padding: Minimum number of non-null values between simulated null runs
            and between simulated and existing null runs.
        intersect: Whether simulated null runs can intersect each other.
        overlap: Whether simulated null runs can overlap existing null runs.
            If `True`, `padding` is ignored.

    Returns:
        Boolean mask of current non-null values to set to null.

    Raises:
        ValueError: Could not find space for run of length {length}.
    """
    new_nulls = np.zeros(self.x.shape, dtype=bool)
    for col in range(self.x.shape[1]):
        is_null = np.isnan(self.x[:, col])
        if lengths is None:
            run_values, run_lengths = encode_run_length(is_null)
            run_lengths = run_lengths[run_values]
        else:
            run_lengths = lengths
        is_new_null = insert_run_length(
            new_nulls[:, col],
            values=np.ones(len(run_lengths), dtype=bool),
            lengths=run_lengths,
            mask=None if overlap else ~is_null,
            padding=0 if overlap else padding,
            intersect=intersect,
        )
        if overlap:
            is_new_null &= ~is_null
        new_nulls[:, col] = is_new_null
    return new_nulls
