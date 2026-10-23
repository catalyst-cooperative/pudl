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
from io import BytesIO
from typing import Any

import matplotlib as mpl
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import pandas as pd
import requests

from pudl.metadata.enums import IMPUTATION_CODES


def _filter_df(
    df: pd.DataFrame,
    idx_cols: list[str],
    idx_vals: tuple[Any, ...],
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
    idx_vals: list[tuple[Any, ...] | str] | None = None,
    xylim: tuple[float, float] | None = None,
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
    if idx_vals is None:
        idx_vals = []
    plt.figure(figsize=(12, 12))

    cmap = mpl.colormaps["tab10"].resampled(len(idx_vals))
    # Convert colors to hex format
    palette = [mcolors.to_hex(cmap(i)) for i in range(len(idx_vals))]
    color_map = {group: palette[i] for i, group in enumerate(idx_vals)}

    for idx in idx_vals:
        filtered = _filter_df(
            df,
            idx_cols=idx_cols,
            idx_vals=idx if isinstance(idx, tuple) else (idx,),
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
            if lh is not None:
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
    idx_vals: tuple[Any, ...],
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
