"""Predict state-level electricity demand.

Using hourly electricity demand reported at the balancing authority and utility level in
the FERC 714, and service territories for utilities and balancing autorities inferred
from the counties served by each utility, and the utilities that make up each balancing
authority in the EIA 861, estimate the total hourly electricity demand for each US
state.

This analysis uses the total electricity sales by state reported in the EIA 861 as a
scaling factor to ensure that the magnitude of electricity sales is roughly correct, and
obtains the shape of the demand curve from the hourly planning area demand reported in
the FERC 714.

The compilation of historical service territories based on the EIA 861 data is somewhat
manual and could certainly be improved, but overall the results seem reasonable.
Additional predictive spatial variables will be required to obtain more granular
electricity demand estimates (e.g. at the county level).

Currently the script takes no arguments and simply runs a predefined analysis across all
states and all years for which both EIA 861 and FERC 714 data are available, and outputs
the results as a CSV in PUDL_DIR/local/state-demand/demand.csv
"""
import argparse
import datetime
import pathlib
import sys
from collections.abc import Iterable
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sqlalchemy as sa

import pudl.analysis.timeseries_cleaning
import pudl.logging_helpers
import pudl.output.pudltabl
import pudl.workspace.setup
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)


# --- Constants --- #

STATES: list[dict[str, str]] = [
    {
        "name": x.subdivision_name,
        "code": x.subdivision_code,
        "fips": x.state_id_fips,
    }
    for x in POLITICAL_SUBDIVISIONS.itertuples()
    if x.state_id_fips is not pd.NA
]

STANDARD_UTC_OFFSETS: dict[str, str] = {
    "Pacific/Honolulu": -10,
    "America/Anchorage": -9,
    "America/Los_Angeles": -8,
    "America/Denver": -7,
    "America/Chicago": -6,
    "America/New_York": -5,
    "America/Halifax": -4,
}
"""Hour offset from Coordinated Universal Time (UTC) by time zone.

Time zones are canonical names (e.g. 'America/Denver') from tzdata (
https://www.iana.org/time-zones) mapped to their standard-time UTC offset.
"""


UTC_OFFSETS: dict[str, int] = {
    "HST": -10,
    "AKST": -9,
    "AKDT": -8,
    "PST": -8,
    "PDT": -7,
    "MST": -7,
    "MDT": -6,
    "CST": -6,
    "CDT": -5,
    "EST": -5,
    "EDT": -4,
    "AST": -4,
    "ADT": -3,
}
"""Hour offset from Coordinated Universal Time (UTC) by time zone.

Time zones are either standard or daylight-savings time zone abbreviations (e.g. 'MST').
"""


# --- Helpers --- #


def lookup_state(state: str | int) -> dict:
    """Lookup US state by state identifier.

    Args:
        state: State name, two-letter abbreviation, or FIPS code.
          String matching is case-insensitive.

    Returns:
        State identifers.

    Examples:
        >>> lookup_state('alabama')
        {'name': 'Alabama', 'code': 'AL', 'fips': '01'}
        >>> lookup_state('AL')
        {'name': 'Alabama', 'code': 'AL', 'fips': '01'}
        >>> lookup_state(1)
        {'name': 'Alabama', 'code': 'AL', 'fips': '01'}
    """
    # Try to cast state as an integer to deal with "02", "2", 2.0, np.int64(2)...
    try:
        is_fips = isinstance(int(state), int)
    except ValueError:
        is_fips = False
    if is_fips:
        state = str(int(state)).zfill(2)
        return {x["fips"]: x for x in STATES}[state]
    key = "code" if len(state) == 2 else "name"
    return {x[key].lower(): x for x in STATES}[state.lower()]


def local_to_utc(local: pd.Series, tz: Iterable, **kwargs: Any) -> pd.Series:
    """Convert local times to UTC.

    Args:
        local: Local times (tz-naive `datetime64[ns]`).
        tz: For each time, a timezone (see :meth:`DatetimeIndex.tz_localize`)
          or UTC offset in hours (`int` or `float`).
        kwargs: Optional arguments to :meth:`DatetimeIndex.tz_localize`.

    Returns:
        UTC times (tz-naive `datetime64[ns]`).

    Examples:
        >>> s = pd.Series([pd.Timestamp(2020, 1, 1), pd.Timestamp(2020, 1, 1)])
        >>> local_to_utc(s, [-7, -6])
        0   2020-01-01 07:00:00
        1   2020-01-01 06:00:00
        dtype: datetime64[ns]
        >>> local_to_utc(s, ['America/Denver', 'America/Chicago'])
        0   2020-01-01 07:00:00
        1   2020-01-01 06:00:00
        dtype: datetime64[ns]
    """
    return local.groupby(tz).transform(
        lambda x: x.dt.tz_localize(
            datetime.timezone(datetime.timedelta(hours=x.name))
            if isinstance(x.name, (int, float))
            else x.name,
            **kwargs,
        ).dt.tz_convert(None)
    )


def utc_to_local(utc: pd.Series, tz: Iterable) -> pd.Series:
    """Convert UTC times to local.

    Args:
        utc: UTC times (tz-naive `datetime64[ns]` or `datetime64[ns, UTC]`).
        tz: For each time, a timezone (see :meth:`DatetimeIndex.tz_localize`)
          or UTC offset in hours (`int` or `float`).

    Returns:
        Local times (tz-naive `datetime64[ns]`).

    Examples:
        >>> s = pd.Series([pd.Timestamp(2020, 1, 1), pd.Timestamp(2020, 1, 1)])
        >>> utc_to_local(s, [-7, -6])
        0   2019-12-31 17:00:00
        1   2019-12-31 18:00:00
        dtype: datetime64[ns]
        >>> utc_to_local(s, ['America/Denver', 'America/Chicago'])
        0   2019-12-31 17:00:00
        1   2019-12-31 18:00:00
        dtype: datetime64[ns]
    """
    if utc.dt.tz is None:
        utc = utc.dt.tz_localize("UTC")
    return utc.groupby(tz).transform(
        lambda x: x.dt.tz_convert(
            datetime.timezone(datetime.timedelta(hours=x.name))
            if isinstance(x.name, (int, float))
            else x.name
        ).dt.tz_localize(None)
    )


# --- Datasets: References --- #


def load_ventyx_hourly_state_demand(path: str) -> pd.DataFrame:
    """Read and format Ventyx hourly state-level demand.

    After manual corrections of the listed time zone, ambiguous time zone issues remain.
    Below is a list of transmission zones (by `Transmission Zone ID`)
    with one or more missing timestamps at transitions to or from daylight-savings:

    * 615253 (Indiana)
    * 615261 (Michigan)
    * 615352 (Wisconsin)
    * 615357 (Missouri)
    * 615377 (Saskatchewan)
    * 615401 (Minnesota, Wisconsin)
    * 615516 (Missouri)
    * 615529 (Oklahoma)
    * 615603 (Idaho, Washington)
    * 1836089 (California)

    Args:
        path: Path to the data file (published as 'state_level_load_2007_2018.csv').

    Returns:
        Dataframe with hourly state-level demand.
        * `state_id_fips`: FIPS code of US state.
        * `utc_datetime`: UTC time of the start of each hour.
        * `demand_mwh`: Hourly demand in MWh.
    """
    df = pd.read_csv(
        path,
        usecols=[
            "State/Province",
            "Local Datetime (Hour Ending)",
            "Time Zone",
            "Estimated State Load MW - Sum",
        ],
    )
    df.rename(
        columns={
            "State/Province": "state",
            "Local Datetime (Hour Ending)": "datetime",
            "Estimated State Load MW - Sum": "demand_mwh",
            "Time Zone": "tz",
        },
        inplace=True,
    )
    # Convert state name to FIPS codes and keep only data for US states
    fips = {x["name"]: x["fips"] for x in STATES}
    df["state_id_fips"] = df["state"].map(fips)
    df = df[~df["state_id_fips"].isnull()]
    # Parse datetime
    df["datetime"] = pd.to_datetime(df["datetime"], format="%m/%d/%Y %H:%M")
    # Correct timezone errors
    mask = df["state"].eq("Wyoming") & df["tz"].eq("PST")
    df.loc[mask, "tz"] = "MST"
    # Sum by local time and timezone
    df = df.groupby(["state_id_fips", "datetime", "tz"], as_index=False)[
        "demand_mwh"
    ].sum()
    # Convert local times to UTC
    df["utc_datetime"] = local_to_utc(df["datetime"], df["tz"].map(UTC_OFFSETS))
    # Sum by UTC time
    df = df.groupby(["state_id_fips", "utc_datetime"], as_index=False)[
        "demand_mwh"
    ].sum()
    # Roll back one hour to convert hour-ending to hour-starting
    df["utc_datetime"] -= pd.Timedelta(hours=1)
    return df


# --- Datasets: FERC 714 hourly demand --- #


def load_ferc714_hourly_demand_matrix(
    pudl_out: pudl.output.pudltabl.PudlTabl,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read and format FERC 714 hourly demand into matrix form.

    Args:
        pudl_out: Used to access
          :meth:`pudl.output.pudltabl.PudlTabl.demand_hourly_pa_ferc714`.

    Returns:
        Hourly demand as a matrix with a `datetime` row index
        (e.g. '2006-01-01 00:00:00', ..., '2019-12-31 23:00:00')
        in local time ignoring daylight-savings,
        and a `respondent_id_ferc714` column index (e.g. 101, ..., 329).
        A second Dataframe lists the UTC offset in hours
        of each `respondent_id_ferc714` and reporting `year` (int).
    """
    demand = pudl_out.demand_hourly_pa_ferc714().copy()
    # Convert UTC to local time (ignoring daylight savings)
    demand["utc_offset"] = demand["timezone"].map(STANDARD_UTC_OFFSETS)
    demand["datetime"] = utc_to_local(demand["utc_datetime"], demand["utc_offset"])
    # Pivot to demand matrix: timestamps x respondents
    matrix = demand.pivot(
        index="datetime", columns="respondent_id_ferc714", values="demand_mwh"
    )
    # List timezone by year for each respondent
    demand["year"] = demand["report_date"].dt.year
    utc_offset = demand.groupby(["respondent_id_ferc714", "year"], as_index=False)[
        "utc_offset"
    ].first()
    return matrix, utc_offset


def clean_ferc714_hourly_demand_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Detect and null anomalous values in FERC 714 hourly demand matrix.

    .. note::
        Takes about 10 minutes.

    Args:
        df: FERC 714 hourly demand matrix,
          as described in :func:`load_ferc714_hourly_demand_matrix`.

    Returns:
        Copy of `df` with nulled anomalous values.
    """
    ts = pudl.analysis.timeseries_cleaning.Timeseries(df)
    ts.flag_ruggles()
    return ts.to_dataframe(copy=False)


def filter_ferc714_hourly_demand_matrix(
    df: pd.DataFrame,
    min_data: int = 100,
    min_data_fraction: float = 0.9,
) -> pd.DataFrame:
    """Filter incomplete years from FERC 714 hourly demand matrix.

    Nulls respondent-years with too few data and
    drops respondents with no data across all years.

    Args:
        df: FERC 714 hourly demand matrix,
          as described in :func:`load_ferc714_hourly_demand_matrix`.
        min_data: Minimum number of non-null hours in a year.
        min_data_fraction: Minimum fraction of non-null hours between the first and last
          non-null hour in a year.

    Returns:
        Hourly demand matrix `df` modified in-place.
    """
    # Identify respondent-years where data coverage is below thresholds
    has_data = ~df.isnull()
    coverage = (
        # Last timestamp with demand in year
        has_data[::-1].groupby(df.index.year[::-1]).idxmax()
        -
        # First timestamp with demand in year
        has_data.groupby(df.index.year).idxmax()
    ).apply(lambda x: 1 + x.dt.days * 24 + x.dt.seconds / 3600, axis=1)
    fraction = has_data.groupby(df.index.year).sum() / coverage
    short = coverage.lt(min_data)
    bad = fraction.gt(0) & fraction.lt(min_data_fraction)
    # Set all values in short or bad respondent-years to null
    mask = (short | bad).loc[df.index.year]
    mask.index = df.index
    df[mask] = np.nan
    # Report nulled respondent-years
    for mask, msg in [
        (short, "Nulled short respondent-years (below min_data)"),
        (bad, "Nulled bad respondent-years (below min_data_fraction)"),
    ]:
        row, col = mask.values.nonzero()
        report = (
            pd.DataFrame({"id": mask.columns[col], "year": mask.index[row]})
            .groupby("id")["year"]
            .apply(lambda x: np.sort(x))
        )
        with pd.option_context("display.max_colwidth", -1):
            logger.info(f"{msg}:\n{report}")
    # Drop respondents with no data
    blank = df.columns[df.isnull().all()].tolist()
    df.drop(columns=blank, inplace=True)
    # Report dropped respondents (with no data)
    logger.info(f"Dropped blank respondents: {blank}")
    return df


def impute_ferc714_hourly_demand_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Impute null values in FERC 714 hourly demand matrix.

    Imputation is performed separately for each year,
    with only the respondents reporting data in that year.

    .. note::
        Takes about 15 minutes.

    Args:
        df: FERC 714 hourly demand matrix,
          as described in :func:`load_ferc714_hourly_demand_matrix`.

    Returns:
        Copy of `df` with imputed values.
    """
    results = []
    for year, gdf in df.groupby(df.index.year):
        logger.info(f"Imputing year {year}")
        keep = df.columns[~gdf.isnull().all()]
        tsi = pudl.analysis.timeseries_cleaning.Timeseries(gdf[keep])
        result = tsi.to_dataframe(tsi.impute(method="tnn"), copy=False)
        results.append(result)
    return pd.concat(results)


def melt_ferc714_hourly_demand_matrix(
    df: pd.DataFrame, tz: pd.DataFrame
) -> pd.DataFrame:
    """Melt FERC 714 hourly demand matrix to long format.

    Args:
        df: FERC 714 hourly demand matrix,
          as described in :func:`load_ferc714_hourly_demand_matrix`.
        tz: FERC 714 respondent time zones,
          as described in :func:`load_ferc714_hourly_demand_matrix`.

    Returns:
        Long-format hourly demand with columns
        `respondent_id_ferc714`, report `year` (int), `utc_datetime`, and `demand_mwh`.
    """
    # Melt demand matrix to long format
    df = df.melt(value_name="demand_mwh", ignore_index=False)
    df.reset_index(inplace=True)
    # Convert local times to UTC
    df["year"] = df["datetime"].dt.year
    df = df.merge(tz, on=["respondent_id_ferc714", "year"])
    df["utc_datetime"] = local_to_utc(df["datetime"], df["utc_offset"])
    df.drop(columns=["utc_offset", "datetime"], inplace=True)
    return df


# --- Datasets: Counties --- #


def load_ferc714_county_assignments(
    pudl_out: pudl.output.pudltabl.PudlTabl,
) -> pd.DataFrame:
    """Load FERC 714 county assignments.

    Args:
        pudl_out: PUDL database extractor.

    Returns:
        Dataframe with columns
        `respondent_id_ferc714`, report `year` (int), and `county_id_fips`.
    """
    respondents = pudl.output.ferc714.Respondents(pudl_out)
    df = respondents.fipsify()[
        ["respondent_id_ferc714", "county_id_fips", "report_date"]
    ]
    # Drop rows where county is blank or a duplicate
    df = df[~df["county_id_fips"].isnull()].drop_duplicates()
    # Convert date to year
    df["year"] = df["report_date"].dt.year
    df.drop(columns=["report_date"], inplace=True)
    return df


def load_counties(
    pudl_out: pudl.output.pudltabl.PudlTabl, pudl_settings: dict
) -> pd.DataFrame:
    """Load county attributes.

    Args:
        pudl_out: PUDL database extractor.
        pudl_settings: PUDL settings.

    Returns:
        Dataframe with columns `county_id_fips` and `population`.
    """
    df = pudl.output.censusdp1tract.get_layer(
        layer="county", pudl_settings=pudl_settings
    )[["geoid10", "dp0010001"]]
    return df.rename(columns={"geoid10": "county_id_fips", "dp0010001": "population"})


# --- Allocation --- #


def load_eia861_state_total_sales(
    pudl_out: pudl.output.pudltabl.PudlTabl,
) -> pd.DataFrame:
    """Read and format EIA 861 sales by state and year.

    Args:
        pudl_out: Used to access
          :meth:`pudl.output.pudltabl.PudlTabl.sales_eia861`.

    Returns:
        Dataframe with columns `state_id_fips`, `year`, `demand_mwh`.
    """
    df = pudl_out.sales_eia861()
    df = df.groupby(["state", "report_date"], as_index=False)["sales_mwh"].sum()
    # Convert report_date to year
    df["year"] = df["report_date"].dt.year
    # Convert state abbreviations to FIPS codes
    fips = {x["code"]: x["fips"] for x in STATES}
    df["state_id_fips"] = df["state"].map(fips)
    # Drop records with zero sales
    df.rename(columns={"sales_mwh": "demand_mwh"}, inplace=True)
    df = df[df["demand_mwh"].gt(0)]
    return df[["state_id_fips", "year", "demand_mwh"]]


def predict_state_hourly_demand(
    demand: pd.DataFrame,
    counties: pd.DataFrame,
    assignments: pd.DataFrame,
    state_totals: pd.DataFrame = None,
    mean_overlaps: bool = False,
) -> pd.DataFrame:
    """Predict state hourly demand.

    Args:
        demand: Hourly demand timeseries, with columns
          `respondent_id_ferc714`, report `year`, `utc_datetime`, and `demand_mwh`.
        counties: Counties, with columns `county_id_fips` and `population`.
        assignments: County assignments for demand respondents,
          with columns `respondent_id_ferc714`, `year`, and `county_id_fips`.
        state_totals: Total annual demand by state,
          with columns `state_id_fips`, `year`, and `demand_mwh`.
          If provided, the predicted hourly demand is scaled to match these totals.
        mean_overlaps: Whether to mean the demands predicted for a county
          in cases when a county is assigned to multiple respondents.
          By default, demands are summed.

    Returns:
        Dataframe with columns
        `state_id_fips`, `utc_datetime`, `demand_mwh`, and
        (if `state_totals` was provided) `scaled_demand_mwh`.
    """
    # Pre-compute list of respondent-years with demand
    with_demand = (
        demand.groupby(["respondent_id_ferc714", "year"], as_index=False)["demand_mwh"]
        .sum()
        .query("demand_mwh > 0")
    )[["respondent_id_ferc714", "year"]]
    # Pre-compute state-county assignments
    counties = counties.copy()
    counties["state_id_fips"] = counties["county_id_fips"].str[:2]
    # Merge counties with respondent- and state-county assignments
    df = (
        assignments
        # Drop respondent-years with no demand
        .merge(with_demand, on=["respondent_id_ferc714", "year"])
        # Merge with counties and state-county assignments
        .merge(counties, on=["county_id_fips"])
    )
    # Divide county population by total population in respondent (by year)
    # TODO: Use more county attributes in the calculation of their weights
    totals = df.groupby(["respondent_id_ferc714", "year"])["population"].transform(
        "sum"
    )
    df["weight"] = df["population"] / totals
    # Normalize county weights by county occurences (by year)
    if mean_overlaps:
        counts = df.groupby(["county_id_fips", "year"])["county_id_fips"].transform(
            "count"
        )
        df["weight"] /= counts
    # Sum county weights by respondent, year, and state
    weights = df.groupby(
        ["respondent_id_ferc714", "year", "state_id_fips"], as_index=False
    )["weight"].sum()
    # Multiply respondent-state weights with demands
    df = weights.merge(demand, on=["respondent_id_ferc714", "year"])
    df["demand_mwh"] *= df["weight"]
    # Scale estimates using state totals
    if state_totals is not None:
        # Compute scale factor between current and target state totals
        totals = (
            df.groupby(["state_id_fips", "year"], as_index=False)["demand_mwh"]
            .sum()
            .merge(state_totals, on=["state_id_fips", "year"])
        )
        totals["scale"] = totals["demand_mwh_y"] / totals["demand_mwh_x"]
        df = df.merge(totals[["state_id_fips", "year", "scale"]])
        df["scaled_demand_mwh"] = df["demand_mwh"] * df["scale"]
    # Sum demand by state by matching UTC time
    fields = [x for x in ["demand_mwh", "scaled_demand_mwh"] if x in df]
    return df.groupby(["state_id_fips", "utc_datetime"], as_index=False)[fields].sum()


def plot_demand_timeseries(
    a: pd.DataFrame,
    b: pd.DataFrame = None,
    window: int = 168,
    title: str = None,
    path: str = None,
) -> None:
    """Make a timeseries plot of predicted and reference demand.

    Args:
        a: Predicted demand with columns `utc_datetime` and any of
          `demand_mwh` (in grey) and `scaled_demand_mwh` (in orange).
        b: Reference demand with columns `utc_datetime` and `demand_mwh` (in red).
        window: Width of window (in rows) to use to compute rolling means,
          or `None` to plot raw values.
        title: Plot title.
        path: Plot path. If provided, the figure is saved to file and closed.
    """
    plt.figure(figsize=(16, 8))
    # Plot predicted
    for field, color in [("demand_mwh", "grey"), ("scaled_demand_mwh", "orange")]:
        if field not in a:
            continue
        y = a[field]
        if window:
            y = y.rolling(window).mean()
        plt.plot(
            a["utc_datetime"], y, color=color, alpha=0.5, label=f"Predicted ({field})"
        )
    # Plot expected
    if b is not None:
        y = b["demand_mwh"]
        if window:
            y = y.rolling(window).mean()
        plt.plot(
            b["utc_datetime"], y, color="red", alpha=0.5, label="Reference (demand_mwh)"
        )
    if title:
        plt.title(title)
    plt.ylabel("Demand (MWh)")
    plt.legend()
    if path:
        plt.savefig(path, bbox_inches="tight")
        plt.close()


def plot_demand_scatter(
    a: pd.DataFrame,
    b: pd.DataFrame,
    title: str = None,
    path: str = None,
) -> None:
    """Make a scatter plot comparing predicted and reference demand.

    Args:
        a: Predicted demand with columns `utc_datetime` and any of
          `demand_mwh` (in grey) and `scaled_demand_mwh` (in orange).
        b: Reference demand with columns `utc_datetime` and `demand_mwh`.
          Every element in `utc_datetime` must match the one in `a`.
        title: Plot title.
        path: Plot path. If provided, the figure is saved to file and closed.

    Raises:
        ValueError: Datetime columns do not match.
    """
    if not a["utc_datetime"].equals(b["utc_datetime"]):
        raise ValueError("Datetime columns do not match")
    plt.figure(figsize=(8, 8))
    plt.gca().set_aspect("equal")
    plt.axline((0, 0), (1, 1), linestyle=":", color="grey")
    for field, color in [("demand_mwh", "grey"), ("scaled_demand_mwh", "orange")]:
        if field not in a:
            continue
        plt.scatter(
            b["demand_mwh"],
            a[field],
            c=color,
            s=0.1,
            alpha=0.5,
            label=f"Prediction ({field})",
        )
    if title:
        plt.title(title)
    plt.xlabel("Reference (MWh)")
    plt.ylabel("Predicted (MWh)")
    plt.legend()
    if path:
        plt.savefig(path, bbox_inches="tight")
        plt.close()


def compare_state_demand(
    a: pd.DataFrame, b: pd.DataFrame, scaled: bool = True
) -> pd.DataFrame:
    """Compute statistics comparing predicted and reference demand.

    Statistics are computed for each year.

    Args:
        a: Predicted demand with columns `utc_datetime` and either
          `demand_mwh` (if `scaled=False) or `scaled_demand_mwh` (if `scaled=True`).
        b: Reference demand with columns `utc_datetime` and `demand_mwh`.
          Every element in `utc_datetime` must match the one in `a`.

    Returns:
        Dataframe with columns `year`,
        `rmse` (root mean square error), and `mae` (mean absolute error).

    Raises:
        ValueError: Datetime columns do not match.
    """
    if not a["utc_datetime"].equals(b["utc_datetime"]):
        raise ValueError("Datetime columns do not match")
    field = "scaled_demand_mwh" if scaled else "demand_mwh"
    df = pd.DataFrame(
        {
            "year": a["utc_datetime"].dt.year,
            "diff": a[field] - b["demand_mwh"],
        }
    )
    return df.groupby(["year"], as_index=False)["diff"].agg(
        {
            "rmse": lambda x: np.sqrt(np.sum(x**2) / x.size),
            "mae": lambda x: np.sum(np.abs(x)) / x.size,
        }
    )


# --- Parse Command Line Args --- #
def parse_command_line(argv):
    """Skeletal command line argument parser to provide a help message."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    return parser.parse_args(argv[1:])


# --- Example usage --- #


def main():
    """Predict state demand."""
    # --- Parse command line args --- #
    args = parse_command_line(sys.argv)

    # --- Connect to PUDL logger --- #
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    # --- Connect to PUDL database --- #

    pudl_settings = pudl.workspace.setup.get_defaults()
    pudl_engine = sa.create_engine(pudl_settings["pudl_db"])
    pudl_out = pudl.output.pudltabl.PudlTabl(pudl_engine)

    # --- Prepare FERC 714 hourly demand --- #

    df, tz = load_ferc714_hourly_demand_matrix(pudl_out)
    df = clean_ferc714_hourly_demand_matrix(df)
    df = filter_ferc714_hourly_demand_matrix(df, min_data=100, min_data_fraction=0.9)
    df = impute_ferc714_hourly_demand_matrix(df)
    demand = melt_ferc714_hourly_demand_matrix(df, tz)

    # --- Predict demand --- #

    counties = load_counties(pudl_out, pudl_settings)
    assignments = load_ferc714_county_assignments(pudl_out)
    state_totals = load_eia861_state_total_sales(pudl_out)
    prediction = predict_state_hourly_demand(
        demand,
        counties=counties,
        assignments=assignments,
        state_totals=state_totals,
        mean_overlaps=False,
    )

    # --- Export results --- #

    local_dir = pathlib.Path(pudl_settings["data_dir"]) / "local"
    ventyx_path = local_dir / "ventyx/state_level_load_2007_2018.csv"
    base_dir = local_dir / "state-demand"
    base_dir.mkdir(parents=True, exist_ok=True)
    demand_path = base_dir / "demand.csv"
    stats_path = base_dir / "demand-stats.csv"
    timeseries_dir = base_dir / "timeseries"
    timeseries_dir.mkdir(parents=True, exist_ok=True)
    scatter_dir = base_dir / "scatter"
    scatter_dir.mkdir(parents=True, exist_ok=True)

    # Write predicted hourly state demand
    prediction.to_csv(
        demand_path, index=False, date_format="%Y%m%dT%H", float_format="%.1f"
    )

    # Load Ventyx as reference if available
    reference = None
    if ventyx_path.exists():
        reference = load_ventyx_hourly_state_demand(ventyx_path)

    # Plots and statistics
    stats = []
    for fips in prediction["state_id_fips"].unique():
        state = lookup_state(fips)
        # Filter demand by state
        a = prediction.query(f"state_id_fips == '{fips}'")
        b = None
        title = f'{state["fips"]}: {state["name"]} ({state["code"]})'
        plot_name = f'{state["fips"]}-{state["name"]}.png'
        if reference is not None:
            b = reference.query(f"state_id_fips == '{fips}'")
        # Save timeseries plot
        plot_demand_timeseries(
            a, b=b, window=168, title=title, path=timeseries_dir / plot_name
        )
        if b is None or b.empty:
            continue
        # Align predicted and reference demand
        a = a.set_index("utc_datetime")
        b = b.set_index("utc_datetime")
        index = a.index.intersection(b.index)
        a = a.loc[index].reset_index()
        b = b.loc[index].reset_index()
        # Compute statistics
        stat = compare_state_demand(a, b, scaled=True)
        stat["state_id_fips"] = fips
        stats.append(stat)
        # Save scatter plot
        plot_demand_scatter(a, b=b, title=title, path=scatter_dir / plot_name)

    # Write statistics
    if reference is not None:
        pd.concat(stats, ignore_index=True).to_csv(
            stats_path, index=False, float_format="%.1f"
        )


if __name__ == "__main__":
    sys.exit(main())
