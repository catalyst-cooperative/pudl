"""Estimate historical hourly state-level electricity demand.

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
"""

import datetime
from collections.abc import Iterable
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
from dagster import AssetOut, Field, asset, multi_asset

import pudl.analysis.timeseries_cleaning
import pudl.logging_helpers
import pudl.output.pudltabl
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
https://www.iana.org/time-zones)
mapped to their standard-time UTC offset.
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
        local: Local times (tz-naive ``datetime64[ns]``).
        tz: For each time, a timezone (see :meth:`DatetimeIndex.tz_localize`)
            or UTC offset in hours (``int`` or ``float``).
        kwargs: Optional arguments to :meth:`DatetimeIndex.tz_localize`.

    Returns:
        UTC times (tz-naive ``datetime64[ns]``).

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
    return local.groupby(tz, observed=True).transform(
        lambda x: x.dt.tz_localize(
            datetime.timezone(datetime.timedelta(hours=x.name))
            if isinstance(x.name, int | float)
            else x.name,
            **kwargs,
        ).dt.tz_convert(None)
    )


def utc_to_local(utc: pd.Series, tz: Iterable) -> pd.Series:
    """Convert UTC times to local.

    Args:
        utc: UTC times (tz-naive ``datetime64[ns]`` or ``datetime64[ns, UTC]``).
        tz: For each time, a timezone (see :meth:`DatetimeIndex.tz_localize`)
          or UTC offset in hours (``int`` or ``float``).

    Returns:
        Local times (tz-naive ``datetime64[ns]``).

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
    return utc.groupby(tz, observed=True).transform(
        lambda x: x.dt.tz_convert(
            datetime.timezone(datetime.timedelta(hours=x.name))
            if isinstance(x.name, int | float)
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
        * ``state_id_fips``: FIPS code of US state.
        * ``datetime_utc``: UTC time of the start of each hour.
        * ``demand_mwh``: Hourly demand in MWh.
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
    df = df.rename(
        columns={
            "State/Province": "state",
            "Local Datetime (Hour Ending)": "datetime",
            "Estimated State Load MW - Sum": "demand_mwh",
            "Time Zone": "tz",
        },
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
    df["datetime_utc"] = local_to_utc(df["datetime"], df["tz"].map(UTC_OFFSETS))
    # Sum by UTC time
    df = df.groupby(["state_id_fips", "datetime_utc"], as_index=False)[
        "demand_mwh"
    ].sum()
    # Roll back one hour to convert hour-ending to hour-starting
    df["datetime_utc"] -= pd.Timedelta(hours=1)
    return df


# --- Datasets: FERC 714 hourly demand --- #


@multi_asset(
    compute_kind="Python",
    outs={
        "_out_ferc714__hourly_pivoted_demand_matrix": AssetOut(),
        "_out_ferc714__utc_offset": AssetOut(),
    },
)
def load_hourly_demand_matrix_ferc714(
    out_ferc714__hourly_planning_area_demand: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read and format FERC 714 hourly demand into matrix form.

    Args:
        out_ferc714__hourly_planning_area_demand: FERC 714 hourly demand time series by
            planning area.

    Returns:
        Hourly demand as a matrix with a `datetime` row index
        (e.g. '2006-01-01 00:00:00', ..., '2019-12-31 23:00:00')
        in local time ignoring daylight-savings,
        and a `respondent_id_ferc714` column index (e.g. 101, ..., 329).
        A second Dataframe lists the UTC offset in hours
        of each `respondent_id_ferc714` and reporting `year` (int).
    """
    # Convert UTC to local time (ignoring daylight savings)
    out_ferc714__hourly_planning_area_demand["utc_offset"] = (
        out_ferc714__hourly_planning_area_demand["timezone"].map(STANDARD_UTC_OFFSETS)
    )
    out_ferc714__hourly_planning_area_demand["datetime"] = utc_to_local(
        out_ferc714__hourly_planning_area_demand["datetime_utc"],
        out_ferc714__hourly_planning_area_demand["utc_offset"],
    )
    # Pivot to demand matrix: timestamps x respondents
    matrix = out_ferc714__hourly_planning_area_demand.pivot(
        index="datetime", columns="respondent_id_ferc714", values="demand_mwh"
    )
    # List timezone by year for each respondent
    out_ferc714__hourly_planning_area_demand["year"] = (
        out_ferc714__hourly_planning_area_demand["report_date"].dt.year
    )
    utc_offset = out_ferc714__hourly_planning_area_demand.groupby(
        ["respondent_id_ferc714", "year"], as_index=False
    )["utc_offset"].first()
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
        row, col = mask.to_numpy().nonzero()
        report = (
            pd.DataFrame({"id": mask.columns[col], "year": mask.index[row]})
            .groupby("id")["year"]
            .apply(lambda x: np.sort(x))
        )
        with pd.option_context("display.max_colwidth", None):
            logger.info(f"{msg}:\n{report}")
    # Drop respondents with no data
    blank = df.columns[df.isnull().all()].tolist()
    df = df.drop(columns=blank)
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
        Long-format hourly demand with columns ``respondent_id_ferc714``, report
        ``year`` (int), ``datetime_utc``, and ``demand_mwh``.
    """
    # Melt demand matrix to long format
    df = df.melt(value_name="demand_mwh", ignore_index=False)
    df = df.reset_index()
    # Convert local times to UTC
    df["year"] = df["datetime"].dt.year
    df = df.merge(tz, on=["respondent_id_ferc714", "year"])
    df["datetime_utc"] = local_to_utc(df["datetime"], df["utc_offset"])
    df = df.drop(columns=["utc_offset", "datetime"])
    return df


# --- Dagster assets for main functions --- #


@asset(
    compute_kind="pandas",
    config_schema={
        "min_data": Field(
            int,
            default_value=100,
            description=("Minimum number of non-null hours in a year."),
        ),
        "min_data_fraction": Field(
            float,
            default_value=0.9,
            description=(
                "Minimum fraction of non-null hours between the first and last non-null"
                " hour in a year."
            ),
        ),
    },
    op_tags={"memory-use": "high"},
)
def _out_ferc714__hourly_demand_matrix(
    context, _out_ferc714__hourly_pivoted_demand_matrix: pd.DataFrame
) -> pd.DataFrame:
    """Cleaned and nulled FERC 714 hourly demand matrix.

    Args:
        _out_ferc714__hourly_pivoted_demand_matrix: FERC 714 hourly demand data in a
            matrix form.

    Returns:
        Matrix with nulled anomalous values, where respondent-years with too few
        responses are nulled and respondents with no data across all years are dropped.
    """
    min_data = context.op_config["min_data"]
    min_data_fraction = context.op_config["min_data_fraction"]
    df = clean_ferc714_hourly_demand_matrix(_out_ferc714__hourly_pivoted_demand_matrix)
    df = filter_ferc714_hourly_demand_matrix(
        df, min_data=min_data, min_data_fraction=min_data_fraction
    )
    return df


@asset(compute_kind="NumPy")
def _out_ferc714__hourly_imputed_demand(
    _out_ferc714__hourly_demand_matrix: pd.DataFrame,
    _out_ferc714__utc_offset: pd.DataFrame,
) -> pd.DataFrame:
    """Imputed FERC714 hourly demand in long format.

    Impute null values for FERC 714 hourly demand matrix, performing imputation
    separately for each year using only respondents reporting data in that year. Then,
    melt data into a long format.

    Args:
        _out_ferc714__hourly_demand_matrix: Cleaned hourly demand matrix from FERC 714.
        _out_ferc714__utc_offset: Timezone by year for each respondent.

    Returns:
        df: DataFrame with imputed FERC714 hourly demand.
    """
    df = impute_ferc714_hourly_demand_matrix(_out_ferc714__hourly_demand_matrix)
    df = melt_ferc714_hourly_demand_matrix(df, _out_ferc714__utc_offset)
    return df


# --- Datasets: Counties --- #


def county_assignments_ferc714(
    out_ferc714__respondents_with_fips,
) -> pd.DataFrame:
    """Load FERC 714 county assignments.

    Args:
        out_ferc714__respondents_with_fips: From `pudl.output.ferc714`, FERC 714 respondents
            with county FIPS IDs.

    Returns:
        Dataframe with columns
        `respondent_id_ferc714`, report `year` (int), and `county_id_fips`.
    """
    df = out_ferc714__respondents_with_fips[
        ["respondent_id_ferc714", "county_id_fips", "report_date"]
    ]
    # Drop rows where county is blank or a duplicate
    df = df[~df["county_id_fips"].isnull()].drop_duplicates()
    # Convert date to year
    df["year"] = df["report_date"].dt.year
    df = df.drop(columns=["report_date"])
    return df


def census_counties(
    _core_censusdp1tract__counties: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Load county attributes.

    Args:
        county_censusdp: The county layer of the Census DP1 geodatabase.

    Returns:
        Dataframe with columns `county_id_fips` and `population`.
    """
    return _core_censusdp1tract__counties[["geoid10", "dp0010001"]].rename(
        columns={"geoid10": "county_id_fips", "dp0010001": "population"}
    )


# --- Allocation --- #


def total_state_sales_eia861(
    core_eia861__yearly_sales,
) -> pd.DataFrame:
    """Read and format EIA 861 sales by state and year.

    Args:
        core_eia861__yearly_sales: Electricity sales data from EIA 861.

    Returns:
        Dataframe with columns `state_id_fips`, `year`, `demand_mwh`.
    """
    df = core_eia861__yearly_sales.groupby(["state", "report_date"], as_index=False)[
        "sales_mwh"
    ].sum()
    # Convert report_date to year
    df["year"] = df["report_date"].dt.year
    # Convert state abbreviations to FIPS codes
    fips = {x["code"]: x["fips"] for x in STATES}
    df["state_id_fips"] = df["state"].map(fips)
    # Drop records with zero sales
    df = df.rename(columns={"sales_mwh": "demand_mwh"})
    df = df[df["demand_mwh"].gt(0)]
    return df[["state_id_fips", "year", "demand_mwh"]]


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="Python",
    config_schema={
        "mean_overlaps": Field(
            bool,
            default_value=False,
            description=(
                "Whether to mean the demands predicted for a county in cases when a "
                "county is assigned to multiple respondents. By default, demands are "
                "summed."
            ),
        ),
    },
    op_tags={"memory-use": "high"},
)
def out_ferc714__hourly_estimated_state_demand(
    context,
    _out_ferc714__hourly_imputed_demand: pd.DataFrame,
    _core_censusdp1tract__counties: pd.DataFrame,
    out_ferc714__respondents_with_fips: pd.DataFrame,
    core_eia861__yearly_sales: pd.DataFrame = None,
) -> pd.DataFrame:
    """Estimate hourly electricity demand by state.

    Args:
        _out_ferc714__hourly_imputed_demand: Hourly demand timeseries, with columns
            ``respondent_id_ferc714``, report ``year``, ``datetime_utc``, and
            ``demand_mwh``.
        _core_censusdp1tract__counties: The county layer of the Census DP1 shapefile.
        out_ferc714__respondents_with_fips: Annual respondents with the county FIPS IDs
            for their service territories.
        core_eia861__yearly_sales: EIA 861 sales data. If provided, the predicted hourly
            demand is scaled to match these totals.

    Returns:
        Dataframe with columns ``state_id_fips``, ``datetime_utc``, ``demand_mwh``, and
        (if ``state_totals`` was provided) ``scaled_demand_mwh``.
    """
    # Get config
    mean_overlaps = context.op_config["mean_overlaps"]

    # Call necessary functions
    count_assign_ferc714 = county_assignments_ferc714(
        out_ferc714__respondents_with_fips
    )
    counties = census_counties(_core_censusdp1tract__counties)
    total_sales_eia861 = total_state_sales_eia861(core_eia861__yearly_sales)

    # Pre-compute list of respondent-years with demand
    with_demand = (
        _out_ferc714__hourly_imputed_demand.groupby(
            ["respondent_id_ferc714", "year"], as_index=False
        )["demand_mwh"]
        .sum()
        .query("demand_mwh > 0")
    )[["respondent_id_ferc714", "year"]]
    # Pre-compute state-county assignments
    counties["state_id_fips"] = counties["county_id_fips"].str[:2]
    # Merge counties with respondent- and state-county assignments
    df = (
        count_assign_ferc714
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
    df = weights.merge(
        _out_ferc714__hourly_imputed_demand, on=["respondent_id_ferc714", "year"]
    )
    df["demand_mwh"] *= df["weight"]
    # Scale estimates using state totals
    if total_sales_eia861 is not None:
        # Compute scale factor between current and target state totals
        totals = (
            df.groupby(["state_id_fips", "year"], as_index=False)["demand_mwh"]
            .sum()
            .merge(total_sales_eia861, on=["state_id_fips", "year"])
        )
        totals["scale"] = totals["demand_mwh_y"] / totals["demand_mwh_x"]
        df = df.merge(totals[["state_id_fips", "year", "scale"]])
        df["scaled_demand_mwh"] = df["demand_mwh"] * df["scale"]
    # Sum demand by state by matching UTC time
    fields = [x for x in ["demand_mwh", "scaled_demand_mwh"] if x in df]
    return df.groupby(["state_id_fips", "datetime_utc"], as_index=False)[fields].sum()
