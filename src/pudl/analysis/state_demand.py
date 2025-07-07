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

import geopandas as gpd
import pandas as pd
from dagster import Field, asset

from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

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

# --- Helpers --- #


def lookup_state(state: str | int) -> dict:
    """Lookup US state by state identifier.

    Args:
        state: State name, two-letter abbreviation, or FIPS code.
          String matching is case-insensitive.

    Returns:
        State identifiers.

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
    out_ferc714__hourly_planning_area_demand: pd.DataFrame,
    _core_censusdp1tract__counties: pd.DataFrame,
    out_ferc714__respondents_with_fips: pd.DataFrame,
    core_eia861__yearly_sales: pd.DataFrame = None,
) -> pd.DataFrame:
    """Estimate hourly electricity demand by state.

    Args:
        out_ferc714__hourly_planning_area_demand: Hourly demand timeseries, with imputed demand.
        _core_censusdp1tract__counties: The county layer of the Census DP1 shapefile.
        out_ferc714__respondents_with_fips: Annual respondents with the county FIPS IDs
            for their service territories.
        core_eia861__yearly_sales: EIA 861 sales data. If provided, the predicted hourly
            demand is scaled to match these totals.

    Returns:
        Dataframe with columns ``state_id_fips``, ``datetime_utc``, ``demand_mwh``, and
        (if ``state_totals`` was provided) ``scaled_demand_mwh``.
    """
    out_ferc714__hourly_planning_area_demand["year"] = (
        out_ferc714__hourly_planning_area_demand.datetime_utc.dt.year
    )
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
        out_ferc714__hourly_planning_area_demand.groupby(
            ["respondent_id_ferc714", "year"], as_index=False
        )["demand_imputed_pudl_mwh"]
        .sum()
        .query("demand_imputed_pudl_mwh > 0")
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
    # Normalize county weights by county occurrences (by year)
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
        out_ferc714__hourly_planning_area_demand, on=["respondent_id_ferc714", "year"]
    ).rename(columns={"demand_imputed_pudl_mwh": "demand_mwh"})
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
