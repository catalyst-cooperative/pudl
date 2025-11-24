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
import polars as pl
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
    out_censusdp1tract__counties: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Load county attributes.

    Args:
        out_censusdp1tract__counties: The county layer of the Census DP1 geodatabase.

    Returns:
        Dataframe with columns `county_id_fips` and `population`.
    """
    return out_censusdp1tract__counties[["county_id_fips", "dp0010001"]].rename(
        columns={"dp0010001": "population"}
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
    out_ferc714__hourly_planning_area_demand: pl.LazyFrame,
    out_censusdp1tract__counties: gpd.GeoDataFrame,
    out_ferc714__respondents_with_fips: pd.DataFrame,
    core_eia861__yearly_sales: pd.DataFrame | None = None,
) -> pl.LazyFrame:
    """Estimate hourly electricity demand by state.

    Args:
        out_ferc714__hourly_planning_area_demand: Hourly demand timeseries, with imputed demand.
        out_censusdp1tract__counties: The county layer of the Census DP1 shapefile.
        out_ferc714__respondents_with_fips: Annual respondents with the county FIPS IDs
            for their service territories.
        core_eia861__yearly_sales: EIA 861 sales data. If provided, the predicted hourly
            demand is scaled to match these totals.

    Returns:
        LazyFrame with columns ``state_id_fips``, ``datetime_utc``, ``demand_mwh``, and
        (if ``state_totals`` was provided) ``scaled_demand_mwh``.
    """

    def prepare_county_respondents_with_demand(
        county_assign_ferc714: pd.DataFrame,
        out_censusdp1tract__counties: pd.DataFrame,
        hourly_demand: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Connect respondent- and state-county assignments to additional county data, keeping only respondent-years with nonzero demand."""
        with_demand = (
            hourly_demand.group_by(["respondent_id_ferc714", "year"])
            .agg(pl.col("demand_imputed_pudl_mwh").sum())
            .filter(pl.col("demand_imputed_pudl_mwh") > 0)
            .select(["respondent_id_ferc714", "year"])
        )
        counties = (
            pl.from_pandas(census_counties(out_censusdp1tract__counties))
            .lazy()
            .with_columns(state_id_fips=pl.col("county_id_fips").str.head(2))
        )
        return (
            pl.from_pandas(county_assign_ferc714)
            .lazy()
            .join(with_demand, on=["respondent_id_ferc714", "year"])
            .join(counties, on=["county_id_fips"])
        )

    def weight_counties(df: pl.LazyFrame) -> pl.LazyFrame:
        """Weight counties by population fraction within each respondent-year.

        TODO: Use more county attributes in the calculation of their weights.
        """
        respondent_population = (
            df.group_by(["respondent_id_ferc714", "year"])
            .agg(respondent_population=pl.col("population").sum())
            .select(["respondent_id_ferc714", "year", "respondent_population"])
        )
        return (
            df.join(
                respondent_population,
                on=["respondent_id_ferc714", "year"],
                how="left",
            )
            .with_columns(
                weight=pl.col("population") / pl.col("respondent_population"),
            )
            .drop("respondent_population")
        )

    def normalize_overlaps(df: pl.LazyFrame) -> pl.LazyFrame:
        """Optionally normalize county weights by county occurrences (by year)."""
        if not context.op_config["mean_overlaps"]:
            return df
        return (
            df.join(
                df.group_by(["county_id_fips", "year"]).agg(count=pl.len()),
                on=["county_id_fips", "year"],
            )
            .with_columns(weight=pl.col("weight") / pl.col("count"))
            .drop("count")
        )

    def distribute_demand_to_states(
        df: pl.LazyFrame,
        hourly_demand: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Distribute respondent-year demand among states by weight."""
        return (
            df.group_by(["respondent_id_ferc714", "year", "state_id_fips"])
            .agg(pl.col("weight").sum())
            .select(["respondent_id_ferc714", "year", "state_id_fips", "weight"])
            .join(
                hourly_demand,
                on=["respondent_id_ferc714", "year"],
            )
            .with_columns(
                demand_mwh=pl.col("demand_imputed_pudl_mwh") * pl.col("weight"),
            )
            .drop("demand_imputed_pudl_mwh")
        )

    def rescale_using_sales(
        df: pl.LazyFrame,
        core_eia861__yearly_sales: pd.DataFrame | None,
    ) -> pl.LazyFrame:
        """Optionally scale estimates using state sales data."""
        if core_eia861__yearly_sales is None:
            return df
        return df.join(
            # compute scale factor between current and target state totals
            df.group_by(["state_id_fips", "year"])
            .agg(pl.col("demand_mwh").sum())
            .join(
                pl.from_pandas(
                    total_state_sales_eia861(core_eia861__yearly_sales)
                ).lazy(),
                on=["state_id_fips", "year"],
                suffix="_sales",
            )
            .with_columns(scale=pl.col("demand_mwh_sales") / pl.col("demand_mwh"))
            .select(["state_id_fips", "year", "scale"]),
            on=["state_id_fips", "year"],
        ).with_columns(scaled_demand_mwh=pl.col("demand_mwh") * pl.col("scale"))

    # demand outputs depend on whether we're doing sales adjustments
    demand_cols = ["demand_mwh"]
    if core_eia861__yearly_sales is not None:
        demand_cols.append("scaled_demand_mwh")

    # Switch to polars for the gnarly bits
    hourly_demand = out_ferc714__hourly_planning_area_demand.with_columns(
        year=pl.col("datetime_utc").dt.year()
    )

    estimated_state_demand = (
        prepare_county_respondents_with_demand(
            county_assignments_ferc714(out_ferc714__respondents_with_fips),
            out_censusdp1tract__counties,
            hourly_demand,
        )
        .pipe(weight_counties)
        .pipe(normalize_overlaps)
        .pipe(distribute_demand_to_states, hourly_demand=hourly_demand)
        .pipe(
            rescale_using_sales,
            core_eia861__yearly_sales,
        )
        # sum by state-hour to yield hourly estimates
        .group_by(["state_id_fips", "datetime_utc"])
        .agg(*[pl.col(x).sum() for x in demand_cols])
        .select(["state_id_fips", "datetime_utc"] + demand_cols)
    )
    return estimated_state_demand
