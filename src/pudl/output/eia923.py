"""Functions for pulling EIA 923 data out of the PUDl DB."""
import logging
from collections import OrderedDict
from datetime import date, datetime
from typing import Literal, TypedDict

import numpy as np
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.metadata.enums import STATE_TO_CENSUS_REGION
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)


class FuelPriceAgg(TypedDict):
    """A data structure for storing fuel price aggregation arguments."""

    agg_cols: list[str]
    fuel_group_eiaepm: Literal[
        "all",
        "coal",
        "natural_gas",
        "other_gas",
        "petroleum",
        "petroleum_coke",
    ]


FUEL_PRICE_AGGS: OrderedDict[str, FuelPriceAgg] = OrderedDict(
    {
        # The most precise estimator we have right now
        "state_esc_month": {
            "agg_cols": ["state", "energy_source_code", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        # Good for coal, since price varies much more with location than time
        "state_esc_year": {
            "agg_cols": ["state", "energy_source_code", "report_year"],
            "fuel_group_eiaepm": "coal",
        },
        # Good for oil products, because prices are consistent geographically
        "region_esc_month": {
            "agg_cols": ["census_region", "energy_source_code", "report_date"],
            "fuel_group_eiaepm": "petroleum",
        },
        # Less fuel specificity, but still precise date and location
        "state_fgc_month": {
            "agg_cols": ["state", "fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        # Less location and fuel specificity
        "region_fgc_month": {
            "agg_cols": ["census_region", "fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "region_fgc_year": {
            "agg_cols": ["census_region", "fuel_group_eiaepm", "report_year"],
            "fuel_group_eiaepm": "all",
        },
        "national_esc_month": {
            "agg_cols": ["energy_source_code", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "national_fgc_month": {
            "agg_cols": ["fuel_group_eiaepm", "report_date"],
            "fuel_group_eiaepm": "all",
        },
        "national_fgc_year": {
            "agg_cols": ["fuel_group_eiaepm", "report_year"],
            "fuel_group_eiaepm": "all",
        },
    }
)
"""Fuel price aggregations ordered by precedence for filling missing values."""


def generation_fuel_eia923(
    pudl_engine,
    freq: Literal["AS", "MS", None] = None,
    start_date: str | date | datetime | pd.Timestamp = None,
    end_date: str | date | datetime | pd.Timestamp = None,
    nuclear: bool = False,
):
    """Pull records from the generation_fuel_eia923 table in given date range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.

    - ``plant_id_eia``
    - ``report_date``
    - ``fuel_type_code_pudl``
    - ``fuel_consumed_units``
    - ``fuel_consumed_for_electricity_units``
    - ``fuel_mmbtu_per_unit``
    - ``fuel_consumed_mmbtu``
    - ``fuel_consumed_for_electricity_mmbtu``
    - ``net_generation_mwh``

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        pudl_engine: SQLAlchemy connection engine for the PUDL DB.
        freq: a pandas timeseries offset alias (either "MS" or "AS") or None.
        start_date: date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date: date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        nuclear: If True, return generation_fuel_nuclear_eia923 table.

    Returns:
        A DataFrame containing records from the EIA 923 Generation Fuel table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)

    table_name = (
        "generation_fuel_nuclear_eia923" if nuclear else "generation_fuel_eia923"
    )
    gf_tbl = pt[table_name]

    gf_select = sa.sql.select(gf_tbl)
    if start_date is not None:
        gf_select = gf_select.where(gf_tbl.c.report_date >= start_date)
    if end_date is not None:
        gf_select = gf_select.where(gf_tbl.c.report_date <= end_date)

    gf_df = pd.read_sql(gf_select, pudl_engine)

    by = [
        "plant_id_eia",
        "fuel_type_code_pudl",
        "energy_source_code",
        "prime_mover_code",
    ]
    if nuclear:
        by = by + ["nuclear_unit_id"]
    if freq is not None:
        # Create a date index for temporal resampling:
        gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]

        # Sum up these values so we can calculate quantity weighted averages
        gf_gb = gf_df.groupby(by=by)
        gf_df = gf_gb.agg(
            {
                "fuel_consumed_units": pudl.helpers.sum_na,
                "fuel_consumed_for_electricity_units": pudl.helpers.sum_na,
                "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                "fuel_consumed_for_electricity_mmbtu": pudl.helpers.sum_na,
                "net_generation_mwh": pudl.helpers.sum_na,
            }
        )
        gf_df["fuel_mmbtu_per_unit"] = (
            gf_df["fuel_consumed_mmbtu"] / gf_df["fuel_consumed_units"]
        )

        gf_df = gf_df.reset_index()

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(
        pudl_engine, start_date=start_date, end_date=end_date
    )

    first_cols = [
        "report_date",
        "plant_id_eia",
        "plant_id_pudl",
        "plant_name_eia",
        "utility_id_eia",
        "utility_id_pudl",
        "utility_name_eia",
    ]
    if nuclear:
        first_cols = first_cols + ["nuclear_unit_id"]

    out_df = (
        pudl.helpers.date_merge(
            left=gf_df,
            right=pu_eia,
            on=["plant_id_eia"],
            date_on=["year"],
            how="left",
        )
        # Drop any records where we've failed to get the 860 data merged in...
        .dropna(subset=["plant_id_eia", "utility_id_eia"])
        .pipe(pudl.helpers.organize_cols, first_cols)
        .pipe(apply_pudl_dtypes, group="eia")
    )

    return out_df


def generation_fuel_all_eia923(gf: pd.DataFrame, gfn: pd.DataFrame) -> pd.DataFrame:
    """Combine nuclear and non-nuclear generation fuel tables into a single output.

    The nuclear and non-nuclear generation fuel data are reported at different
    granularities. For non-nuclear generation, each row is a unique combination of date,
    plant ID, prime mover, and fuel type. Nuclear generation is additionally split out
    by nuclear_unit_id (which happens to be the same as generator_id).

    This function aggregates the nuclear data across all nuclear units within a plant so
    that it is structurally the same as the non-nuclear data and can be treated
    identically in subsequent analyses. Then the nuclear and non-nuclear data are
    concatenated into a single dataframe and returned.

    Args:
        gf: non-nuclear generation fuel dataframe.
        gfn: nuclear generation fuel dataframe.

    """
    primary_key = [
        "report_date",
        "plant_id_eia",
        "prime_mover_code",
        "energy_source_code",
    ]
    sum_cols = [
        "fuel_consumed_for_electricity_mmbtu",
        "fuel_consumed_for_electricity_units",
        "fuel_consumed_mmbtu",
        "fuel_consumed_units",
        "net_generation_mwh",
    ]
    other_cols = [
        "nuclear_unit_id",  # dropped in the groupby / aggregation.
        "fuel_mmbtu_per_unit",  # recalculated based on aggregated sum_cols.
    ]
    # Rather than enumerating all of the non-data columns, identify them by process of
    # elimination, in case they change in the future.
    non_data_cols = list(set(gfn.columns) - set(primary_key + sum_cols + other_cols))

    gfn_gb = gfn.groupby(primary_key)
    # Ensure that all non-data columns are homogeneous within groups
    if not (gfn_gb[non_data_cols].nunique() == 1).all(axis=None):
        raise ValueError(
            "Found inhomogeneous non-data cols while aggregating nuclear generation."
        )
    gfn_agg = pd.concat(
        [
            gfn_gb[non_data_cols].first(),
            gfn_gb[sum_cols].sum(min_count=1),
        ],
        axis="columns",
    )
    # Nuclear plants don't report units of fuel consumed, so fuel heat content ends up
    # being calculated as infinite. However, some nuclear plants report using small
    # amounts of DFO. Ensure infite heat contents are set to NA instead:
    gfn_agg = gfn_agg.assign(
        fuel_mmbtu_per_unit=np.where(
            gfn_agg.fuel_consumed_units != 0,
            gfn_agg.fuel_consumed_mmbtu / gfn_agg.fuel_consumed_units,
            np.nan,
        )
    ).reset_index()
    return pd.concat([gfn_agg, gf]).sort_values(primary_key).reset_index(drop=True)


def fuel_receipts_costs_eia923(
    pudl_engine,
    freq: Literal["AS", "MS", None] = None,
    start_date: str | date | datetime | pd.Timestamp = None,
    end_date: str | date | datetime | pd.Timestamp = None,
    fill: bool = True,
    debug: bool = False,
) -> pd.DataFrame:
    """Pull records from ``fuel_receipts_costs_eia923`` table in given date range.

    Optionally, aggregate the records at a monthly or longer timescale, as well
    as by fuel type within a plant, by setting freq to something other than
    the default None value.

    If the records are not being aggregated, then all of the fields found in
    the PUDL database are available.  If they are being aggregated, then the
    following fields are preserved, and appropriately summed or re-calculated
    based on the specified aggregation. In both cases, new total values are
    calculated, for total fuel heat content and total fuel cost.

    - ``plant_id_eia``
    - ``report_date``
    - ``fuel_type_code_pudl`` (formerly energy_source_simple)
    - ``fuel_received_units`` (sum)
    - ``fuel_cost_per_mmbtu`` (weighted average)
    - ``total_fuel_cost`` (sum)
    - ``fuel_consumed_mmbtu`` (sum)
    - ``fuel_mmbtu_per_unit`` (weighted average)
    - ``sulfur_content_pct`` (weighted average)
    - ``ash_content_pct`` (weighted average)
    - ``moisture_content_pct`` (weighted average)
    - ``mercury_content_ppm`` (weighted average)
    - ``chlorine_content_ppm`` (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Optionally fill in missing fuel costs based median values from geographic, temporal,
    and fuel group aggregations.

    Args:
        pudl_engine: SQLAlchemy connection engine for the PUDL DB.
        freq: a pandas timeseries offset alias ("MS" or "AS") or None. The original data
            is reported monthly.
        start_date: date-like object, including a string of the form 'YYYY-MM-DD' which
            will be used to specify the date range of records to be pulled. Dates are
            inclusive.
        end_date: date-like object, including a string of the form 'YYYY-MM-DD' which
            will be used to specify the date range of records to be pulled.  Dates are
            inclusive.
        fill: if True, fill in missing fuel prices based on the median values from
            geographic, temporal, and fuel group aggregations.

    Returns:
        A DataFrame containing records from the EIA 923 Fuel Receipts and Costs table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt["fuel_receipts_costs_eia923"]
    frc_select = sa.sql.select(frc_tbl)

    # Need to re-integrate the MSHA coalmine info:
    cmi_tbl = pt["coalmine_eia923"]
    cmi_select = sa.sql.select(cmi_tbl)
    cmi_df = pd.read_sql(cmi_select, pudl_engine)

    if start_date is not None:
        frc_select = frc_select.where(frc_tbl.c.report_date >= start_date)
    if end_date is not None:
        frc_select = frc_select.where(frc_tbl.c.report_date <= end_date)

    plant_states = pd.read_sql(
        "SELECT plant_id_eia, state FROM plants_entity_eia", pudl_engine
    )
    fuel_group_eiaepm = pd.read_sql(
        "SELECT code AS energy_source_code, fuel_group_eiaepm FROM energy_sources_eia",
        pudl_engine,
    )
    frc_df = (
        pd.read_sql(frc_select, pudl_engine)
        .merge(cmi_df, how="left", on="mine_id_pudl")
        .rename(columns={"state": "mine_state"})
        .drop(["mine_id_pudl"], axis=1)
        .merge(plant_states, on="plant_id_eia", how="left")
        .merge(fuel_group_eiaepm, on="energy_source_code", how="left")
        .pipe(apply_pudl_dtypes, group="eia")
        .rename(columns={"county_id_fips": "coalmine_county_id_fips"})
        .assign(filled_by=pd.NA)
    )

    if fill:
        logger.info("Filling in missing fuel prices using aggregated median values.")
        frc_df = frc_df.assign(
            report_year=lambda x: x.report_date.dt.year,
            census_region=lambda x: x.state.map(STATE_TO_CENSUS_REGION),
            fuel_cost_per_mmbtu=lambda x: x.fuel_cost_per_mmbtu.replace(0.0, np.nan),
        )
        frc_df.loc[frc_df.fuel_cost_per_mmbtu.notna(), ["filled_by"]] = "original"

        for agg in FUEL_PRICE_AGGS:
            agg_cols = FUEL_PRICE_AGGS[agg]["agg_cols"]
            fgc = FUEL_PRICE_AGGS[agg]["fuel_group_eiaepm"]
            frc_df[agg] = frc_df.groupby(agg_cols)["fuel_cost_per_mmbtu"].transform(
                "median"
            )  # could switch to weighted median to avoid right-skew
            frc_df[agg + "_err"] = (
                frc_df[agg] - frc_df.fuel_cost_per_mmbtu
            ) / frc_df.fuel_cost_per_mmbtu
            mask = (
                (frc_df.fuel_cost_per_mmbtu.isna())
                & (frc_df[agg].notna())
                & (True if fgc == "all" else frc_df.fuel_group_eiaepm == fgc)
            )
            frc_df.loc[mask, "filled_by"] = agg
            frc_df.loc[mask, "fuel_cost_per_mmbtu"] = frc_df.loc[mask, agg]
            logger.info(
                f"Filled in {sum(mask)} missing fuel prices with {agg} "
                f"aggregation for fuel group {fgc}."
            )
        # Unless debugging, remove columns used to fill missing fuel prices
        if not debug:
            cols_to_drop = list(FUEL_PRICE_AGGS)
            cols_to_drop += list(c + "_err" for c in cols_to_drop)
            frc_df = frc_df.drop(columns=cols_to_drop)

    # Calculate a few totals that are commonly needed:
    frc_df["fuel_consumed_mmbtu"] = (
        frc_df["fuel_mmbtu_per_unit"] * frc_df["fuel_received_units"]
    )
    frc_df["total_fuel_cost"] = (
        frc_df["fuel_consumed_mmbtu"] * frc_df["fuel_cost_per_mmbtu"]
    )

    if freq is not None:
        by = ["plant_id_eia", "fuel_type_code_pudl", pd.Grouper(freq=freq)]
        # Create a date index for temporal resampling:
        frc_df = frc_df.set_index(pd.DatetimeIndex(frc_df.report_date))
        # Sum up these values so we can calculate quantity weighted averages
        frc_df["total_ash_content"] = (
            frc_df["ash_content_pct"] * frc_df["fuel_received_units"]
        )
        frc_df["total_sulfur_content"] = (
            frc_df["sulfur_content_pct"] * frc_df["fuel_received_units"]
        )
        frc_df["total_mercury_content"] = (
            frc_df["mercury_content_ppm"] * frc_df["fuel_received_units"]
        )
        frc_df["total_moisture_content"] = (
            frc_df["moisture_content_pct"] * frc_df["fuel_received_units"]
        )
        frc_df["total_chlorine_content"] = (
            frc_df["chlorine_content_ppm"] * frc_df["fuel_received_units"]
        )

        frc_gb = frc_df.groupby(by=by)
        frc_df = frc_gb.agg(
            {
                "fuel_received_units": pudl.helpers.sum_na,
                "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                "total_fuel_cost": pudl.helpers.sum_na,
                "total_sulfur_content": pudl.helpers.sum_na,
                "total_ash_content": pudl.helpers.sum_na,
                "total_mercury_content": pudl.helpers.sum_na,
                "total_moisture_content": pudl.helpers.sum_na,
                "total_chlorine_content": pudl.helpers.sum_na,
                "filled_by": "any",
            }
        )
        frc_df["fuel_cost_per_mmbtu"] = (
            frc_df["total_fuel_cost"] / frc_df["fuel_consumed_mmbtu"]
        )
        frc_df["fuel_mmbtu_per_unit"] = (
            frc_df["fuel_consumed_mmbtu"] / frc_df["fuel_received_units"]
        )
        frc_df["sulfur_content_pct"] = (
            frc_df["total_sulfur_content"] / frc_df["fuel_received_units"]
        )
        frc_df["ash_content_pct"] = (
            frc_df["total_ash_content"] / frc_df["fuel_received_units"]
        )
        frc_df["mercury_content_ppm"] = (
            frc_df["total_mercury_content"] / frc_df["fuel_received_units"]
        )
        frc_df["chlorine_content_ppm"] = (
            frc_df["total_chlorine_content"] / frc_df["fuel_received_units"]
        )
        frc_df["moisture_content_pct"] = (
            frc_df["total_moisture_content"] / frc_df["fuel_received_units"]
        )
        frc_df = frc_df.reset_index()
        frc_df = frc_df.drop(
            [
                "total_ash_content",
                "total_sulfur_content",
                "total_moisture_content",
                "total_chlorine_content",
                "total_mercury_content",
            ],
            axis=1,
        )

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(
        pudl_engine, start_date=start_date, end_date=end_date
    )

    out_df = (
        pudl.helpers.date_merge(
            left=frc_df,
            right=pu_eia,
            on=["plant_id_eia"],
            date_on=["year"],
            how="left",
        )
        .dropna(subset=["utility_id_eia"])
        .pipe(
            pudl.helpers.organize_cols,
            cols=[
                "report_date",
                "plant_id_eia",
                "plant_id_pudl",
                "plant_name_eia",
                "utility_id_eia",
                "utility_id_pudl",
                "utility_name_eia",
            ],
        )
        .pipe(apply_pudl_dtypes, group="eia")
    )

    if freq is None:
        # There are a couple of records with no energy_source_code specified.
        out_df = out_df.dropna(subset=["fuel_group_eiaepm"])

    return out_df


def boiler_fuel_eia923(pudl_engine, freq=None, start_date=None, end_date=None):
    """Pull records from the boiler_fuel_eia923 table in a given data range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.

    * ``fuel_consumed_units`` (sum)
    * ``fuel_mmbtu_per_unit`` (weighted average)
    * ``fuel_consumed_mmbtu`` (sum)
    * ``sulfur_content_pct`` (weighted average)
    * ``ash_content_pct`` (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all records from the EIA 923
        Boiler Fuel table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    bf_eia923_tbl = pt["boiler_fuel_eia923"]
    bf_eia923_select = sa.sql.select(bf_eia923_tbl)
    if start_date is not None:
        bf_eia923_select = bf_eia923_select.where(
            bf_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        bf_eia923_select = bf_eia923_select.where(
            bf_eia923_tbl.c.report_date <= end_date
        )
    bf_df = pd.read_sql(bf_eia923_select, pudl_engine)

    # The total heat content is also useful in its own right, and we'll keep it
    # around.  Also needed to calculate average heat content per unit of fuel.
    bf_df["fuel_consumed_mmbtu"] = (
        bf_df["fuel_consumed_units"] * bf_df["fuel_mmbtu_per_unit"]
    )

    # Create a date index for grouping based on freq
    by = ["plant_id_eia", "boiler_id", "energy_source_code", "fuel_type_code_pudl"]
    if freq is not None:
        # In order to calculate the weighted average sulfur
        # content and ash content we need to calculate these totals.
        bf_df["total_sulfur_content"] = (
            bf_df["fuel_consumed_units"] * bf_df["sulfur_content_pct"]
        )
        bf_df["total_ash_content"] = (
            bf_df["fuel_consumed_units"] * bf_df["ash_content_pct"]
        )
        bf_df = bf_df.set_index(pd.DatetimeIndex(bf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        bf_gb = bf_df.groupby(by=by)

        # Sum up these totals within each group, and recalculate the per-unit
        # values (weighted in this case by fuel_consumed_units)
        bf_df = bf_gb.agg(
            {
                "fuel_consumed_mmbtu": pudl.helpers.sum_na,
                "fuel_consumed_units": pudl.helpers.sum_na,
                "total_sulfur_content": pudl.helpers.sum_na,
                "total_ash_content": pudl.helpers.sum_na,
            }
        )

        bf_df["fuel_mmbtu_per_unit"] = (
            bf_df["fuel_consumed_mmbtu"] / bf_df["fuel_consumed_units"]
        )
        bf_df["sulfur_content_pct"] = (
            bf_df["total_sulfur_content"] / bf_df["fuel_consumed_units"]
        )
        bf_df["ash_content_pct"] = (
            bf_df["total_ash_content"] / bf_df["fuel_consumed_units"]
        )
        bf_df = bf_df.reset_index()
        bf_df = bf_df.drop(["total_ash_content", "total_sulfur_content"], axis=1)

    # Grab some basic plant & utility information to add.
    pu_eia = pudl.output.eia860.plants_utils_eia860(
        pudl_engine, start_date=start_date, end_date=end_date
    )

    out_df = pudl.helpers.date_merge(
        left=bf_df,
        right=pu_eia,
        on=["plant_id_eia"],
        date_on=["year"],
        how="left",
    ).dropna(subset=["plant_id_eia", "utility_id_eia", "boiler_id"])
    # Merge in the unit_id_pudl assigned to each generator in the BGA process
    # Pull the BGA table and make it unit-boiler only:
    bga_boilers = (
        pudl.output.eia860.boiler_generator_assn_eia860(
            pudl_engine, start_date=start_date, end_date=end_date
        )
        .loc[:, ["report_date", "plant_id_eia", "boiler_id", "unit_id_pudl"]]
        .drop_duplicates()
    )

    out_df = pudl.helpers.date_merge(
        left=out_df,
        right=bga_boilers,
        on=["plant_id_eia", "boiler_id"],
        date_on=["year"],
        how="left",
    )
    out_df = pudl.helpers.organize_cols(
        out_df,
        cols=[
            "report_date",
            "plant_id_eia",
            "plant_id_pudl",
            "plant_name_eia",
            "utility_id_eia",
            "utility_id_pudl",
            "utility_name_eia",
            "boiler_id",
            "unit_id_pudl",
        ],
    ).pipe(apply_pudl_dtypes, group="eia")

    return out_df


def generation_eia923(pudl_engine, freq=None, start_date=None, end_date=None):
    """Pull records from the boiler_fuel_eia923 table in a given data range.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
        pandas.DataFrame: A DataFrame containing all records from the EIA 923
        Generation table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    g_eia923_tbl = pt["generation_eia923"]
    g_eia923_select = sa.sql.select(g_eia923_tbl)
    if start_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        g_eia923_select = g_eia923_select.where(g_eia923_tbl.c.report_date <= end_date)
    g_df = pd.read_sql(g_eia923_select, pudl_engine)

    # Index by date and aggregate net generation.
    # Create a date index for grouping based on freq
    by = ["plant_id_eia", "generator_id"]
    if freq is not None:
        g_df = g_df.set_index(pd.DatetimeIndex(g_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        g_gb = g_df.groupby(by=by)
        g_df = g_gb.agg({"net_generation_mwh": pudl.helpers.sum_na}).reset_index()

    out_df = denorm_generation_eia923(g_df, pudl_engine, start_date, end_date)

    return out_df


def denorm_generation_eia923(g_df, pudl_engine, start_date, end_date):
    """Denomralize generation_eia923 table.

    Args:
        g_df (pandas.DataFrame): generation_eia923 table. Should have columns:
            ["plant_id_eia", "generator_id", "report_date",
            "net_generation_mwh"]
        pudl_engine (sqlalchemy.engine.Engine): SQLAlchemy connection engine
            for the PUDL DB.
        start_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        end_date (date-like): date-like object, including a string of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
    """
    # Grab EIA 860 plant and utility specific information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(
        pudl_engine, start_date=start_date, end_date=end_date
    )

    # Merge annual plant/utility data in with the more granular dataframe
    out_df = pudl.helpers.date_merge(
        left=g_df,
        right=pu_eia,
        on=["plant_id_eia"],
        date_on=["year"],
        how="left",
    ).dropna(subset=["plant_id_eia", "utility_id_eia", "generator_id"])

    # Merge in the unit_id_pudl assigned to each generator in the BGA process
    # Pull the BGA table and make it unit-generator only:
    bga_gens = (
        pudl.output.eia860.boiler_generator_assn_eia860(
            pudl_engine, start_date=start_date, end_date=end_date
        )
        .loc[:, ["report_date", "plant_id_eia", "generator_id", "unit_id_pudl"]]
        .drop_duplicates()
    )
    out_df = pudl.helpers.date_merge(
        left=out_df,
        right=bga_gens,
        on=["plant_id_eia", "generator_id"],
        date_on=["year"],
        how="left",
    )
    out_df = out_df.pipe(
        pudl.helpers.organize_cols,
        cols=[
            "report_date",
            "plant_id_eia",
            "plant_id_pudl",
            "plant_name_eia",
            "utility_id_eia",
            "utility_id_pudl",
            "utility_name_eia",
            "generator_id",
        ],
    ).pipe(apply_pudl_dtypes, group="eia")
    return out_df
