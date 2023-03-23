"""Functions for pulling EIA 923 data out of the PUDl DB."""
from datetime import date, datetime
from typing import Literal

import numpy as np
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)


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
    fill: bool = False,
    roll: bool = False,
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

    Optionally fill in missing fuel costs based on monthly state averages
    from the EIA's bulk electricity data, and/or use a rolling average
    to fill in gaps in the fuel costs. These behaviors are controlled by the
    ``fill`` and ``roll`` parameters.

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
        fill: if set to True, fill in missing coal, gas and oil fuel cost per mmbtu from
            EIA's bulk data. This fills with montly state-level averages.
        roll: if set to True, apply a rolling average to a subset of output table's
            columns (currently only 'fuel_cost_per_mmbtu' for the frc table).

    Returns:
        A DataFrame containing records from the EIA 923 Fuel Receipts and Costs table.
    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt["fuel_receipts_costs_eia923"]
    frc_select = sa.sql.select(frc_tbl)

    # Need to re-integrate the MSHA coalmine info:
    cmi_df = pd.read_sql_table("coalmine_eia923", pudl_engine).drop(
        ["data_maturity"], axis="columns"
    )

    if start_date is not None:
        frc_select = frc_select.where(frc_tbl.c.report_date >= start_date)
    if end_date is not None:
        frc_select = frc_select.where(frc_tbl.c.report_date <= end_date)

    frc_df = (
        pd.read_sql(frc_select, pudl_engine)
        .merge(cmi_df, how="left", on="mine_id_pudl")
        .rename(columns={"state": "mine_state"})
        .drop(["mine_id_pudl"], axis="columns")
        .pipe(apply_pudl_dtypes, group="eia")
        .rename(columns={"county_id_fips": "coalmine_county_id_fips"})
    )

    if fill:
        logger.info("filling in fuel cost NaNs")
        frc_df = _impute_via_bulk_elec(frc_df, pudl_engine)
        # frc_df = _impute_via_eia_api(frc_df, pudl_engine)
    # add the flag column to note that we didn't fill in with API data
    else:
        frc_df = frc_df.assign(fuel_cost_from_eiaapi=False)
    # this next step smoothes fuel_cost_per_mmbtu as a rolling monthly average.
    # for each month where there is any data make weighted averages of each
    # plant/fuel/month.
    if roll:
        logger.info("filling in fuel cost NaNs with rolling averages")
        frc_df = pudl.helpers.fillna_w_rolling_avg(
            frc_df,
            group_cols=["plant_id_eia", "energy_source_code"],
            data_col="fuel_cost_per_mmbtu",
            window=12,
            min_periods=6,
            win_type="triang",
        )

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
                "fuel_cost_from_eiaapi": "any",
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
        # There are a couple of invalid records with no specified fuel.
        out_df = out_df.dropna(subset=["fuel_group_code"])

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
    by = [
        "plant_id_eia",
        "boiler_id",
        "energy_source_code",
        "fuel_type_code_pudl",
        "prime_mover_code",
    ]

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
    # merge in the static entity columns
    # don't need to deal with time (freq/end or start dates bc this table is static)
    out_df = out_df.merge(
        pd.read_sql("boilers_entity_eia", pudl_engine),
        how="left",
        on=["plant_id_eia", "boiler_id"],
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


def get_fuel_cost_avg_bulk_elec(pudl_engine: sa.engine.Engine) -> pd.DataFrame:
    """Get state-level average fuel costs from EIA's bulk electricity data.

    This table is intended for use in ``fuel_receipts_costs_eia923()`` as a drop in
    replacement for a previous process that fetched data from the unreliable EIA API.

    Args:
        pudl_engine: SQLAlchemy connection engine for the PUDL DB.

    Returns:
        pandas.DataFrame: a dataframe containing state-level montly fuel cost.
        The table contains the following columns, some of which are refernce
        columns: 'report_date', 'fuel_cost_per_mmbtu', 'state',
        'fuel_type_code_pudl'
    """
    aggregates = pd.read_sql(
        """
        SELECT
            fuel_agg,
            geo_agg,
            report_date,
            fuel_cost_per_mmbtu
        FROM fuel_receipts_costs_aggs_eia
        WHERE
            sector_agg = 'all_electric_power'
            AND temporal_agg = 'monthly'
            AND fuel_agg in ('all_coal', 'petroleum_liquids', 'natural_gas')
            -- geo_agg will take care of itself in the join
            AND fuel_cost_per_mmbtu IS NOT NULL
            ;
        """,
        pudl_engine,
        parse_dates=["report_date"],
    )
    fuel_map = {  # convert to fuel_type_code_pudl categories
        "all_coal": "coal",
        "natural_gas": "gas",
        "petroleum_liquids": "oil",
    }
    aggregates["fuel_type_code_pudl"] = aggregates["fuel_agg"].map(fuel_map)
    aggregates.drop(columns="fuel_agg", inplace=True)

    col_rename_dict = {
        "geo_agg": "state",
        "fuel_cost_per_mmbtu": "bulk_agg_fuel_cost_per_mmbtu",
    }
    aggregates.rename(columns=col_rename_dict, inplace=True)
    return aggregates


def _impute_via_bulk_elec(
    frc_df: pd.DataFrame, pudl_engine: sa.engine.Engine
) -> pd.DataFrame:
    fuel_costs_avg_eia_bulk_elec = get_fuel_cost_avg_bulk_elec(pudl_engine)
    # Merge to bring in states associated with each plant:
    plant_states = pd.read_sql(
        "SELECT plant_id_eia, state FROM plants_entity_eia;", pudl_engine
    )
    out_df = frc_df.merge(plant_states, on="plant_id_eia", how="left")

    # Merge in monthly per-state fuel costs from EIA based on fuel type.
    out_df = out_df.merge(
        fuel_costs_avg_eia_bulk_elec,
        on=["report_date", "state", "fuel_type_code_pudl"],
        how="left",
    )

    out_df["fuel_cost_from_eiaapi"] = (
        # add an indicator column to show if a value has been imputed
        out_df["fuel_cost_per_mmbtu"].isnull()
        & out_df["bulk_agg_fuel_cost_per_mmbtu"].notnull()
    )
    out_df.loc[:, "fuel_cost_per_mmbtu"].fillna(
        out_df["bulk_agg_fuel_cost_per_mmbtu"], inplace=True
    )

    return out_df
