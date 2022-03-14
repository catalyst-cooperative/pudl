
"""Functions for pulling EIA 923 data out of the PUDl DB."""
import logging
import os
from datetime import date, datetime
from typing import Literal, Union

import numpy as np
import pandas as pd
import requests
import sqlalchemy as sa

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = logging.getLogger(__name__)

BASE_URL_EIA = "https://api.eia.gov/"

FUEL_TYPE_EIAAPI_MAP = {
    "COW": "coal",
    "NG": "gas",
    "PEL": "oil",
}

FUEL_COST_CATEGORIES_EIAAPI = [41696, 41762, 41740]
"""
The category ids for fuel costs by fuel for electricity for coal, gas and oil.

Each category id is a peice of a query to EIA's API. Each query here contains
a set of state-level child series which contain fuel cost data.

See EIA's query browse here:
 - Coal: https://www.eia.gov/opendata/qb.php?category=41696
 - Gas: https://www.eia.gov/opendata/qb.php?category=41762
 - Oil: https://www.eia.gov/opendata/qb.php?category=41740

"""


def generation_fuel_eia923(
    pudl_engine,
    freq: Literal["AS", "MS", None] = None,
    start_date: Union[str, date, datetime, pd.Timestamp] = None,
    end_date: Union[str, date, datetime, pd.Timestamp] = None,
    nuclear: bool = False
):
    """
    Pull records from the generation_fuel_eia923 table in given date range.

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

    table_name = "generation_fuel_nuclear_eia923" if nuclear else "generation_fuel_eia923"
    gf_tbl = pt[table_name]

    gf_select = sa.sql.select(gf_tbl)
    if start_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date >= start_date)
    if end_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date <= end_date)

    gf_df = pd.read_sql(gf_select, pudl_engine)

    by = [
        'plant_id_eia',
        'fuel_type_code_pudl',
        'energy_source_code',
        'prime_mover_code',
    ]
    if nuclear:
        by = by + ['nuclear_unit_id']
    if freq is not None:
        # Create a date index for temporal resampling:
        gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]

        # Sum up these values so we can calculate quantity weighted averages
        gf_gb = gf_df.groupby(by=by)
        gf_df = gf_gb.agg({
            'fuel_consumed_units': pudl.helpers.sum_na,
            'fuel_consumed_for_electricity_units': pudl.helpers.sum_na,
            'fuel_consumed_mmbtu': pudl.helpers.sum_na,
            'fuel_consumed_for_electricity_mmbtu': pudl.helpers.sum_na,
            'net_generation_mwh': pudl.helpers.sum_na,
        })
        gf_df['fuel_mmbtu_per_unit'] = \
            gf_df['fuel_consumed_mmbtu'] / gf_df['fuel_consumed_units']

        gf_df = gf_df.reset_index()

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name_eia',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
    ]
    if nuclear:
        first_cols = first_cols + ['nuclear_unit_id']

    out_df = (
        pudl.helpers.clean_merge_asof(
            left=gf_df,
            right=pu_eia,
            by=["plant_id_eia"],
        )
        # Drop any records where we've failed to get the 860 data merged in...
        .dropna(subset=['plant_id_eia', 'utility_id_eia'])
        .pipe(pudl.helpers.organize_cols, first_cols)
        .pipe(apply_pudl_dtypes, group="eia")
    )

    return out_df


def generation_fuel_all_eia923(gf: pd.DataFrame, gfn: pd.DataFrame) -> pd.DataFrame:
    """
    Combine nuclear and non-nuclear generation fuel tables into a single output.

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
    non_data_cols = list(
        set(gfn.columns) - set(primary_key + sum_cols + other_cols)
    )

    gfn_gb = gfn.groupby(primary_key)
    # Ensure that all non-data columns are homogeneous within groups
    if not (gfn_gb[non_data_cols].nunique() == 1).all(axis=None):
        raise ValueError(
            "Found inhomogeneous non-data cols while aggregating nuclear generation."
        )
    gfn_agg = pd.concat([
        gfn_gb[non_data_cols].first(),
        gfn_gb[sum_cols].sum(min_count=1),
    ], axis="columns")
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
    return (
        pd.concat([gfn_agg, gf])
        .sort_values(primary_key)
        .reset_index(drop=True)
    )


def fuel_receipts_costs_eia923(
    pudl_engine,
    freq: Literal["AS", "MS", None] = None,
    start_date: Union[str, date, datetime, pd.Timestamp] = None,
    end_date: Union[str, date, datetime, pd.Timestamp] = None,
    fill: bool = False,
    roll: bool = False,
) -> pd.DataFrame:
    """
    Pull records from ``fuel_receipts_costs_eia923`` table in given date range.

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
    which are pulled from the EIA's open data API, and/or use a rolling average
    to fill in gaps in the fuel costs. These behaviors are controlled by the
    ``fill`` and ``roll`` parameters. If you set ``fill=True`` you need to
    ensure that you have stored your API key in an environment variable named
    ``API_KEY_EIA``. You can register for a free EIA API key here:

    https://www.eia.gov/opendata/register.php


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
            EIA's API. This fills with montly state-level averages.
        roll: if set to True, apply a rolling average to a subset of output table's
            columns (currently only 'fuel_cost_per_mmbtu' for the frc table).

    Returns:
        A DataFrame containing records from the EIA 923 Fuel Receipts and Costs table.

    """
    if fill:
        _check_eia_api_key()
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt['fuel_receipts_costs_eia923']
    frc_select = sa.sql.select(frc_tbl)

    # Need to re-integrate the MSHA coalmine info:
    cmi_tbl = pt['coalmine_eia923']
    cmi_select = sa.sql.select(cmi_tbl)
    cmi_df = pd.read_sql(cmi_select, pudl_engine)

    if start_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date >= start_date)
    if end_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date <= end_date)

    frc_df = (
        pd.read_sql(frc_select, pudl_engine)
        .merge(cmi_df, how='left', on='mine_id_pudl')
        .rename(columns={"state": "mine_state"})
        .drop(['mine_id_pudl'], axis=1)
        .pipe(apply_pudl_dtypes, group="eia")
        .rename(columns={'county_id_fips': 'coalmine_county_id_fips'})
    )

    if fill:
        logger.info('filling in fuel cost NaNs EIA APIs monthly state averages')
        fuel_costs_avg_eiaapi = get_fuel_cost_avg_eiaapi(
            FUEL_COST_CATEGORIES_EIAAPI)
        # Merge to bring in states associated with each plant:
        plant_states = pd.read_sql(
            "SELECT plant_id_eia, state FROM plants_entity_eia;", pudl_engine
        )
        frc_df = frc_df.merge(plant_states, on="plant_id_eia", how="left")

        # Merge in monthly per-state fuel costs from EIA based on fuel type.
        frc_df = frc_df.merge(
            fuel_costs_avg_eiaapi,
            on=['report_date', 'state', 'fuel_type_code_pudl'],
            how='left',
        )
        frc_df = frc_df.assign(
            # add a flag column to note if we are using the api data
            fuel_cost_from_eiaapi=lambda x:
                np.where(
                    x.fuel_cost_per_mmbtu.isnull() & x.fuel_cost_per_unit.notnull(),
                    True,
                    False
                ),
            fuel_cost_per_mmbtu=lambda x:
                np.where(
                    x.fuel_cost_per_mmbtu.isnull(),
                    (x.fuel_cost_per_unit / x.fuel_mmbtu_per_unit),
                    x.fuel_cost_per_mmbtu
                )
        )
    # add the flag column to note that we didn't fill in with API data
    else:
        frc_df = frc_df.assign(fuel_cost_from_eiaapi=False)
    # this next step smoothes fuel_cost_per_mmbtu as a rolling monthly average.
    # for each month where there is any data make weighted averages of each
    # plant/fuel/month.
    if roll:
        logger.info('filling in fuel cost NaNs with rolling averages')
        frc_df = pudl.helpers.fillna_w_rolling_avg(
            frc_df,
            group_cols=['plant_id_eia', 'energy_source_code'],
            data_col='fuel_cost_per_mmbtu',
            window=12,
            min_periods=6,
            win_type='triang'
        )

    # Calculate a few totals that are commonly needed:
    frc_df['fuel_consumed_mmbtu'] = \
        frc_df['fuel_mmbtu_per_unit'] * frc_df['fuel_received_units']
    frc_df['total_fuel_cost'] = \
        frc_df['fuel_consumed_mmbtu'] * frc_df['fuel_cost_per_mmbtu']

    if freq is not None:
        by = ['plant_id_eia', 'fuel_type_code_pudl', pd.Grouper(freq=freq)]
        # Create a date index for temporal resampling:
        frc_df = frc_df.set_index(pd.DatetimeIndex(frc_df.report_date))
        # Sum up these values so we can calculate quantity weighted averages
        frc_df['total_ash_content'] = \
            frc_df['ash_content_pct'] * frc_df['fuel_received_units']
        frc_df['total_sulfur_content'] = \
            frc_df['sulfur_content_pct'] * frc_df['fuel_received_units']
        frc_df['total_mercury_content'] = \
            frc_df['mercury_content_ppm'] * frc_df['fuel_received_units']
        frc_df['total_moisture_content'] = \
            frc_df['moisture_content_pct'] * frc_df['fuel_received_units']
        frc_df['total_chlorine_content'] = \
            frc_df['chlorine_content_ppm'] * frc_df['fuel_received_units']

        frc_gb = frc_df.groupby(by=by)
        frc_df = frc_gb.agg({
            'fuel_received_units': pudl.helpers.sum_na,
            'fuel_consumed_mmbtu': pudl.helpers.sum_na,
            'total_fuel_cost': pudl.helpers.sum_na,
            'total_sulfur_content': pudl.helpers.sum_na,
            'total_ash_content': pudl.helpers.sum_na,
            'total_mercury_content': pudl.helpers.sum_na,
            'total_moisture_content': pudl.helpers.sum_na,
            'total_chlorine_content': pudl.helpers.sum_na,
            'fuel_cost_from_eiaapi': 'any',
        })
        frc_df['fuel_cost_per_mmbtu'] = \
            frc_df['total_fuel_cost'] / frc_df['fuel_consumed_mmbtu']
        frc_df['fuel_mmbtu_per_unit'] = \
            frc_df['fuel_consumed_mmbtu'] / frc_df['fuel_received_units']
        frc_df['sulfur_content_pct'] = \
            frc_df['total_sulfur_content'] / frc_df['fuel_received_units']
        frc_df['ash_content_pct'] = \
            frc_df['total_ash_content'] / frc_df['fuel_received_units']
        frc_df['mercury_content_ppm'] = \
            frc_df['total_mercury_content'] / frc_df['fuel_received_units']
        frc_df['chlorine_content_ppm'] = \
            frc_df['total_chlorine_content'] / frc_df['fuel_received_units']
        frc_df['moisture_content_pct'] = \
            frc_df['total_moisture_content'] / frc_df['fuel_received_units']
        frc_df = frc_df.reset_index()
        frc_df = frc_df.drop([
            'total_ash_content',
            'total_sulfur_content',
            'total_moisture_content',
            'total_chlorine_content',
            'total_mercury_content'
        ], axis=1)

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(
        pudl_engine,
        start_date=start_date,
        end_date=end_date
    )

    out_df = (
        pudl.helpers.clean_merge_asof(
            left=frc_df,
            right=pu_eia,
            by=["plant_id_eia"],
        )
        .dropna(subset=['utility_id_eia'])
        .pipe(
            pudl.helpers.organize_cols,
            cols=[
                'report_date',
                'plant_id_eia',
                'plant_id_pudl',
                'plant_name_eia',
                'utility_id_eia',
                'utility_id_pudl',
                'utility_name_eia',
            ]
        )
        .pipe(apply_pudl_dtypes, group="eia")
    )

    if freq is None:
        # There are a couple of invalid records with no specified fuel.
        out_df = out_df.dropna(subset=['fuel_group_code'])

    return out_df


def boiler_fuel_eia923(pudl_engine, freq=None,
                       start_date=None, end_date=None):
    """
    Pull records from the boiler_fuel_eia923 table in a given data range.

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
    bf_eia923_tbl = pt['boiler_fuel_eia923']
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
    bf_df['fuel_consumed_mmbtu'] = bf_df['fuel_consumed_units'] * \
        bf_df['fuel_mmbtu_per_unit']

    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'boiler_id', 'energy_source_code', 'fuel_type_code_pudl']
    if freq is not None:
        # In order to calculate the weighted average sulfur
        # content and ash content we need to calculate these totals.
        bf_df['total_sulfur_content'] = bf_df['fuel_consumed_units'] * \
            bf_df['sulfur_content_pct']
        bf_df['total_ash_content'] = bf_df['fuel_consumed_units'] * \
            bf_df['ash_content_pct']
        bf_df = bf_df.set_index(pd.DatetimeIndex(bf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        bf_gb = bf_df.groupby(by=by)

        # Sum up these totals within each group, and recalculate the per-unit
        # values (weighted in this case by fuel_consumed_units)
        bf_df = bf_gb.agg({
            'fuel_consumed_mmbtu': pudl.helpers.sum_na,
            'fuel_consumed_units': pudl.helpers.sum_na,
            'total_sulfur_content': pudl.helpers.sum_na,
            'total_ash_content': pudl.helpers.sum_na,
        })

        bf_df['fuel_mmbtu_per_unit'] = bf_df['fuel_consumed_mmbtu'] / \
            bf_df['fuel_consumed_units']
        bf_df['sulfur_content_pct'] = bf_df['total_sulfur_content'] / \
            bf_df['fuel_consumed_units']
        bf_df['ash_content_pct'] = bf_df['total_ash_content'] / \
            bf_df['fuel_consumed_units']
        bf_df = bf_df.reset_index()
        bf_df = bf_df.drop(['total_ash_content', 'total_sulfur_content'],
                           axis=1)

    # Grab some basic plant & utility information to add.
    pu_eia = pudl.output.eia860.plants_utils_eia860(
        pudl_engine,
        start_date=start_date,
        end_date=end_date
    )
    out_df = (
        pudl.helpers.clean_merge_asof(
            left=bf_df,
            right=pu_eia,
            by=["plant_id_eia"],
        )
        .dropna(subset=['plant_id_eia', 'utility_id_eia', 'boiler_id'])
    )
    # Merge in the unit_id_pudl assigned to each generator in the BGA process
    # Pull the BGA table and make it unit-boiler only:
    bga_boilers = (
        pudl.output.eia860.boiler_generator_assn_eia860(
            pudl_engine,
            start_date=start_date,
            end_date=end_date
        )
        .loc[:, ["report_date", "plant_id_eia", "boiler_id", "unit_id_pudl"]]
        .drop_duplicates()
    )
    out_df = pudl.helpers.clean_merge_asof(
        left=out_df,
        right=bga_boilers,
        by=["plant_id_eia", "boiler_id"],
    )
    out_df = pudl.helpers.organize_cols(
        out_df,
        cols=[
            'report_date',
            'plant_id_eia',
            'plant_id_pudl',
            'plant_name_eia',
            'utility_id_eia',
            'utility_id_pudl',
            'utility_name_eia',
            'boiler_id',
            'unit_id_pudl',
        ]
    ).pipe(apply_pudl_dtypes, group="eia")

    return out_df


def generation_eia923(
    pudl_engine,
    freq=None,
    start_date=None,
    end_date=None
):
    """
    Pull records from the boiler_fuel_eia923 table in a given data range.

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
    g_eia923_tbl = pt['generation_eia923']
    g_eia923_select = sa.sql.select(g_eia923_tbl)
    if start_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date >= start_date
        )
    if end_date is not None:
        g_eia923_select = g_eia923_select.where(
            g_eia923_tbl.c.report_date <= end_date
        )
    g_df = pd.read_sql(g_eia923_select, pudl_engine)

    # Index by date and aggregate net generation.
    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'generator_id']
    if freq is not None:
        g_df = g_df.set_index(pd.DatetimeIndex(g_df.report_date))
        by = by + [pd.Grouper(freq=freq)]
        g_gb = g_df.groupby(by=by)
        g_df = g_gb.agg(
            {'net_generation_mwh': pudl.helpers.sum_na}).reset_index()

    out_df = denorm_generation_eia923(g_df, pudl_engine, start_date, end_date)

    return out_df


def denorm_generation_eia923(g_df, pudl_engine, start_date, end_date):
    """
    Denomralize generation_eia923 table.

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
        pudl_engine,
        start_date=start_date,
        end_date=end_date
    )

    # Merge annual plant/utility data in with the more granular dataframe
    out_df = (
        pudl.helpers.clean_merge_asof(
            left=g_df,
            right=pu_eia,
            by=["plant_id_eia"],
        )
        .dropna(subset=['plant_id_eia', 'utility_id_eia', 'generator_id'])
    )
    # Merge in the unit_id_pudl assigned to each generator in the BGA process
    # Pull the BGA table and make it unit-generator only:
    bga_gens = (
        pudl.output.eia860.boiler_generator_assn_eia860(
            pudl_engine,
            start_date=start_date,
            end_date=end_date
        )
        .loc[:, ["report_date", "plant_id_eia", "generator_id", "unit_id_pudl"]]
        .drop_duplicates()
    )
    out_df = pudl.helpers.clean_merge_asof(
        left=out_df,
        right=bga_gens,
        by=["plant_id_eia", "generator_id"],
    )
    out_df = (
        out_df.pipe(pudl.helpers.organize_cols, cols=[
            'report_date',
            'plant_id_eia',
            'plant_id_pudl',
            'plant_name_eia',
            'utility_id_eia',
            'utility_id_pudl',
            'utility_name_eia',
            'generator_id',
        ])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return out_df


def make_url_cat_eiaapi(category_id):
    """
    Generate a url for a category from EIA's API.

    Requires an environment variable named ``API_KEY_EIA`` be set, containing
    a valid EIA API key, which you can obtain from:

    https://www.eia.gov/opendata/register.php

    """
    _check_eia_api_key()
    return (
        f"{BASE_URL_EIA}category/?api_key={os.environ.get('API_KEY_EIA')}"
        f"&category_id={category_id}"
    )


def make_url_series_eiaapi(series_id):
    """
    Generate a url for a series EIA's API.

    Requires an environment variable named ``API_KEY_EIA`` be set, containing
    a valid EIA API key, which you can obtain from:

    https://www.eia.gov/opendata/register.php

    """
    _check_eia_api_key()
    if series_id.count(';') > 100:
        raise AssertionError(
            f"""
            Too many series ids in this request: {series_id.count(';')}
            EIA allows up to 100 series in a request. Reduce the selection.
            """)
    return (
        f"{BASE_URL_EIA}series/?api_key={os.environ.get('API_KEY_EIA')}"
        f"&series_id={series_id}"
    )


def _check_eia_api_key():
    if "API_KEY_EIA" not in os.environ:
        raise RuntimeError("""
            The environment variable API_KEY_EIA is not set, and you are
            attempting to fill in missing fuel cost data using the EIA API.
            Please register for an EIA API key here, and store it in an
            environment variable.

            https://www.eia.gov/opendata/register.php

        """)


def get_response(url):
    """Get a response from the API's url."""
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(
            f"API response code may be invalid. Code: {response.status_code}")
    return response


def grab_fuel_state_monthly(cat_id):
    """
    Grab an API response for monthly fuel costs for one fuel category.

    The data we want from EIA is in monthly, state-level series for each fuel
    type. For each fuel category, there are at least 51 embeded child series.
    This function compiles one fuel type's child categories into one request.
    The resulting api response should contain a list of series responses from
    each state which we can convert into a pandas.DataFrame using
    convert_cost_json_to_df.

    Args:
        cat_id (int): category id for one fuel type. Known to be
    """
    _check_eia_api_key()
    # we are going to compile a string of series ids to put into one request
    series_all = ""
    fuel_level_cat = get_response(make_url_cat_eiaapi(cat_id))
    try:
        for child in fuel_level_cat.json()['category']['childseries']:
            # get only the monthly... the f in the childseries seems to refer
            # the recporting to frequency
            if child['f'] == 'M':
                logger.debug(f"    {child['series_id']}")
                series_all = series_all + ";" + str(child['series_id'])

    except KeyError:
        raise AssertionError(
            f"Error in Response: {fuel_level_cat.json()['data']['error']}\n"
            f"API_KEY_EIA={os.environ.get('API_KEY_EIA')}"
        )
    return get_response(make_url_series_eiaapi(series_all))


def convert_cost_json_to_df(response_fuel_state_annual):
    """
    Convert a fuel-type/state response into a clean dataframe.

    Args:
        response_fuel_state_annual (api response): an EIA API response which
            contains state-level series including monthly fuel cost data.

    Returns:
        pandas.DataFrame: a dataframe containing state-level montly fuel cost.
        The table contains the following columns, some of which are refernce
        columns: 'report_date', 'fuel_cost_per_unit', 'state',
        'fuel_type_code_pudl', 'units' (ref), 'series_id' (ref),
        'name' (ref).
    """
    cost_df = (
        pd.json_normalize(
            data=response_fuel_state_annual.json()['series'],
            record_path='data',
            meta=['geography', 'units', 'series_id', 'name', ])
        .rename(columns={0: 'report_date',
                         1: 'fuel_cost_per_unit',
                         'geography': 'state', })
        .assign(state=lambda x: x.state.str.partition('-', True)[2],
                # break up the long series_id to extract the fuel code
                fuel_type_code_pudl=lambda x:
                (x.series_id.str.partition('.', True)[2]
                 .str.partition('.', True)[2]
                 .str.partition('-', True)[0])
                )
        .replace({'fuel_type_code_pudl': FUEL_TYPE_EIAAPI_MAP})
    )
    cost_df.loc[:, 'report_date'] = pd.to_datetime(
        cost_df['report_date'], format='%Y%m')
    return cost_df


def get_fuel_cost_avg_eiaapi(fuel_cost_cat_ids):
    """
    Get a dataframe of state-level average fuel costs for EIA's API.

    Args:
        fuel_cost_cat_ids (list): list of category ids. Known/testing working
            ids are stored in FUEL_COST_CATEGORIES_EIAAPI.

    Returns:
        pandas.DataFrame: a dataframe containing state-level montly fuel cost.
        The table contains the following columns, some of which are refernce
        columns: 'report_date', 'fuel_cost_per_unit', 'state',
        'fuel_type_code_pudl', 'units' (ref), 'series_id' (ref),
        'name' (ref).
    """
    # grab_fuel_state_monthly compiles childseries for us to make larger
    # requests, but we can request up to 100 series from EIA but each
    # state-level fuel type is over 50 so we need to pull one fuel type at a
    # time and concat the resulting df
    dfs_to_concat = []
    for fuel_cat_id in fuel_cost_cat_ids:
        dfs_to_concat.append(convert_cost_json_to_df(
            grab_fuel_state_monthly(fuel_cat_id)))
    return pd.concat(dfs_to_concat)
