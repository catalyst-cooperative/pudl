"""Functions for pulling EIA 923 data out of the PUDl DB."""

import logging

import pandas as pd
import sqlalchemy as sa

import pudl

logger = logging.getLogger(__name__)


def generation_fuel_eia923(pudl_engine, freq=None,
                           start_date=None, end_date=None):
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
        Generation Fuel table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    gf_tbl = pt['generation_fuel_eia923']
    gf_select = sa.sql.select([gf_tbl, ])
    if start_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date >= start_date)
    if end_date is not None:
        gf_select = gf_select.where(
            gf_tbl.c.report_date <= end_date)

    gf_df = pd.read_sql(gf_select, pudl_engine)

    cols_to_drop = ['id']
    gf_df = gf_df.drop(cols_to_drop, axis=1)

    # fuel_type_code_pudl was formerly aer_fuel_category
    by = ['plant_id_eia', 'fuel_type_code_pudl']
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
    out_df = pudl.helpers.merge_on_date_year(
        gf_df, pu_eia, on=['plant_id_eia'])
    # Drop any records where we've failed to get the 860 data merged in...
    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name_eia',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
    ])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name_eia',
                  'utility_id_eia',
                  'utility_id_pudl',
                  'utility_name_eia', ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id_eia'] = out_df.plant_id_eia.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['utility_id_eia'] = out_df.utility_id_eia.astype(int)
    out_df['utility_id_pudl'] = out_df.utility_id_pudl.astype(int)

    return out_df


def fuel_receipts_costs_eia923(pudl_engine, freq=None,
                               start_date=None, end_date=None,
                               rolling=False):
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
    - ``fuel_qty_units`` (sum)
    - ``fuel_cost_per_mmbtu`` (weighted average)
    - ``total_fuel_cost`` (sum)
    - ``total_heat_content_mmbtu`` (sum)
    - ``heat_content_mmbtu_per_unit`` (weighted average)
    - ``sulfur_content_pct`` (weighted average)
    - ``ash_content_pct`` (weighted average)
    - ``moisture_content_pct`` (weighted average)
    - ``mercury_content_ppm`` (weighted average)
    - ``chlorine_content_ppm`` (weighted average)

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
        Fuel Receipts and Costs table.

    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    # Most of the fields we want come direclty from Fuel Receipts & Costs
    frc_tbl = pt['fuel_receipts_costs_eia923']
    frc_select = sa.sql.select([frc_tbl, ])

    # Need to re-integrate the MSHA coalmine info:
    cmi_tbl = pt['coalmine_eia923']
    cmi_select = sa.sql.select([cmi_tbl, ])
    cmi_df = pd.read_sql(cmi_select, pudl_engine)

    if start_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date >= start_date)
    if end_date is not None:
        frc_select = frc_select.where(
            frc_tbl.c.report_date <= end_date)

    frc_df = pd.read_sql(frc_select, pudl_engine)

    frc_df = pd.merge(frc_df, cmi_df,
                      how='left',
                      on='mine_id_pudl')

    cols_to_drop = ['id', 'mine_id_pudl']
    frc_df = frc_df.drop(cols_to_drop, axis=1)

    # this next step smoothes fuel_cost_per_mmbtu as a rolling monthly average.
    # for each month where there is any data make weighted averages of each
    # plant/fuel/month.
    if rolling is True:
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
    frc_df['total_heat_content_mmbtu'] = \
        frc_df['heat_content_mmbtu_per_unit'] * frc_df['fuel_qty_units']
    frc_df['total_fuel_cost'] = \
        frc_df['total_heat_content_mmbtu'] * frc_df['fuel_cost_per_mmbtu']

    if freq is not None:
        by = ['plant_id_eia', 'fuel_type_code_pudl', pd.Grouper(freq=freq)]
        # Create a date index for temporal resampling:
        frc_df = frc_df.set_index(pd.DatetimeIndex(frc_df.report_date))
        # Sum up these values so we can calculate quantity weighted averages
        frc_df['total_ash_content'] = \
            frc_df['ash_content_pct'] * frc_df['fuel_qty_units']
        frc_df['total_sulfur_content'] = \
            frc_df['sulfur_content_pct'] * frc_df['fuel_qty_units']
        frc_df['total_mercury_content'] = \
            frc_df['mercury_content_ppm'] * frc_df['fuel_qty_units']
        frc_df['total_moisture_content'] = \
            frc_df['moisture_content_pct'] * frc_df['fuel_qty_units']
        frc_df['total_chlorine_content'] = \
            frc_df['chlorine_content_ppm'] * frc_df['fuel_qty_units']

        frc_gb = frc_df.groupby(by=by)
        frc_df = frc_gb.agg({
            'fuel_qty_units': pudl.helpers.sum_na,
            'total_heat_content_mmbtu': pudl.helpers.sum_na,
            'total_fuel_cost': pudl.helpers.sum_na,
            'total_sulfur_content': pudl.helpers.sum_na,
            'total_ash_content': pudl.helpers.sum_na,
            'total_mercury_content': pudl.helpers.sum_na,
            'total_moisture_content': pudl.helpers.sum_na,
            'total_chlorine_content': pudl.helpers.sum_na,
        })
        frc_df['fuel_cost_per_mmbtu'] = \
            frc_df['total_fuel_cost'] / frc_df['total_heat_content_mmbtu']
        frc_df['heat_content_mmbtu_per_unit'] = \
            frc_df['total_heat_content_mmbtu'] / frc_df['fuel_qty_units']
        frc_df['sulfur_content_pct'] = \
            frc_df['total_sulfur_content'] / frc_df['fuel_qty_units']
        frc_df['ash_content_pct'] = \
            frc_df['total_ash_content'] / frc_df['fuel_qty_units']
        frc_df['mercury_content_ppm'] = \
            frc_df['total_mercury_content'] / frc_df['fuel_qty_units']
        frc_df['chlorine_content_ppm'] = \
            frc_df['total_chlorine_content'] / frc_df['fuel_qty_units']
        frc_df['moisture_content_pct'] = \
            frc_df['total_moisture_content'] / frc_df['fuel_qty_units']
        frc_df = frc_df.reset_index()
        frc_df = frc_df.drop(['total_ash_content',
                              'total_sulfur_content',
                              'total_moisture_content',
                              'total_chlorine_content',
                              'total_mercury_content'], axis=1)

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)
    out_df = pudl.helpers.merge_on_date_year(
        frc_df, pu_eia, on=['plant_id_eia'])

    # Drop any records where we've failed to get the 860 data merged in...
    out_df = out_df.dropna(subset=['utility_id_eia', 'utility_name_eia'])

    if freq is None:
        # There are a couple of invalid records with no specified fuel.
        out_df = out_df.dropna(subset=['fuel_group_code'])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name_eia',
                  'utility_id_eia',
                  'utility_id_pudl',
                  'utility_name_eia', ]

    # Re-arrange the columns for easier readability:
    out_df = pudl.helpers.organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id_eia'] = out_df.plant_id_eia.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['utility_id_eia'] = out_df.utility_id_eia.astype(int)
    out_df['utility_id_pudl'] = out_df.utility_id_pudl.astype(int)

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
    * ``total_heat_content_mmbtu`` (sum)
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
    bf_eia923_select = sa.sql.select([bf_eia923_tbl, ])
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
    bf_df['total_heat_content_mmbtu'] = bf_df['fuel_consumed_units'] * \
        bf_df['fuel_mmbtu_per_unit']

    # Create a date index for grouping based on freq
    by = ['plant_id_eia', 'boiler_id', 'fuel_type_code_pudl']
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
            'total_heat_content_mmbtu': pudl.helpers.sum_na,
            'fuel_consumed_units': pudl.helpers.sum_na,
            'total_sulfur_content': pudl.helpers.sum_na,
            'total_ash_content': pudl.helpers.sum_na,
        })

        bf_df['fuel_mmbtu_per_unit'] = \
            bf_df['total_heat_content_mmbtu'] / bf_df['fuel_consumed_units']
        bf_df['sulfur_content_pct'] = \
            bf_df['total_sulfur_content'] / bf_df['fuel_consumed_units']
        bf_df['ash_content_pct'] = \
            bf_df['total_ash_content'] / bf_df['fuel_consumed_units']
        bf_df = bf_df.reset_index()
        bf_df = bf_df.drop(['total_ash_content', 'total_sulfur_content'],
                           axis=1)

    # Grab some basic plant & utility information to add.
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)
    out_df = pudl.helpers.merge_on_date_year(
        bf_df, pu_eia, on=['plant_id_eia'])
    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'utility_id_eia',
        'utility_id_pudl',
        'boiler_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name_eia',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
        'boiler_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = pudl.helpers.organize_cols(out_df, first_cols)

    out_df['utility_id_eia'] = out_df.utility_id_eia.astype(int)
    out_df['utility_id_pudl'] = out_df.utility_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return out_df


def generation_eia923(pudl_engine, freq=None,
                      start_date=None, end_date=None):
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
    g_eia923_select = sa.sql.select([g_eia923_tbl, ])
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

    # Grab EIA 860 plant and utility specific information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(pudl_engine,
                                                    start_date=start_date,
                                                    end_date=end_date)

    # Merge annual plant/utility data in with the more granular dataframe
    out_df = pudl.helpers.merge_on_date_year(g_df, pu_eia, on=['plant_id_eia'])

    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    # These ID fields are vital -- without them we don't have a complete record
    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'utility_id_eia',
        'utility_id_pudl',
        'generator_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name_eia',
        'utility_id_eia',
        'utility_id_pudl',
        'utility_name_eia',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = pudl.helpers.organize_cols(out_df, first_cols)

    out_df['utility_id_eia'] = out_df.utility_id_eia.astype(int)
    out_df['utility_id_pudl'] = out_df.utility_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return out_df
