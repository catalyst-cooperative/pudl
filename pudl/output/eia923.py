"""Functions for pulling EIA 923 data out of the PUDl DB."""

import sqlalchemy as sa
import pandas as pd

from pudl import init, helpers

import pudl.models.entities
pt = pudl.models.entities.PUDLBase.metadata.tables


def generation_fuel_eia923(freq=None, testing=False,
                           start_date=None, end_date=None):
    """
    Pull records from the generation_fuel_eia923 table, in a given date range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.
     - plant_id_eia
     - report_date
     - fuel_type_pudl
     - fuel_consumed_total
     - fuel_consumed_for_electricity
     - fuel_mmbtu_per_unit
     - fuel_consumed_total_mmbtu
     - fuel_consumed_for_electricity_mmbtu
     - net_generation_mwh

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
    -----
        testing (bool): True if we are connecting to the pudl_test DB, False
            if we're using the live DB.  False by default.
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date & end_date: date-like objects, including strings of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.

    Returns:
    --------
        gf_df: a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
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

    # fuel_type_pudl was formerly aer_fuel_category
    by = ['plant_id_eia', 'fuel_type_pudl']
    if freq is not None:
        # Create a date index for temporal resampling:
        gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df.report_date))
        by = by + [pd.Grouper(freq=freq)]

        # Sum up these values so we can calculate quantity weighted averages
        gf_gb = gf_df.groupby(by=by)
        gf_df = gf_gb.agg({
            'fuel_consumed_total': helpers.sum_na,
            'fuel_consumed_for_electricity': helpers.sum_na,
            'fuel_consumed_total_mmbtu': helpers.sum_na,
            'fuel_consumed_for_electricity_mmbtu': helpers.sum_na,
            'net_generation_mwh': helpers.sum_na,
        })
        gf_df['fuel_mmbtu_per_unit'] = \
            gf_df['fuel_consumed_total_mmbtu'] / gf_df['fuel_consumed_total']

        gf_df = gf_df.reset_index()

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(start_date=start_date,
                                                    end_date=end_date,
                                                    testing=testing)
    out_df = helpers.merge_on_date_year(gf_df, pu_eia, on=['plant_id_eia'])
    # Drop any records where we've failed to get the 860 data merged in...
    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
    ])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name', ]

    out_df = helpers.organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id_eia'] = out_df.plant_id_eia.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return out_df


def fuel_receipts_costs_eia923(freq=None, testing=False,
                               start_date=None, end_date=None):
    """
    Pull records from fuel_receipts_costs_eia923 table, in a given date range.

    Optionally, aggregate the records at a monthly or longer timescale, as well
    as by fuel type within a plant, by setting freq to something other than
    the default None value.

    If the records are not being aggregated, then all of the fields found in
    the PUDL database are available.  If they are being aggregated, then the
    following fields are preserved, and appropriately summed or re-calculated
    based on the specified aggregation. In both cases, new total values are
    calculated, for total fuel heat content and total fuel cost.
     - plant_id_eia
     - report_date
     - fuel_type_pudl (formerly energy_source_simple)
     - fuel_quantity (sum)
     - fuel_cost_per_mmbtu (weighted average)
     - total_fuel_cost (sum)
     - total_heat_content_mmbtu (sum)
     - heat_content_mmbtu_per_unit (weighted average)
     - sulfur_content_pct (weighted average)
     - ash_content_pct (weighted average)
     - mercury_content_ppm (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
    -----
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date & end_date: date-like objects, including strings of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB. False by default.

    Returns:
    --------
        frc_df: a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
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
                      left_on='mine_id_pudl',
                      right_on='id')

    cols_to_drop = ['fuel_receipt_id', 'mine_id_pudl', 'id']
    frc_df = frc_df.drop(cols_to_drop, axis=1)

    # Calculate a few totals that are commonly needed:
    frc_df['total_heat_content_mmbtu'] = \
        frc_df['heat_content_mmbtu_per_unit'] * frc_df['fuel_quantity']
    frc_df['total_fuel_cost'] = \
        frc_df['total_heat_content_mmbtu'] * frc_df['fuel_cost_per_mmbtu']

    if freq is not None:
        by = ['plant_id_eia', 'fuel_type_pudl', pd.Grouper(freq=freq)]
        # Create a date index for temporal resampling:
        frc_df = frc_df.set_index(pd.DatetimeIndex(frc_df.report_date))
        # Sum up these values so we can calculate quantity weighted averages
        frc_df['total_ash_content'] = \
            frc_df['ash_content_pct'] * frc_df['fuel_quantity']
        frc_df['total_sulfur_content'] = \
            frc_df['sulfur_content_pct'] * frc_df['fuel_quantity']
        frc_df['total_mercury_content'] = \
            frc_df['mercury_content_ppm'] * frc_df['fuel_quantity']

        frc_gb = frc_df.groupby(by=by)
        frc_df = frc_gb.agg({
            'fuel_quantity': helpers.sum_na,
            'total_heat_content_mmbtu': helpers.sum_na,
            'total_fuel_cost': helpers.sum_na,
            'total_sulfur_content': helpers.sum_na,
            'total_ash_content': helpers.sum_na,
            'total_mercury_content': helpers.sum_na,
        })
        frc_df['fuel_cost_per_mmbtu'] = \
            frc_df['total_fuel_cost'] / frc_df['total_heat_content_mmbtu']
        frc_df['heat_content_mmbtu_per_unit'] = \
            frc_df['total_heat_content_mmbtu'] / frc_df['fuel_quantity']
        frc_df['sulfur_content_pct'] = \
            frc_df['total_sulfur_content'] / frc_df['fuel_quantity']
        frc_df['ash_content_pct'] = \
            frc_df['total_ash_content'] / frc_df['fuel_quantity']
        frc_df['mercury_content_ppm'] = \
            frc_df['total_mercury_content'] / frc_df['fuel_quantity']
        frc_df = frc_df.reset_index()
        frc_df = frc_df.drop(['total_ash_content',
                              'total_sulfur_content',
                              'total_mercury_content'], axis=1)

    # Bring in some generic plant & utility information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(start_date=start_date,
                                                    end_date=end_date,
                                                    testing=testing)
    out_df = helpers.merge_on_date_year(frc_df, pu_eia, on=['plant_id_eia'])

    # Drop any records where we've failed to get the 860 data merged in...
    out_df = out_df.dropna(subset=['operator_id', 'operator_name'])

    if freq is None:
        # There are a couple of invalid records with no specified fuel.
        out_df = out_df.dropna(subset=['fuel_group'])

    first_cols = ['report_date',
                  'plant_id_eia',
                  'plant_id_pudl',
                  'plant_name',
                  'operator_id',
                  'util_id_pudl',
                  'operator_name', ]

    # Re-arrange the columns for easier readability:
    out_df = helpers.organize_cols(out_df, first_cols)

    # Clean up the types of a few columns...
    out_df['plant_id_eia'] = out_df.plant_id_eia.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)
    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)

    return out_df


def boiler_fuel_eia923(freq=None, testing=False,
                       start_date=None, end_date=None):
    """
    Pull records from the boiler_fuel_eia923 table, in a given data range.

    Optionally, aggregate the records over some timescale -- monthly, yearly,
    quarterly, etc. as well as by fuel type within a plant.

    If the records are not being aggregated, all of the database fields are
    available. If they're being aggregated, then we preserve the following
    fields. Per-unit values are re-calculated based on the aggregated totals.
    Totals are summed across whatever time range is being used, within a
    given plant and fuel type.
     - fuel_consumed_units (sum)
     - fuel_mmbtu_per_unit (weighted average)
     - total_heat_content_mmbtu (sum)
     - sulfur_content_pct (weighted average)
     - ash_content_pct (weighted average)

    In addition, plant and utility names and IDs are pulled in from the EIA
    860 tables.

    Args:
    -----
        freq (str): a pandas timeseries offset alias. The original data is
            reported monthly, so the best time frequencies to use here are
            probably month start (freq='MS') and year start (freq='YS').
        start_date & end_date: date-like objects, including strings of the
            form 'YYYY-MM-DD' which will be used to specify the date range of
            records to be pulled.  Dates are inclusive.
        testing (bool): True if we're using the pudl_test DB, False if we're
            using the live PUDL DB.  False by default.

    Returns:
    --------
        bf_df: a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
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
            'total_heat_content_mmbtu': helpers.sum_na,
            'fuel_consumed_units': helpers.sum_na,
            'total_sulfur_content': helpers.sum_na,
            'total_ash_content': helpers.sum_na,
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
    pu_eia = pudl.output.eia860.plants_utils_eia860(start_date=start_date,
                                                    end_date=end_date,
                                                    testing=False)
    out_df = helpers.merge_on_date_year(bf_df, pu_eia, on=['plant_id_eia'])
    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'boiler_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'boiler_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = helpers.organize_cols(out_df, first_cols)

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return out_df


def generation_eia923(freq=None, testing=False,
                      start_date=None, end_date=None):
    """
    Sum net generation by generator at the specified frequency.

    In addition, some human readable plant and utility names, as well as some
    ID values for joining with other dataframes is added back in to the
    dataframe before it is returned.

    Args:
    -----
        pudl_engine: An SQLAlchemy DB connection engine.
        freq: A string used to specify a time grouping frequency.
        testing (bool): True if we're using the pudl_test DB, False if we're
                        using the live PUDL DB.  False by default.

    Returns:
    --------
        out_df: a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
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
        g_df = g_gb.agg({'net_generation_mwh': helpers.sum_na}).reset_index()

    # Grab EIA 860 plant and utility specific information:
    pu_eia = pudl.output.eia860.plants_utils_eia860(start_date=start_date,
                                                    end_date=end_date,
                                                    testing=testing)

    # Merge annual plant/utility data in with the more granular dataframe
    out_df = helpers.merge_on_date_year(g_df, pu_eia, on=['plant_id_eia'])

    if freq is None:
        out_df = out_df.drop(['id'], axis=1)

    # These ID fields are vital -- without them we don't have a complete record
    out_df = out_df.dropna(subset=[
        'plant_id_eia',
        'plant_id_pudl',
        'operator_id',
        'util_id_pudl',
        'generator_id',
    ])

    first_cols = [
        'report_date',
        'plant_id_eia',
        'plant_id_pudl',
        'plant_name',
        'operator_id',
        'util_id_pudl',
        'operator_name',
        'generator_id',
    ]

    # Re-arrange the columns for easier readability:
    out_df = helpers.organize_cols(out_df, first_cols)

    out_df['operator_id'] = out_df.operator_id.astype(int)
    out_df['util_id_pudl'] = out_df.util_id_pudl.astype(int)
    out_df['plant_id_pudl'] = out_df.plant_id_pudl.astype(int)

    return out_df
