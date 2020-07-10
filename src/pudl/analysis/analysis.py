"""A module with functions to aid in data analysis using the PUDL database.

NOTE: Currently the functions here are for reference only. They are not
currently being tested and maintained, but are being retained because we are
working on similar things right now, and may update or re-purpose them.

"""
import logging

# Useful high-level external modules.
import numpy as np
import pandas as pd
import sqlalchemy as sa

# Our own code...
import pudl
import pudl.helpers

logger = logging.getLogger(__name__)


def fuel_ferc1_by_pudl(pudl_plant_ids, pudl_engine,
                       fuels=('gas', 'oil', 'coal'),
                       cols=('fuel_consumed_total_mmbtu',
                             'fuel_consumed_total_cost_mmbtu',
                             'fuel_consumed_total_cost_unit')):
    """
    Aggregate FERC Form 1 fuel data by PUDL plant id and, optionally, fuel.

    Args:
        pudl_plant_ids (list-like): which PUDL plants should we retain for
            aggregation?
        fuels (list-like): Should the columns listed in cols be broken out by
            each individual fuel? If so, which fuels do we want totals for? If
            you want all fuels lumped together, pass in 'all'.
        cols (list-like): which columns from the fuel_ferc1 table should be
            summed.

    Returns:
        pandas.DataFrame: with pudl_plant_id, year, and the summed values
        specified in cols. If fuels is not 'all' then it also has a column
        specifying fuel type.

    """
    fuel_df = pudl.output.ferc1.fuel_ferc1_df(pudl_engine)

    # Calculate the total fuel heat content for the plant by fuel
    fuel_df = fuel_df[fuel_df.plant_id_pudl.isin(pudl_plant_ids)]

    if fuels == 'all':
        cols_to_gb = ['plant_id_pudl', 'report_year']
    else:
        # Limit to records that pertain to our fuels of interest.
        fuel_df = fuel_df[fuel_df['fuel'].isin(fuels)]
        # Group by fuel as well, so we get individual fuel totals.
        cols_to_gb = ['plant_id_pudl', 'report_year', 'fuel']

    fuel_df = fuel_df.groupby(cols_to_gb)[cols].sum()
    fuel_df = fuel_df.reset_index()

    return fuel_df


def steam_ferc1_by_pudl(pudl_plant_ids, pudl_engine,
                        cols=('net_generation_mwh', )):
    """
    Aggregate and return data from the steam_ferc1 table by pudl_plant_id.

    Args:
        pudl_plant_ids (list-like): A list of ids to include in the output.
        cols (list-like): The data columns that you want to aggregate and
        return.

    Returns:
        pandas.DataFrameA dataframe with columns for report_year, pudl_plant_id
        and cols, with the values in cols aggregated by plant and year.

    """
    steam_df = pudl.output.ferc1.plants_steam_ferc1_df(pudl_engine)
    steam_df = steam_df[steam_df.plant_id_pudl.isin(pudl_plant_ids)]
    steam_df = steam_df.groupby(['plant_id_pudl', 'report_year'])[cols].sum()
    steam_df = steam_df.reset_index()

    return steam_df


def frc_by_pudl(pudl_plant_ids, pudl_engine,
                fuels=('gas', 'oil', 'coal'),
                cols=('total_fuel_cost', )):
    """
    Aggregate fuel_receipts_costs_eia923 table for comparison with FERC Form 1.

    In order to correlate information between EIA 923 and FERC Form 1, we need
    to aggregate the EIA data annually, and potentially by fuel. This function
    groups fuel_receipts_costs_eia923 by pudl_plant_id, fuel, and year, and
    sums the columns of interest specified in cols, and returns a dataframe
    with the totals by pudl_plant_id, fuel, and year.

    Args:
        pudl_plant_ids: list of plant IDs to keep.
        fuels: list of fuel strings that we want to group by. Alternatively,
            this can be set to 'all' in which case fuel is not grouped by.
        cols: List of data columns which we are summing.

    Returns:
        pandas.DataFrame: A dataframe with the sums of cols, as grouped by pudl
        ID, year, and (optionally) fuel.

    """
    md = sa.MetaData(bind=pudl_engine)
    md.reflect()
    # Get all the EIA info from generation_fuel_eia923
    frc_df = pudl.output.eia923.fuel_receipts_costs_eia923(pudl_engine,
                                                           md.tables)
    # Limit just to the plants we're looking at
    frc_df = frc_df[frc_df.plant_id_pudl.isin(pudl_plant_ids)]
    # Just keep the columns we need for output:
    cols_to_keep = ['plant_id_pudl', 'report_date']
    cols_to_keep = cols_to_keep + cols
    cols_to_gb = [pd.Grouper(freq='A'), 'plant_id_pudl']

    if fuels != 'all':
        frc_df = frc_df[frc_df.fuel.isin(fuels)]
        cols_to_keep = cols_to_keep + ['fuel', ]
        cols_to_gb = cols_to_gb + ['fuel', ]

    # Pare down the dataframe to make it easier to play with:
    frc_df = frc_df[cols_to_keep]

    # Prepare to group annually
    frc_df['report_date'] = pd.to_datetime(frc_df['report_date'])
    frc_df.index = frc_df.report_date
    frc_df.drop('report_date', axis=1, inplace=True)

    # Group and sum of the columns of interest:
    frc_gb = frc_df.groupby(by=cols_to_gb)
    frc_totals_df = frc_gb[cols].sum()

    # Simplify and clean the DF for return:
    frc_totals_df = frc_totals_df.reset_index()
    frc_totals_df['report_year'] = frc_totals_df.report_date.dt.year
    frc_totals_df = frc_totals_df.drop('report_date', axis=1)
    frc_totals_df = frc_totals_df.dropna()

    return frc_totals_df


def gen_fuel_by_pudl(pudl_plant_ids, pudl_engine,
                     fuels=('gas', 'oil', 'coal'),
                     cols=('fuel_consumed_mmbtu',
                           'net_generation_mwh')):
    """
    Aggregate generation_fuel_eia923 table for comparison with FERC Form 1.

    In order to correlate informataion between EIA 923 and FERC Form 1, we need
    to aggregate the EIA data annually, and potentially by fuel. This function
    groups generation_fuel_eia923 by pudl_plant_id, fuel, and year, and sums
    the columns of interest specified in cols, and returns a dataframe with
    the totals by pudl_plant_id, fuel, and year.

    Args:
        pudl_plant_ids (list-like): list of plant IDs to keep.
        fuels (list-like): list of fuel strings that we want to group by.
            Alternatively, this can be set to 'all' in which case fuel is not
            grouped by.
        cols (list-like): List of data columns which we are summing.

    Returns:
        pandas.DataFrame: A dataframe with the sums of cols, as grouped by pudl
        ID, year, and (optionally) fuel.

    """
    md = sa.MetaData(bind=pudl_engine)
    md.reflect()
    # Get all the EIA info from generation_fuel_eia923
    gf_df = pudl.output.eia923.generation_fuel_eia923(pudl_engine, md.tables)

    # Standardize the fuel codes (need to fix this in the DB!!!!)
    gf_df = gf_df.rename(columns={'fuel_type_code_pudl': 'fuel'})
    # gf_df['fuel'] = gf_df.fuel.replace(to_replace='petroleum', value='oil')

    # Select only the records that pertain to our target IDs
    gf_df = gf_df[gf_df.plant_id_pudl.isin(pudl_plant_ids)]

    cols_to_keep = ['plant_id_pudl', 'report_date']
    cols_to_keep = cols_to_keep + cols
    cols_to_gb = [pd.Grouper(freq='A'), 'plant_id_pudl']

    if fuels != 'all':
        gf_df = gf_df[gf_df.fuel.isin(fuels)]
        cols_to_keep = cols_to_keep + ['fuel', ]
        cols_to_gb = cols_to_gb + ['fuel', ]

    # Pare down the dataframe to make it easier to play with:
    gf_df = gf_df[cols_to_keep]

    # Prepare to group annually
    gf_df['report_date'] = pd.to_datetime(gf_df['report_date'])
    gf_df.index = gf_df.report_date
    gf_df.drop('report_date', axis=1, inplace=True)

    gf_gb = gf_df.groupby(by=cols_to_gb)
    gf_totals_df = gf_gb[cols].sum()
    gf_totals_df = gf_totals_df.reset_index()

    # Simplify date info for easy comparison with FERC.
    gf_totals_df['report_year'] = gf_totals_df.report_date.dt.year
    gf_totals_df = gf_totals_df.drop('report_date', axis=1)
    gf_totals_df = gf_totals_df.dropna()

    return gf_totals_df


def generator_proportion_eia923(g, id_col='plant_id_eia'):
    """
    Generate a dataframe with the proportion of generation for each generator.

    Args:
        g (pandas.DataFrame): a dataframe from either all of generation_eia923
            or some subset of records from generation_eia923. The dataframe
            needs the following columns to be present: plant_id_eia,
            generator_id, report_date, net_generation_mwh

    Returns:
        pandas.DataFrame: containing report_year, plant_id_eia, generator_id,
        proportion_of_generation

    """
    # Set the datetimeindex
    g = g.set_index(pd.DatetimeIndex(g['report_year']))
    # groupby plant_id_eia and by year
    g_yr = g.groupby([pd.Grouper(freq='A'), id_col, 'generator_id'])
    # sum net_gen by year by plant
    g_net_generation_per_generator = pd.DataFrame(
        g_yr.net_generation_mwh.sum())
    g_net_generation_per_generator = \
        g_net_generation_per_generator.reset_index(level=['generator_id'])

    # groupby plant_id_eia and by year
    g_net_generation_per_plant = g.groupby(
        [pd.Grouper(freq='A'), id_col])
    # sum net_gen by year by plant and convert to datafram
    g_net_generation_per_plant = pd.DataFrame(
        g_net_generation_per_plant.net_generation_mwh.sum())

    # Merge the summed net generation by generator with the summed net
    # generation by plant
    g_gens_proportion = g_net_generation_per_generator.merge(
        g_net_generation_per_plant, how="left", left_index=True,
        right_index=True)
    g_gens_proportion['proportion_of_generation'] = (
        g_gens_proportion.net_generation_mwh_x /
        g_gens_proportion.net_generation_mwh_y)
    # Remove the net generation columns
    g_gens_proportion = g_gens_proportion.drop(
        ['net_generation_mwh_x', 'net_generation_mwh_y'], axis=1)
    g_gens_proportion.reset_index(inplace=True)

    return g_gens_proportion


def capacity_proportion_eia923(g, id_col='plant_id_eia',
                               capacity='capacity_mw'):
    """
    Generate dataframe with proportion of plant capacity for each generator.

    Args:
        g (pandas.DataFrame): a dataframe from either all of generation_eia923
            or some subset of records from generation_eia923. The dataframe
            needs the following columns to be present: generator_id,
            report_date, capacity_mw
        id_col (str): either plant_id_eia (default) or plant_id_pudl
        capacity (str): capacity_mw (default), summer_capacity_mw, or
            winter_capacity_mw

    Returns:
        pandas.DataFrame: containing report_year, plant_id_eia, generator_id,
        proportion_of_capacity

    """
    # groupby plant_id_eia and by year
    g_net_capacity_per_plant = g.groupby(['report_year', id_col])
    # sum net_gen by year by plant and convert to datafram
    g_net_capacity_per_plant = pd.DataFrame(
        g_net_capacity_per_plant.capacity_mw.sum())
    g_net_capacity_per_plant.reset_index(inplace=True)

    # Merge the summed net generation by generator with the summed net
    # generation by plant
    g_capacity_proportion = g.merge(
        g_net_capacity_per_plant, on=[id_col, 'report_year'], how="left")
    g_capacity_proportion['proportion_of_plant_capacity'] = (
        g_capacity_proportion.capacity_mw_x /
        g_capacity_proportion.capacity_mw_y)
    # Remove the net generation columns
    g_capacity_proportion = g_capacity_proportion.rename(
        columns={'capacity_mw_x': 'capacity_gen_mw',
                 'capacity_mw_y': 'capacity_plant_mw'})

    return g_capacity_proportion


def values_by_generator_eia923(table_eia923, column_name, g):
    """
    Generate a dataframe with a plant value proportioned out by generator.

    Args:
        table_eia923 (pandas.DataFrame: an EIA923 table (this has been tested
            with fuel_receipts_costs_eia923 and generation_fuel_eia923).
        column_name: a column name from the table_eia923.
        g (pandas.DataFrame): a dataframe from either all of generation_eia923
            or some subset of records from generation_eia923. The dataframe
            needs the following columns to be present: plant_id_eia,
            generator_id, report_date, and net_generation_mwh.

    Returns:
        pandas.DataFrame: with report_date, plant_id_eia, generator_id, and the
        proportioned value from the column_name.

    """
    # Set the datetimeindex
    table_eia923 = table_eia923.set_index(
        pd.DatetimeIndex(table_eia923['report_date']))
    # groupby plant_id_eia and by year
    table_eia923_gb = table_eia923.groupby(
        [pd.Grouper(freq='A'), 'plant_id_eia'])
    # sum fuel cost by year by plant
    table_eia923_sr = table_eia923_gb[column_name].sum()
    # Convert back into a dataframe
    table_eia923_df = pd.DataFrame(table_eia923_sr)
    column_name_by_plant = "{}_plant".format(column_name)
    table_eia923_df = table_eia923_df.rename(
        columns={column_name: column_name_by_plant})
    # get the generator proportions
    g_gens_proportion = generator_proportion_eia923(g)
    # merge the per generator proportions with the summed fuel cost
    g_generator = g_gens_proportion.merge(
        table_eia923_df, how="left", right_index=True, left_index=True)
    # calculate the proportional fuel costs
    g_generator["{}_generator".format(column_name)] = (
        g_generator[column_name_by_plant] *
        g_generator.proportion_of_generation)
    # drop the unneccessary columns
    g_generator = g_generator.drop(
        ['proportion_of_generation', column_name_by_plant], axis=1)
    return g_generator


def primary_fuel_ferc1(fuel_df, fuel_thresh=0.5):
    """
    Determine the primary fuel for plants listed in the PUDL fuel_ferc1 table.

    Given a selection of records from the PUDL fuel_ferc1 table, determine
    the primary fuel type for each plant (as identified by a unique
    combination of report_year, respondent_id, and plant_name).

    Args:
        fuel_df (DataFrame): a DataFrame selected from the PUDL fuel_ferc1
            table, with columns including report_year, respondent_id,
            plant_name, fuel, fuel_qty_burned, and fuel_avg_mmbtu_per_unit.
        fuel_thresh (float): What is the minimum proportion of a plant's
            annual fuel consumption in terms of heat content, that a fuel
            must account for, in order for that fuel to be considered the
            primary fuel.

    Returns:
        pandas.DataFrame: a containing report_year, respondent_id, plant_name,
        and primary_fuel.

    """
    plants_by_heat = plant_fuel_proportions_ferc1(fuel_df)

    # On a per plant, per year basis, identify the fuel that made the largest
    # contribution to the plant's overall heat content consumed. If that
    # proportion is greater than fuel_thresh, set the primary_fuel to be
    # that fuel.  Otherwise, leave it None.
    plants_by_heat = plants_by_heat.set_index(['report_year',
                                               'respondent_id',
                                               'plant_name'])
    plants_by_heat = plants_by_heat.drop('total_mmbtu', axis=1)
    mask = plants_by_heat >= fuel_thresh
    plants_by_heat = plants_by_heat.where(mask)
    plants_by_heat['primary_fuel'] = plants_by_heat.idxmax(axis=1)
    return plants_by_heat[['primary_fuel', ]].reset_index()


def plant_fuel_proportions_ferc1(fuel_df):
    """
    Calculate annual fuel proportions by plant based on FERC data.

    Args:
        fuel_df (pandas.DataFrame): FERC 1 Fuel table, or some subset of it.

    Returns:
        pandas.DataFrame

    """
    fuel_df = fuel_df.copy()

    fuel_df['total_mmbtu'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_avg_mmbtu_per_unit']

    heat_df = fuel_df[['report_year',
                       'respondent_id',
                       'plant_name',
                       'fuel',
                       'total_mmbtu']]

    heat_pivot = heat_df.pivot_table(
        index=['report_year', 'respondent_id', 'plant_name'],
        columns='fuel',
        values='total_mmbtu')

    heat_pivot['total'] = heat_pivot.sum(axis=1, numeric_only=True)
    mmbtu_total = heat_pivot.copy()
    mmbtu_total = pd.DataFrame(mmbtu_total['total'])

    heat_pivot = heat_pivot.fillna(value=0)
    heat_pivot = heat_pivot.divide(heat_pivot.total, axis='index')
    heat_pivot = heat_pivot.drop('total', axis=1)
    heat_pivot = heat_pivot.reset_index()

    heat_pivot = heat_pivot.merge(mmbtu_total.reset_index())
    heat_pivot.rename(columns={'total': 'total_mmbtu'},
                      inplace=True)
    del heat_pivot.columns.name

    return heat_pivot


def plant_fuel_proportions_frc_eia923(frc_df, id_col='plant_id_eia'):
    """Calculate annual fuel proportions by plant from EIA923 fuel receipts."""
    frc_df = frc_df.copy()

    # Add a column with total fuel heat content per delivery
    frc_df['total_mmbtu'] = frc_df.fuel_qty_units * frc_df.average_heat_content

    # Drop everything but report_date, plant_id_eia, fuel_group_code,
    # total_mmbtu
    frc_df = frc_df[['report_date', 'plant_id_eia',
                     'plant_id_pudl', 'fuel_group_code', 'total_mmbtu']]

    # Group by report_date(annual), plant_id_eia, fuel_group_code
    frc_gb = frc_df.groupby(
        [id_col, pd.Grouper(freq='A'), 'fuel_group_code'])

    # Add up all the MMBTU for each plant & year. At this point each record
    # in the dataframe contains only information about a single fuel.
    heat_df = frc_gb.agg(np.sum)

    # Simplfy the DF a little before we turn it into a pivot table.
    heat_df = heat_df.reset_index()
    heat_df['year'] = pd.DatetimeIndex(heat_df['report_date']).year
    heat_df = heat_df.drop('report_date', axis=1)

    # Take the individual rows organized by fuel_group_code, and turn them into
    # columns, each with the total MMBTU for that fuel, year, and plant.
    heat_pivot = heat_df.pivot_table(
        index=['year', id_col],
        columns='fuel_group_code',
        values='total_mmbtu')

    # Add a column that has the *total* heat content of all fuels:
    heat_pivot['total'] = heat_pivot.sum(axis=1, numeric_only=True)

    # Replace any NaN values we got from pivoting with zeros.
    heat_pivot = heat_pivot.fillna(value=0)

    # Divide all columns by the total heat content, giving us the proportions
    # for each fuel instead of the heat content.
    heat_pivot = heat_pivot.divide(heat_pivot.total, axis='index')

    # Drop the total column (it's nothing but 1.0 values) and clean up the
    # index and columns a bit before returning the DF.
    heat_pivot = heat_pivot.drop('total', axis=1)
    heat_pivot = heat_pivot.reset_index()
    del heat_pivot.columns.name

    return heat_pivot


def primary_fuel_frc_eia923(frc_df, id_col='plant_id_eia', fuel_thresh=0.5):
    """Determine a plant's primary fuel from EIA923 fuel receipts table."""
    frc_df = frc_df.copy()

    # Figure out the heat content proportions of each fuel received:
    frc_by_heat = plant_fuel_proportions_frc_eia923(frc_df)

    # On a per plant, per year basis, identify the fuel that made the largest
    # contribution to the plant's overall heat content consumed. If that
    # proportion is greater than fuel_thresh, set the primary_fuel to be
    # that fuel.  Otherwise, leave it None.
    frc_by_heat = frc_by_heat.set_index([id_col, 'year'])
    mask = frc_by_heat >= fuel_thresh
    frc_by_heat = frc_by_heat.where(mask)
    frc_by_heat['primary_fuel'] = frc_by_heat.idxmax(axis=1)
    return frc_by_heat[['primary_fuel', ]].reset_index()


def plant_fuel_proportions_gf_eia923(gf_df):
    """Calculate annual fuel proportions by plant from EIA923 gen fuel."""
    gf_df = gf_df.copy()

    # Drop everything but report_date, plant_id_eia, fuel_type_code_pudl,
    # total_mmbtu
    gf_df = gf_df[['report_date',
                   'plant_id_eia',
                   'fuel_type_code_pudl',
                   'fuel_consumed_mmbtu']]

    # Set report_date as a DatetimeIndex
    gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df['report_date']))

    # Group by report_date(annual), plant_id_eia, fuel_type_code_pudl
    gf_gb = gf_df.groupby(
        ['plant_id_eia', pd.Grouper(freq='A'), 'fuel_type_code_pudl'])

    # Add up all the MMBTU for each plant & year. At this point each record
    # in the dataframe contains only information about a single fuel.
    heat_df = gf_gb.agg(np.sum)

    # Simplfy the DF a little before we turn it into a pivot table.
    heat_df = heat_df.reset_index()
    heat_df['year'] = pd.DatetimeIndex(heat_df['report_date']).year
    heat_df = heat_df.drop('report_date', axis=1)

    # Take the individual rows organized by fuel_type_code_pudl, and turn them
    # into columns, each with the total MMBTU for that fuel, year, and plant.
    heat_pivot = heat_df.pivot_table(
        index=['year', 'plant_id_eia'],
        columns='fuel_type_code_pudl',
        values='fuel_consumed_mmbtu')

    # Add a column that has the *total* heat content of all fuels:
    heat_pivot['total'] = heat_pivot.sum(axis=1, numeric_only=True)

    # Replace any NaN values we got from pivoting with zeros.
    heat_pivot = heat_pivot.fillna(value=0)

    # Divide all columns by the total heat content, giving us the proportions
    # for each fuel instead of the heat content.
    heat_pivot = heat_pivot.divide(heat_pivot.total, axis='index')

    # Drop the total column (it's nothing but 1.0 values) and clean up the
    # index and columns a bit before returning the DF.
    heat_pivot = heat_pivot.drop('total', axis=1)
    heat_pivot = heat_pivot.reset_index()
    del heat_pivot.columns.name

    return heat_pivot


def primary_fuel_gf_eia923(gf_df, id_col='plant_id_eia', fuel_thresh=0.5):
    """Determines a plant's primary fuel from EIA923 generation fuel table."""
    gf_df = gf_df.copy()

    # Figure out the heat content proportions of each fuel received:
    gf_by_heat = plant_fuel_proportions_gf_eia923(gf_df)

    # On a per plant, per year basis, identify the fuel that made the largest
    # contribution to the plant's overall heat content consumed. If that
    # proportion is greater than fuel_thresh, set the primary_fuel to be
    # that fuel.  Otherwise, leave it None.
    gf_by_heat = gf_by_heat.set_index([id_col, 'report_year'])
    mask = gf_by_heat >= fuel_thresh
    gf_by_heat = gf_by_heat.where(mask)
    gf_by_heat['primary_fuel'] = gf_by_heat.idxmax(axis=1)
    return gf_by_heat[['primary_fuel', ]].reset_index()
