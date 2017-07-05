"""A module with functions to aid in data analysis using the PUDL database."""

# Useful high-level external modules.
import numpy as np
import pandas as pd
import sqlalchemy as sa
import matplotlib.pyplot as plt

# Our own code...
from pudl import pudl, ferc1, eia923, settings, constants
from pudl import models, models_ferc1, models_eia923
from pudl import clean_eia923, clean_ferc1, clean_pudl


def eia_operator_plants(operator_id, pudl_engine):
    """Return all the EIA plant IDs associated with a given EIA operator ID."""
    Session = sa.orm.sessionmaker()
    Session.configure(bind=pudl_engine)
    session = Session()
    pudl_plant_ids = [p.plant_id for p in session.query(models.UtilityEIA923).
                      filter_by(operator_id=operator_id).
                      first().util_pudl.plants]
    eia923_plant_ids = [p.plant_id for p in
                        session.query(models.PlantEIA923).
                        filter(models.
                               PlantEIA923.
                               plant_id_pudl.
                               in_(pudl_plant_ids))]
    session.close_all()
    return(eia923_plant_ids)


def consolidate_ferc1_expns(steam_df, min_capfac=0.6, min_corr=0.5):
    """
    Calculate non-fuel production & nonproduction costs from a steam DataFrame.

    Takes a DataFrame containing information from the plants_steam_ferc1 table
    and add columns representing the non-production costs, and non-fuel
    production costs, which are sums of other expense columns. Which columns
    are treated as production vs. non-production costs is determined based on
    the overall correlation between those column values and net_generation_mwh
    for the entire steam_df DataFrame.

    Args:
        steam_df (DataFrame): Data selected from the PUDL plants_steam_ferc1
            table, containing expense columns, prefixed with expns_
        min_capfac (float): Minimum capacity factor required for a plant's
            data to be used in determining which expense columns are
            production vs. non-production costs.
        min_corr (float): Minimum correlation with net_generation_mwh required
            to indicate that a given expense field should be considered a
            "production" cost.

    Returns:
        DataFrame containing all the same information as the original steam_df,
            but with two additional columns consolidating the non-fuel
            production and non-production costs for ease of calculation.
    """
    steam_df = steam_df.copy()
    # Calculate correlation of expenses to net power generation. Require a
    # minimum plant capacity factor of 0.6 so we the signal will be high,
    # but we'll still have lots of plants to look at:
    expns_corr = ferc1_expns_corr(steam_df, capacity_factor=min_capfac)

    # We've already got fuel separately, and we know it's a production expense
    expns_corr.pop('expns_fuel')
    # Sort these expense fields into nonfuel production (nonfuel_px) or
    # non-production (npx) expenses.
    nonfuel_px = [k for k in expns_corr.keys() if expns_corr[k] >= min_corr]
    npx = [k for k in expns_corr.keys() if expns_corr[k] < min_corr]

    # The three main categories of expenses we're reporting:
    # - fuel production expenses (already in the table)
    # - non-fuel production expenses
    steam_df['expns_total_nonfuel_production'] = \
        steam_df[nonfuel_px].copy().sum(axis=1)
    # - non-production expenses
    steam_df['expns_total_nonproduction'] = steam_df[npx].copy().sum(axis=1)

    return(steam_df)


def ferc1_expns_corr(steam_df, capacity_factor=0.6):
    """
    Calculate generation vs. expense correlation for FERC Form 1 plants.

    This function helped us identify which of the expns_* fields in the FERC
    Form 1 dataset represent production costs, and which are non-production
    costs, for the purposes of modeling marginal cost of electricity from
    various plants.  We expect the difference in expenses vs. generation to
    be more indicative of production vs. non-production costs for plants with
    higher capacity factors, and since what we're trying to do here is
    identify which *fields* in the FERC Form 1 data are production costs, we
    allow a capacity_factor threshold to be set -- analysis is only done for
    those plants with capacity factors larger than the threshold.

    Additionaly, some types of plants simply do not have some types of
    expenses, so to keep those plants from dragging down otherwise meaningful
    correlations, any zero expense values are dropped before calculating the
    correlations.

    Returns a dictionary with expns_ field names as the keys, and correlations
    as the values.
    """
    steam_df = steam_df.copy()
    steam_df['capacity_factor'] = \
        (steam_df['net_generation_mwh'] / 8760 * steam_df['total_capacity_mw'])

    # Limit plants by capacity factor
    steam_df = steam_df[steam_df['capacity_factor'] > capacity_factor]

    # This is all the expns_* fields, except for the per_mwh and total.
    cols_to_correlate = ['expns_operations',
                         'expns_fuel',
                         'expns_coolants',
                         'expns_steam',
                         'expns_steam_other',
                         'expns_transfer',
                         'expns_electric',
                         'expns_misc_power',
                         'expns_rents',
                         'expns_allowances',
                         'expns_engineering',
                         'expns_structures',
                         'expns_boiler',
                         'expns_plants',
                         'expns_misc_steam']

    expns_corr = {}
    for expns in cols_to_correlate:
        mwh_plants = steam_df.net_generation_mwh[steam_df[expns] != 0]
        expns_plants = steam_df[expns][steam_df[expns] != 0]
        expns_corr[expns] = np.corrcoef(mwh_plants, expns_plants)[0, 1]

    return(expns_corr)


def get_steam_ferc1_df(testing=False):
    """
    FERC Steam Stuff for Eric:
     - Respondent ID
     - Respondent Name
     - Plant Name
     - Report Year
     - PUDL ID
     - Net Generation (MWh)
     - Capacity (MW)
     - Capacity Factor
     - All itemized expenses (USD)
     - Total fuel expenses (USD)
     - Total non-fuel production expenses (USD)
     - Total non-production expenses (USD)
     - Total MMBTU consumed (USD)
     - Heat Rate (BTU/kWh)
    """
    # Connect to the DB
    pudl_engine = pudl.db_connect_pudl(testing=testing)

    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    steam_ferc1_select = sa.sql.select([
        pt['plants_steam_ferc1'].c.report_year,
        pt['utilities_ferc1'].c.respondent_id,
        pt['utilities_ferc1'].c.util_id_pudl,
        pt['utilities_ferc1'].c.respondent_name,
        pt['plants_ferc1'].c.plant_id_pudl,
        pt['plants_steam_ferc1'].c.plant_name,
        pt['plants_steam_ferc1'].c.total_capacity_mw,
        pt['plants_steam_ferc1'].c.year_constructed,
        pt['plants_steam_ferc1'].c.year_installed,
        pt['plants_steam_ferc1'].c.peak_demand_mw,
        pt['plants_steam_ferc1'].c.water_limited_mw,
        pt['plants_steam_ferc1'].c.not_water_limited_mw,
        pt['plants_steam_ferc1'].c.plant_hours,
        pt['plants_steam_ferc1'].c.net_generation_mwh,
        pt['plants_steam_ferc1'].c.expns_operations,
        pt['plants_steam_ferc1'].c.expns_fuel,
        pt['plants_steam_ferc1'].c.expns_coolants,
        pt['plants_steam_ferc1'].c.expns_steam,
        pt['plants_steam_ferc1'].c.expns_steam_other,
        pt['plants_steam_ferc1'].c.expns_transfer,
        pt['plants_steam_ferc1'].c.expns_electric,
        pt['plants_steam_ferc1'].c.expns_misc_power,
        pt['plants_steam_ferc1'].c.expns_rents,
        pt['plants_steam_ferc1'].c.expns_allowances,
        pt['plants_steam_ferc1'].c.expns_engineering,
        pt['plants_steam_ferc1'].c.expns_structures,
        pt['plants_steam_ferc1'].c.expns_boiler,
        pt['plants_steam_ferc1'].c.expns_plants,
        pt['plants_steam_ferc1'].c.expns_misc_steam,
        pt['plants_steam_ferc1'].c.expns_production_total,
        pt['plants_steam_ferc1'].c.expns_per_mwh]).\
        where(sa.sql.and_(
            pt['utilities_ferc1'].c.respondent_id == pt['plants_steam_ferc1'].c.respondent_id,
            pt['plants_ferc1'].c.respondent_id == pt['plants_steam_ferc1'].c.respondent_id,
            pt['plants_ferc1'].c.plant_name == pt['plants_steam_ferc1'].c.plant_name,
        ))

    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    return(steam_df)


def get_fuel_ferc1_df(testing=False):
    """
    Pull a useful dataframe related to FERC Form 1 fuel information.

    We often want to pull information from the PUDL database that is
    not exactly what's contained in the table. This might include
    joining with other tables to get IDs for cross referencing, or doing
    some basic calculations to get e.g. heat rate or capacity factor.
    """
    # Connect to the DB
    pudl_engine = pudl.db_connect_pudl(testing=testing)

    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    # Build a SELECT statement that gives us information from several different
    # tables that are relevant to FERC Fuel.
    fuel_ferc1_select = sa.sql.select([
        pt['fuel_ferc1'].c.report_year,
        pt['utilities_ferc1'].c.respondent_id,
        pt['utilities_ferc1'].c.respondent_name,
        pt['utilities_ferc1'].c.util_id_pudl,
        pt['plants_ferc1'].c.plant_id_pudl,
        pt['fuel_ferc1'].c.plant_name,
        pt['fuel_ferc1'].c.fuel,
        pt['fuel_ferc1'].c.fuel_qty_burned,
        pt['fuel_ferc1'].c.fuel_avg_mmbtu_per_unit,
        pt['fuel_ferc1'].c.fuel_cost_per_unit_burned,
        pt['fuel_ferc1'].c.fuel_cost_per_unit_delivered,
        pt['fuel_ferc1'].c.fuel_cost_per_mmbtu,
        pt['fuel_ferc1'].c.fuel_cost_per_mwh,
        pt['fuel_ferc1'].c.fuel_mmbtu_per_mwh]).\
        where(sa.sql.and_(
            pt['utilities_ferc1'].c.respondent_id == pt['fuel_ferc1'].c.respondent_id,
            pt['plants_ferc1'].c.respondent_id == pt['fuel_ferc1'].c.respondent_id,
            pt['plants_ferc1'].c.plant_name == pt['fuel_ferc1'].c.plant_name))

    # Pull the data from the DB into a DataFrame
    fuel_df = pd.read_sql(fuel_ferc1_select, pudl_engine)

    # Calculate additional useful quantities, and add them to the DataFrame
    # before we return the result, including

    return(fuel_df)


def generator_proportion_eia923(g):
    """
    Generate a dataframe with the proportion of generation for each generator.

    Args:
        g: a dataframe from either all of generation_eia923 or some subset of
        records from generation_eia923. The dataframe needs the following
        columns to be present:
            plant_id, generator_id, report_date, net_generation_mwh

    Returns: a dataframe with:
            report_date, plant_id, generator_id, proportion_of_generation
    """
    # Set the datetimeindex
    g = g.set_index(pd.DatetimeIndex(g['report_date']))
    # groupby plant_id and by year
    g_yr = g.groupby([pd.TimeGrouper(freq='A'), 'plant_id', 'generator_id'])
    # sum net_gen by year by plant
    g_net_generation_per_generator = pd.DataFrame(
        g_yr.net_generation_mwh.sum())
    g_net_generation_per_generator = \
        g_net_generation_per_generator.reset_index(level=['generator_id'])

    # groupby plant_id and by year
    g_net_generation_per_plant = g.groupby(
        [pd.TimeGrouper(freq='A'), 'plant_id'])
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
    return(g_gens_proportion)


def values_by_generator_eia923(table_eia923, column_name, g):
    """
    Generate a dataframe with a plant value proportioned out by generator.

    Args:
        table_eia923: an EIA923 table (this has been tested with
        fuel_receipts_costs_eia923 and generation_fuel_eia923).
        column_name: a column name from the table_eia923.
        g: a dataframe from either all of generation_eia923 or some subset of
        records from generation_eia923. The dataframe needs the following
        columns to be present:
            plant_id, generator_id, report_date, and net_generation_mwh.

    Returns: a dataframe with report_date, plant_id, generator_id, and the
        proportioned value from the column_name.
    """
    # Set the datetimeindex
    table_eia923 = table_eia923.set_index(
        pd.DatetimeIndex(table_eia923['report_date']))
    # groupby plant_id and by year
    table_eia923_gb = table_eia923.groupby(
        [pd.TimeGrouper(freq='A'), 'plant_id'])
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
    return(g_generator)


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
        plants_by_primary_fuel (DataFrame): a DataFrame containing report_year,
            respondent_id, plant_name, and primary_fuel.
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
    fuels = fuel_df.fuel.unique()

    heat_pivot['primary_fuel'] = None
    for f in fuel_df.fuel.unique():
        fuel_mask = heat_pivot[f] >= fuel_thresh
        heat_pivot.primary_fuel = \
            heat_pivot.primary_fuel.where(~fuel_mask, other=f)

    plants_by_primary_fuel = \
        heat_pivot.reset_index()[['report_year',
                                  'respondent_id',
                                  'plant_name',
                                  'primary_fuel']]
    plants_by_primary_fuel = \
        plants_by_primary_fuel.merge(mmbtu_total.reset_index())
    plants_by_primary_fuel.rename(columns={'total': 'total_mmbtu'},
                                  inplace=True)

    return(plants_by_primary_fuel)


def plant_fuel_proportions_frc_eia923(frc_df):
    """Calculate annual fuel proportions by plant from EIA923 fuel receipts."""
    frc_df = frc_df.copy()

    # Add a column with total fuel heat content per delivery
    frc_df['total_mmbtu'] = frc_df.fuel_quantity * frc_df.average_heat_content

    # Drop everything but report_date, plant_id, fuel_group, total_mmbtu
    frc_df = frc_df[['report_date', 'plant_id', 'fuel_group', 'total_mmbtu']]

    # Group by report_date(annual), plant_id, fuel_group
    frc_gb = frc_df.groupby(
        ['plant_id', pd.TimeGrouper(freq='A'), 'fuel_group'])

    # Add up all the MMBTU for each plant & year. At this point each record
    # in the dataframe contains only information about a single fuel.
    heat_df = frc_gb.agg(np.sum)

    # Simplfy the DF a little before we turn it into a pivot table.
    heat_df = heat_df.reset_index()
    heat_df['year'] = pd.DatetimeIndex(heat_df['report_date']).year
    heat_df = heat_df.drop('report_date', axis=1)

    # Take the individual rows organized by fuel_group, and turn them into
    # columns, each with the total MMBTU for that fuel, year, and plant.
    heat_pivot = heat_df.pivot_table(
        index=['year', 'plant_id'],
        columns='fuel_group',
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

    return(heat_pivot)


def primary_fuel_frc_eia923(frc_df, fuel_thresh=0.5):
    """Determine a plant's primary fuel from EIA923 fuel receipts table."""
    frc_df = frc_df.copy()

    # Figure out the heat content proportions of each fuel received:
    frc_by_heat = plant_fuel_proportions_frc_eia923(frc_df)

    # On a per plant, per year basis, identify the fuel that made the largest
    # contribution to the plant's overall heat content consumed. If that
    # proportion is greater than fuel_thresh, set the primary_fuel to be
    # that fuel.  Otherwise, leave it None.
    frc_by_heat = frc_by_heat.set_index(['plant_id', 'year'])
    mask = frc_by_heat >= fuel_thresh
    frc_by_heat = frc_by_heat.where(mask)
    frc_by_heat['primary_fuel'] = frc_by_heat.idxmax(axis=1)
    return(frc_by_heat[['primary_fuel', ]].reset_index())


def plant_fuel_proportions_gf_eia923(gf_df):
    """Calculate annual fuel proportions by plant from EIA923 gen fuel."""
    gf_df = gf_df.copy()

    # Drop everything but report_date, plant_id, fuel_group, total_mmbtu
    gf_df = gf_df[['report_date',
                   'plant_id',
                   'aer_fuel_category',
                   'fuel_consumed_total_mmbtu']]

    # Group by report_date(annual), plant_id, fuel_group
    gf_gb = gf_df.groupby(
        ['plant_id', pd.TimeGrouper(freq='A'), 'aer_fuel_category'])

    # Add up all the MMBTU for each plant & year. At this point each record
    # in the dataframe contains only information about a single fuel.
    heat_df = gf_gb.agg(np.sum)

    # Simplfy the DF a little before we turn it into a pivot table.
    heat_df = heat_df.reset_index()
    heat_df['year'] = pd.DatetimeIndex(heat_df['report_date']).year
    heat_df = heat_df.drop('report_date', axis=1)

    # Take the individual rows organized by fuel_group, and turn them into
    # columns, each with the total MMBTU for that fuel, year, and plant.
    heat_pivot = heat_df.pivot_table(
        index=['year', 'plant_id'],
        columns='aer_fuel_category',
        values='fuel_consumed_total_mmbtu')

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

    return(heat_pivot)


def primary_fuel_gf_eia923(gf_df, fuel_thresh=0.5):
    """Determine a plant's primary fuel from EIA923 generation fuel table."""
    gf_df = gf_df.copy()

    # Figure out the heat content proportions of each fuel received:
    gf_by_heat = plant_fuel_proportions_gf_eia923(gf_df)

    # On a per plant, per year basis, identify the fuel that made the largest
    # contribution to the plant's overall heat content consumed. If that
    # proportion is greater than fuel_thresh, set the primary_fuel to be
    # that fuel.  Otherwise, leave it None.
    gf_by_heat = gf_by_heat.set_index(['plant_id', 'year'])
    mask = gf_by_heat >= fuel_thresh
    gf_by_heat = gf_by_heat.where(mask)
    gf_by_heat['primary_fuel'] = gf_by_heat.idxmax(axis=1)
    return(gf_by_heat[['primary_fuel', ]].reset_index())


def partition(collection):
    """
    Generate all possible partitions of a collection of items.

    Recursively generate all the possible groupings of the individual items in
    the collection, such that all items appear, and appear only once.

    We use this funciton to generate different sets of potential plant and
    sub-plant generation infrastructure within the collection of generators
    associated with each plant_id_pudl -- both in the FERC and EIA data, so
    that we can find the association between the two data sets which results
    in the highest correlation between some variables reported in each one.
    Namely: net generation, fuel heat content, and fuel costs.

    Args:
        collection (list of items): the set to partition.
    Returns:
        A list of all valid set partitions.
    """
    if len(collection) == 1:
        yield [collection]
        return

    first = collection[0]
    for smaller in partition(collection[1:]):
        # insert `first` in each of the subpartition's subsets
        for n, subset in enumerate(smaller):
            yield smaller[:n] + [[first] + subset] + smaller[n + 1:]
        # put `first` in its own subset
        yield [[first]] + smaller
