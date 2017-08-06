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


def simple_select(table_name, pudl_engine):
    """
    Simple select statement.

    Args:
        table_name: pudl table name
        pudl_engine

    Returns:
        DataFrame from table
    """
    # Pull in the table
    tbl = models.PUDLBase.metadata.tables[table_name]
    # Creates a sql Select object
    select = sa.sql.select([tbl, ])
    # Converts sql object to pandas dataframe
    return(pd.read_sql(select, pudl_engine))


def yearly_sum_eia(table,
                   sum_by,
                   columns=['plant_id',
                            'report_date',
                            'generator_id']):
    if 'report_date' in table.columns:
        table = table.set_index(pd.DatetimeIndex(table['report_date']).year)
        table.drop('report_date', axis=1, inplace=True)
        table.reset_index(inplace=True)
    gb = table.groupby(by=columns)
    return(gb.agg({sum_by: np.sum}))


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
        pt['utilities_ferc'].c.respondent_id,
        pt['utilities_ferc'].c.util_id_pudl,
        pt['utilities_ferc'].c.respondent_name,
        pt['plants_ferc'].c.plant_id_pudl,
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
            pt['utilities_ferc'].c.respondent_id ==
            pt['plants_steam_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.respondent_id ==
            pt['plants_steam_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.plant_name ==
            pt['plants_steam_ferc1'].c.plant_name,
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
        pt['utilities_ferc'].c.respondent_id,
        pt['utilities_ferc'].c.respondent_name,
        pt['utilities_ferc'].c.util_id_pudl,
        pt['plants_ferc'].c.plant_id_pudl,
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
            pt['utilities_ferc'].c.respondent_id ==
            pt['fuel_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.respondent_id ==
            pt['fuel_ferc1'].c.respondent_id,
            pt['plants_ferc'].c.plant_name ==
            pt['fuel_ferc1'].c.plant_name))

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
    return(plants_by_heat[['primary_fuel', ]].reset_index())


def plant_fuel_proportions_ferc1(fuel_df):
    """Calculate annual fuel proportions by plant based on FERC data."""
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

    return(heat_pivot)


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


def partition_k(collection, k):
    """
    """
    for part in partition(collection):
        if(len(part) == k):
            yield part


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


def fercplants(plant_tables=['f1_steam',
                             'f1_gnrt_plant',
                             'f1_hydro',
                             'f1_pumped_storage'],
               years=constants.ferc1_working_years,
               new=True,
               min_capacity=5.0):
    """
    Generate a list of FERC plants for matching with EIA plants.

    There are several kinds of FERC plants, with different information stored
    in different FERC database tables. FERC doesn't provide any kind of
    plant_id like EIA, so the unique identifier that we're using is a
    combination of the respondent_id (the utility) and plant_name.

    For each table in the FERC DB that contains per-plant information, we'll
    grab the respondent_id and plant_name, and join that with respondent_name
    so that the utility is more readily identifiable.  We'll also add a column
    indicating what table the plant came from, and return a DataFrame with
    those four columns in it, for use in the matching. That matching currently
    happens in an Excel spreadsheet, so you will likely want to output the
    resulting DataFrame as a CSV or XLSX file.

    The function can generate an exhaustive list of plants, or it can only grab
    plants from a particular range of years. It can also optionally grab only
    new plants i.e. those which do not appear in the existing PUDL database.
    This is useful for finding new plants when a new year of FERC data comes
    out.

    Args:
        f1_tables (list): A list of tables in the FERC Form 1 DB whose plants
            you want to get information about.  Can include any of: f1_steam,
            f1_gnrt_plant, f1_hydro, and f1_pumped_storage.
        years (list): The set of years for which you wish to obtain plant by
            plant information.
        new (boolean): If True (the default) then return only those plants
            which appear in the years of FERC data being specified by years,
            and NOT also in the currently initialized PUDL DB.
        min_capacity (float): The smallest size plant, in MW, that should be
            included in the output. This avoids most of the plants being tiny.

    Returns:
        DataFrame: with four columns: respondent_id, respondent_name,
            plant_name, and plant_table.
    """
    # Need to be able to use years outside the "valid" range if we're trying
    # to get new plant ID info...
    if not new:
        for yr in years:
            assert yr in constants.ferc1_working_years

    okay_tbls = ['f1_steam',
                 'f1_gnrt_plant',
                 'f1_hydro',
                 'f1_pumped_storage']

    # Function only knows how to work with these tables.
    for tbl in plant_tables:
        assert tbl in okay_tbls

    f1_engine = ferc1.db_connect_ferc1()

    # Need to make sure we have a populated metadata object, which isn't
    # always the case, since folks often are not initializing the FERC DB.
    ferc1.define_db(max(constants.ferc1_working_years),
                    constants.ferc1_working_tables,
                    ferc1.ferc1_meta)
    f1_tbls = ferc1.ferc1_meta.tables

    # FERC doesn't use the sme column names for the same values across all of
    # Their tables... but all of these are cpacity in MW.
    capacity_cols = {'f1_steam': 'tot_capacity',
                     'f1_gnrt_plant': 'capacity_rating',
                     'f1_hydro': 'tot_capacity',
                     'f1_pumped_storage': 'tot_capacity'}

    rspndnt_tbl = f1_tbls['f1_respondent_id']
    ferc1_plants_all = pd.DataFrame()
    for tbl in plant_tables:
        plant_select = sa.sql.select([
            f1_tbls[tbl].c.respondent_id,
            f1_tbls[tbl].c.plant_name,
            rspndnt_tbl.c.respondent_name
        ]).distinct().where(
            sa.and_(
                f1_tbls[tbl].c.respondent_id == rspndnt_tbl.c.respondent_id,
                f1_tbls[tbl].c.plant_name != '',
                f1_tbls[tbl].columns[capacity_cols[tbl]] >= min_capacity,
                f1_tbls[tbl].c.report_year.in_(years)
            )
        )
        # Add all the plants from the current table to our bigger list:
        new_plants = pd.read_sql(plant_select, f1_engine)
        new_plants.respondent_name = new_plants.respondent_name.str.strip()
        new_plants.respondent_name = new_plants.respondent_name.str.title()
        new_plants.plant_name = new_plants.plant_name.str.strip().str.title()
        new_plants['plant_table'] = tbl
        ferc1_plants_all = ferc1_plants_all.append(
            new_plants[['respondent_id',
                        'respondent_name',
                        'plant_name',
                        'plant_table']]
        )

    # If we're only trying to get the NEW plants, then we need to see which
    # ones we've already got in the PUDL DB, and look at what's different.
    if(new):
        ferc1_plants_all = ferc1_plants_all.set_index(
            ['respondent_id', 'plant_name'])

        pudl_engine = pudl.db_connect_pudl()
        pudl_tbls = pudl.models.PUDLBase.metadata.tables

        ferc1_plants_tbl = pudl_tbls['plants_ferc']
        ferc1_plants_select = sa.sql.select([
            ferc1_plants_tbl.c.respondent_id,
            ferc1_plants_tbl.c.plant_name
        ]).distinct()
        ferc1_plants_old = pd.read_sql(ferc1_plants_select, pudl_engine)
        ferc1_plants_old = ferc1_plants_old.set_index(
            ['respondent_id', 'plant_name'])

        # Take the difference between the two table indexes -- I.e. get a
        # list of just the index values that appear in the FERC index, but
        # not in the PUDL index.
        new_index = ferc1_plants_all.index.difference(ferc1_plants_old.index)
        ferc1_plants = ferc1_plants_all.loc[new_index].reset_index()
    else:
        ferc1_plants = ferc1_plants_all

    return(ferc1_plants)


def random_chunk(li, min_chunk=1, max_chunk=3):
    """Chunk a list of items into a list of lists containing the same items.

    Takes a list, and creates a generator that returns sub-lists containing
    at least min_chunk items and at most max_chunk items from the original
    list. Used here to create groupings of individual generators that get
    aggregated into either FERC plants or PUDL plants for synthetic data.
    """
    from itertools import islice
    from random import randint
    it = iter(li)
    while True:
        nxt = list(islice(it, randint(min_chunk, max_chunk)))
        if nxt:
            yield nxt
        else:
            break


def zippertestdata(gens=50, max_group_size=6, series=3, samples=10,
                   noise=[0.25, 0.25, 0.25]):
    """Generate a test dataset for the datazipper, with known solutions.

    Args:
        gens (int): number of actual atomic units (analogous to generators)
            which may not fully enumerated in either dataset.
        max_group_size (int): Maximum number of atomic units which should
            be allowed to aggregate in the FERC groups.
        series (int): How many shared data series should exist between the two
            datasets, for use in connecting them to each other?
        samples (int): How many samples should be available in each shared data
            series?
        noise (array): An array-like collection of numbers indicating the
            amount of noise (dispersion) to add between the two synthetic
            datasets. Larger numbers will result in lower correlations.

    Returns:
        eia_df (pd.DataFrame): Synthetic test data representing the EIA data
            to be used in connecting EIA and FERC plant data. For now it
            assumes that we have annual data at the generator level for all
            of the variables we're using to correlate.
        ferc_df (pd.DataFrame): Synthetic data representing the FERC data to
            be used in connecting the EIA and FERC plant data. Does not have
            individual generator level information. Rather, the dependent
            variables are grouped by ferc_plant_id, each of which is a sum
            of several original generator level data series. The ferc_plant_id
            values indicate which original generators went into creating the
            data series, allowing us to easily check whether they've been
            correctly matched.
    """
    from string import ascii_uppercase, ascii_lowercase
    from itertools import product

    # Make sure we've got enough plant IDs to work with:
    rpt = 1
    while(len(ascii_lowercase)**rpt < gens):
        rpt = rpt + 1

    # Generate the list of atomic generator IDs for both FERC (upper case) and
    # EIA (lower case) Using the same IDs across both datasets will make it
    # easy for us to tell whether we've correctly inferred the connections
    # between them.
    gen_ids_ferc = [''.join(s) for s in product(ascii_uppercase, repeat=rpt)]
    gen_ids_ferc = gen_ids_ferc[:gens]
    gen_ids_eia = [''.join(s) for s in product(ascii_lowercase, repeat=rpt)]
    gen_ids_eia = gen_ids_eia[:gens]

    # make some dummy years to use as the independent (time) variable:
    years = np.arange(2000, 2000 + samples)

    # Set up some empty Data Frames to receive the synthetic data:
    eia_df = pd.DataFrame(columns=[['year', 'eia_gen_id']])
    ferc_df = pd.DataFrame(columns=[['year', 'ferc_gen_id']])

    # Now we create several pairs of synthetic data by atomic generator, which
    # will exist in both datasets, but apply some noise to one of them, so the
    # correlation between them isn't perfect.

    for ferc_gen_id, eia_gen_id in zip(gen_ids_ferc, gen_ids_eia):
        # Create a new set of FERC and EIA records, 'samples' long, with
        # years as the independent variable.
        eia_new = pd.DataFrame(columns=['year', 'eia_gen_id'])
        ferc_new = pd.DataFrame(columns=['year', 'ferc_gen_id'])

        eia_new['year'] = years
        eia_new['eia_gen_id'] = eia_gen_id
        ferc_new['year'] = years
        ferc_new['ferc_gen_id'] = ferc_gen_id
        for N in range(0, series):
            series_label = 'series{}'.format(N)
            # Create a pair of logarithmically distributed correlated
            # randomized data series:
            eia_data = 10**(np.random.uniform(low=3, high=9, size=samples))
            ferc_data = eia_data * np.random.normal(loc=1,
                                                    scale=noise[N],
                                                    size=samples)
            eia_new[series_label] = eia_data
            ferc_new[series_label] = ferc_data

        # Add the new set of records (samples years for each ID)
        eia_df = eia_df.append(eia_new)
        ferc_df = ferc_df.append(ferc_new)

    # Now we're going to group the "true" data together into groups which are
    # the same in both datasets -- these are analogous to the PUDL Plant ID
    # groups. Here we're just randomly chunking the list of all generator IDs
    # into little pieces:

    eia_groups = [group for group in random_chunk(gen_ids_eia,
                                                  min_chunk=1,
                                                  max_chunk=max_group_size)]

    ferc_groups = [[id.upper() for id in group] for group in eia_groups]

    # Then within each of these groups, we need to randomly aggregate the data
    # series on the FERC side, to represent the non-atomic FERC plants, which
    # are made up of more than a single generator, but which are still
    # contained within the PUDL ID group:
    ferc_plant_groups = []
    for group in ferc_groups:
        ferc_plant_groups.append([g for g in
                                  random_chunk(group,
                                               min_chunk=1,
                                               max_chunk=max_group_size)])

    for pudl_plant_id in np.arange(0, len(ferc_plant_groups)):
        # set the pudl_plant_id on every record whose ID is in this group.
        for ferc_plant in ferc_plant_groups[pudl_plant_id]:
            # set ferc_plant_id on every record whose ID is in this sub-group.
            ferc_plant_id = '_'.join(ferc_plant)
            ferc_plant_mask = ferc_df.ferc_gen_id.isin(ferc_plant)
            ferc_df.loc[ferc_plant_mask, 'ferc_plant_id'] = ferc_plant_id
            ferc_df.loc[ferc_plant_mask, 'pudl_plant_id'] = pudl_plant_id

    # Fix the type of the pudl_plant_id... getting upcast to float
    ferc_df.pudl_plant_id = ferc_df.pudl_plant_id.astype(int)

    # Assign a numerical pudl_plant_id to each EIA generator, enabling us to
    # compare the same small groups of generators on both the FERC and EIA
    # sides.  This is the only thing that makes the search space workable with
    # so few data points in each series to correlate.
    for pudl_plant_id in np.arange(0, len(eia_groups)):
        eia_group_mask = eia_df.eia_gen_id.isin(eia_groups[pudl_plant_id])
        eia_df.loc[eia_group_mask, 'pudl_plant_id'] = pudl_plant_id

    # Fix the type of the pudl_plant_id... getting upcast to float
    eia_df.pudl_plant_id = eia_df.pudl_plant_id.astype(int)

    # Sum the dependent data series by PUDL plant, FERC plant, and year,
    # creating our synthetic lumped dataset for the algorithm to untangle.
    ferc_gb = ferc_df.groupby(['pudl_plant_id', 'ferc_plant_id', 'year'])
    ferc_df = ferc_gb.agg(sum).reset_index()
    return(eia_df, ferc_df)


def correlate_pudl_plant(eia_df, ferc_df):
    """Calculate correlations between all possible plant mappings.

    Given two dataframes in which the same data is reported, but potentially
    aggregated differently in the two cases, create a bunch of candidate
    matching groups between them, and calculate the correlations between them.
    """
    import re
    # Create a DataFrame where we will accumulate the tests cases:
    eia_test_df = pd.DataFrame(columns=eia_df.columns)
    eia_pudl_ids = eia_df.pudl_plant_id.unique()
    ferc_pudl_ids = ferc_df.pudl_plant_id.unique()
    assert set(eia_pudl_ids) == set(ferc_pudl_ids)
    pudl_plant_ids = eia_pudl_ids

    for pudl_plant_id in pudl_plant_ids:
        # Grab a single PUDL plant from FERC:
        ferc_pudl_plant = ferc_df[ferc_df['pudl_plant_id'] == pudl_plant_id]
        eia_pudl_plant = eia_df[eia_df['pudl_plant_id'] == pudl_plant_id]

        # Count how many FERC plants there are within this PUDL ID.
        ferc_plant_ids = ferc_pudl_plant.ferc_plant_id.unique()
        ferc_plant_n = len(ferc_plant_ids)

        # Enumerate all the EIA generator set coverages with the same number of
        # elements as there are FERC plants.
        eia_test_groups = \
            partition_k(list(eia_pudl_plant.eia_gen_id.unique()), ferc_plant_n)

        # Create new records from the EIA dataframe with the data series
        # aggregated according to each of the candidate generator groupings.
        test_group_id = 0
        for group in eia_test_groups:
            new_eia_grouping = eia_pudl_plant.copy()
            new_eia_grouping['test_group_id'] = test_group_id

            for subgroup in group:
                eia_subgroup_id = '_'.join(subgroup)
                eia_subgroup_mask = new_eia_grouping.eia_gen_id.isin(subgroup)
                new_eia_grouping.loc[eia_subgroup_mask, 'eia_gen_subgroup'] = \
                    eia_subgroup_id

            eia_test_df = eia_test_df.append(new_eia_grouping)
            test_group_id = test_group_id + 1

    eia_test_df['test_group_id'] = eia_test_df['test_group_id'].astype(int)
    eia_test_df = eia_test_df.groupby(['pudl_plant_id',
                                       'test_group_id',
                                       'eia_gen_subgroup',
                                       'year']).agg(sum)

    # Within each (pudl_plant_id, test_group_id) pairing, we'll have a list of
    # N generator subgroups. We need to calculate the correlations with FERC
    # Form 1 for each possible generator subgroup ordering We can generate the
    # list of all possible combinations of FERC plant and EIA subgroups using
    # the itertools.product() function... but what do we do with that
    # information?
    eia_test_df = eia_test_df.reset_index()
    eia_test_df = eia_test_df.rename(
        columns=lambda x: re.sub('(series[0-9]*$)', r'\1_eia', x))
    ferc_df = ferc_df.reset_index()
    ferc_df = ferc_df.drop('index', axis=1)
    ferc_df = ferc_df.rename(
        columns=lambda x: re.sub('(series[0-9]*$)', r'\1_ferc', x))
    both_df = eia_test_df.merge(ferc_df, on=['pudl_plant_id', 'year'])

    return(both_df)


def score_all(df):
    """
    Select the mapping of FERC to EIA plant mappings based on data series.

    This needs to be generalized to work for more than one data series.
    """
    candidates = {}
    # Iterate through each PUDL Plant ID
    for ppid in df.pudl_plant_id.unique():
        combo_list = []
        # Create a miniature dataframe for just this PUDL Plant ID:
        ppid_df = df[df.pudl_plant_id == ppid].copy()
        # For each test group that exists within this PUDL Plant ID:
        for tgid in ppid_df.test_group_id.unique():
            # Create yet another subset dataframe... jsut for this test group:
            tg_df = ppid_df[ppid_df.test_group_id == tgid].copy()
            ferc_ids = tg_df.ferc_plant_id.unique()
            ferc_combos = itertools.combinations(ferc_ids, len(ferc_ids))
            eia_ids = tg_df.eia_gen_subgroup.unique()
            eia_permus = itertools.permutations(eia_ids, len(eia_ids))
            combos = list(itertools.product(ferc_combos, eia_permus))
            combo_list = combo_list + combos

        # Re-organize these lists of tuples into binary mappings... ugh.
        y = []
        for x in combo_list:
            y = y + [[z for z in zip(x[0], x[1])], ]

        # Now we've got a dictionary with pudl plant IDs as the keys,
        # and lists of all possible candidate FERC/EIA mappings as the values.
        candidates[ppid] = y

    candidates_df = pd.DataFrame(columns=df.columns)
    candidates_df['candidate_id'] = []
    candidates_df.drop(['test_group_id', ], axis=1, inplace=True)

    for ppid in candidates.keys():
        cid = 0
        for c in candidates[ppid]:
            candidate = pd.DataFrame(columns=candidates_df.columns)
            for mapping in c:
                newrow = df.loc[(df['ferc_plant_id'] == mapping[0]) &
                                (df['eia_gen_subgroup'] == mapping[1])]
                candidate = candidate.append(newrow)
            candidate['candidate_id'] = cid
            candidates_df = candidates_df.append(candidate)
            cid = cid + 1

    candidates_df.candidate_id = candidates_df.candidate_id.astype(int)
    candidates_df = candidates_df.drop('test_group_id', axis=1)
    cand_gb = candidates_df.groupby(['pudl_plant_id', 'candidate_id'])
    cand_mean_corrs = cand_gb.agg({'corr': np.mean})
    idx = cand_mean_corrs.groupby(['pudl_plant_id', ])['corr'].\
        transform(max) == cand_mean_corrs['corr']

    winners = cand_mean_corrs[idx].reset_index()
    winners = winners.merge(candidates_df,
                            how='left',
                            on=['pudl_plant_id', 'candidate_id'])
    winners = winners.drop(['corr_y', ], axis=1).\
        drop_duplicates(['eia_gen_subgroup', ]).\
        rename(columns={'corr_x': 'mean_corr'})
    winners['success'] = \
        winners.eia_gen_subgroup == winners.ferc_plant_id.str.lower()
    return(winners)


def correlation_merge():
    """
    Merge two datasets based on correlations between selected series.

    A couple of times now, we've come up against the issue of having two
    datasets which contain similar/comparable data, but no common ID that
    can be used to merge them together conclusively. This function attempts
    to use the correlations between common data series in the two data sets
    to infer a mapping between them, so that other un-shared fields in the
    two data sets can also be cross-referenced.

    Pieces of the two datasets that we need to use to create the connection
    between them include:
    - the independent variable for correlations (e.g. time) which must be
      present in each dataset.
    - the list of pairs of dependent variables from each dataset, and how they
      map to each other. Maybe this should be a dictionary of DataFrame field
      names.
    - A subset identifier, (in our case the PUDL Plant ID) that can be used
      to limit the number of correlations which are attempted. This also needs
      to be present in both datasets.
    - The atomic identifier for the objects which we are trying different
      combinations of within either dataset, to try and get a good match. In
      our case, this is the FERC (respondent_id, plant_name) and the EIA
      plant_id (or is it boiler_id or generator_id...)

    Once we've got all of that stuff pulled together, the algorithm goes
    something like this:
    - For each subset_id, come up with all the set coverages of a given number
      of elements within each of the datasets (and actually, this could
      potentially be generalized to more than 2 datasets... but let's not get
      crazy right now)
    - Generate the list of all possible mappings from N elements in one
      dataset to the other.
    - For each of those mappings, for each value of N ranging from 1 to the
      smaller maximum number of atomic elements within the two subsets,
      calculate the correlation between each of the enumerated depdendent
      variables.
    - At this point, we should have a bunch of correlations that we can score.
      The bigger the N is, the more specific the mapping from one dataset to
      the other will be, and specificity is valuble, so given similar
      correlations between two mappings, the more specific of the two should
      be preferred. But by how much? How do we encode this preference flexibly?
    - It might also be the case that different dependent variables are more or
      less meaningful, in terms of whether we believe the two data sets really
      match up, so we might want to be able to adjust the weighting of the
      different data series in scoring the calculated correlations.
    - Once the scoring mechanism has been determined, what do we output?
      Ideally, we should be able to create new subset_ids, which show which
      collections of records in the two datasets best correlate with each
      other, and should be used to link the two data sets together. This would
      allow direct comparison of other dependent variables, if all records
      associated with a given subset_id and time slice were aggregated
      together appropriately.

    A lot of this is reminding me of "clustering" analysis, and I'm wondering
    which parts of it have already been done "right" somewhere that could be
    learned from.
    """
