"""A module with functions to aid in data analysis using the PUDL database."""

# Useful high-level external modules.
import numpy as np
import pandas as pd
import sqlalchemy as sa
import matplotlib.pyplot as plt
import itertools
import random

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


def simple_ferc1_plant_ids(pudl_engine):
    """
    Generate a list of all the PUDL plant IDs which map to a single FERC plant.
    """
    ferc1_plant_ids = pd.read_sql('''SELECT plant_id_pudl FROM plants_ferc''',
                                  pudl_engine)
    ferc1_simple_plant_ids = ferc1_plant_ids.drop_duplicates('plant_id_pudl',
                                                             keep=False)
    return(ferc1_simple_plant_ids)


def simple_eia_plant_ids(pudl_engine):
    """
    Generate a list of all the PUDL plant IDs which map to a single EIA plant.
    """
    eia_plant_ids = pd.read_sql('''SELECT plant_id_pudl FROM plants_eia''',
                                pudl_engine)
    eia_simple_plant_ids = eia_plant_ids.drop_duplicates('plant_id_pudl',
                                                         keep=False)
    return(eia_simple_plant_ids)


def simple_pudl_plant_ids(pudl_engine):
    """
    Get all PUDL plant IDs that map to a single EIA and a single FERC plant ID.
    """
    ferc1_simple = simple_ferc1_plant_ids(pudl_engine)
    eia_simple = simple_eia_plant_ids(pudl_engine)
    pudl_simple = np.intersect1d(ferc1_simple['plant_id_pudl'],
                                 eia_simple['plant_id_pudl'])
    return(pudl_simple)


def ferc_eia_shared_plant_ids(pudl_engine):
    """
    Generate a list of PUDL plant IDs that appear in both FERC and EIA.
    """
    ferc_plant_ids = pd.read_sql('''SELECT plant_id_pudl FROM plants_ferc''',
                                 pudl_engine)
    eia_plant_ids = pd.read_sql('''SELECT plant_id_pudl FROM plants_eia''',
                                pudl_engine)
    shared_plant_ids = np.intersect1d(ferc_plant_ids['plant_id_pudl'],
                                      eia_plant_ids['plant_id_pudl'])
    return(shared_plant_ids)


def ferc_pudl_plant_ids(pudl_engine):
    """Generate a list of PUDL plant IDs that correspond to FERC plants."""
    ferc_plant_ids = pd.read_sql('''SELECT plant_id_pudl FROM plants_ferc''',
                                 pudl_engine)
    return(ferc_plant_ids)


def eia_pudl_plant_ids(pudl_engine):
    """Generate a list of PUDL plant IDs that correspond to EIA plants."""
    eia_plant_ids = pd.read_sql('''SELECT plant_id_pudl FROM plants_eia''',
                                pudl_engine)
    return(eia_plant_ids)


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
    expns_corr = ferc1_expns_corr(steam_df, min_capfac=min_capfac)

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


def ferc1_expns_corr(steam_df, min_capfac=0.6):
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
    steam_df = steam_df[steam_df['capacity_factor'] > min_capfac]

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


def get_steam_ferc1_df(pudl_engine):
    """Select and join some useful fields from the FERC Form 1 steam table."""
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


def get_fuel_ferc1_df(pudl_engine):
    """
    Pull a useful dataframe related to FERC Form 1 fuel information.

    We often want to pull information from the PUDL database that is
    not exactly what's contained in the table. This might include
    joining with other tables to get IDs for cross referencing, or doing
    some basic calculations to get e.g. heat rate or capacity factor.
    """
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

    # We have two different ways of assessing the total cost of fuel given cost
    # per unit delivered and cost per mmbtu. They *should* be the same, but we
    # know they aren't always. Calculate both so we can compare both.
    fuel_df['fuel_consumed_total_mmbtu'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_avg_mmbtu_per_unit']
    fuel_df['fuel_consumed_total_cost_mmbtu'] = \
        fuel_df['fuel_cost_per_mmbtu'] * fuel_df['fuel_consumed_total_mmbtu']
    fuel_df['fuel_consumed_total_cost_unit'] = \
        fuel_df['fuel_cost_per_unit_burned'] * fuel_df['fuel_qty_burned']

    return(fuel_df)


def simple_ferc_expenses(pudl_engine, min_capfac=0.6, min_corr=0.5):
    """
    Gather operating expense data for all simple FERC steam plants.

    Args:
        pudl_engine: a connection to the PUDL database.
        min_capfac: the minimum plant capacity factor to use in
            determining whether an expense category is a production or
            non-production cost.
        min_corr: The threhold correlation to use in determining whether an
            expense is a production or non-production expense. If an expense
            has a correlation to net generation that is greater than or equal
            to this threshold, it is categorized as a production expense.

    Returns:
        ferc1_expns_corr: A dictionary of expense categories
            and their correlations to the plant's net electricity
            generation.
        steam_df: a dataframe with all the operating expenses
            broken out for each simple FERC PUDL plant.
    """
    # All the simple FERC plants -- only one unit reported per PUDL ID:
    simple_ferc = simple_ferc1_plant_ids(pudl_engine)
    # All of the EIA PUDL plant IDs
    eia_pudl = eia_pudl_plant_ids(pudl_engine)
    # All of the large steam plants from FERC:
    steam_df = get_steam_ferc1_df(pudl_engine)

    # Calculate the dataset-wide expense correlations, for the record.
    expns_corrs = ferc1_expns_corr(steam_df, min_capfac=min_capfac)
    # Lump the operating expenses based on those correlations. Note that we
    # could also do this lumping after limiting the set of plants that we're
    # reporting on.  However, doing it based on the entire dataset seems more
    # appropriate, given that these correlations are properties of the fields,
    # not the plants... or so we hope.
    steam_df = consolidate_ferc1_expns(steam_df,
                                       min_capfac=min_capfac,
                                       min_corr=min_corr)

    # Limit the plants in the output to be those which are both simple FERC
    # plants, and appear in the EIA data.
    steam_df = steam_df[steam_df.plant_id_pudl.isin(simple_ferc.plant_id_pudl)]
    steam_df = steam_df[steam_df.plant_id_pudl.isin(eia_pudl.plant_id_pudl)]

    # Pass back both the expense correlations, and the plant data.
    return(expns_corrs, steam_df)


def get_gen_fuel_eia923_df(pudl_engine):
    """Pull a useful set of fields related to generation_fuel_eia923 table."""
    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    gf_eia923_select = sa.sql.select([
        pt['utilities_eia'].c.operator_id,
        pt['utilities_eia'].c.operator_name,
        pt['plants_eia'].c.plant_id_pudl,
        pt['plants_eia'].c.plant_name,
        pt['generation_fuel_eia923'].c.plant_id,
        pt['generation_fuel_eia923'].c.report_date,
        pt['generation_fuel_eia923'].c.aer_fuel_category,
        pt['generation_fuel_eia923'].c.fuel_consumed_total_mmbtu,
        pt['generation_fuel_eia923'].c.net_generation_mwh]).\
        where(sa.sql.and_(
            pt['plants_eia'].c.plant_id ==
            pt['generation_fuel_eia923'].c.plant_id,
            pt['util_plant_assn'].c.plant_id ==
            pt['plants_eia'].c.plant_id_pudl,
            pt['util_plant_assn'].c.utility_id ==
            pt['utilities_eia'].c.util_id_pudl
        ))

    return(pd.read_sql(gf_eia923_select, pudl_engine))


def get_frc_eia923_df(pudl_engine):
    """Pull a useful fields related to fuel_receipts_costs_eia923 table."""
    # Grab the list of tables so we can reference them shorthand.
    pt = models.PUDLBase.metadata.tables

    frc_eia923_select = sa.sql.select([
        pt['utilities_eia'].c.operator_id,
        pt['utilities_eia'].c.operator_name,
        pt['plants_eia'].c.plant_id_pudl,
        pt['plants_eia'].c.plant_name,
        pt['fuel_receipts_costs_eia923'].c.plant_id,
        pt['fuel_receipts_costs_eia923'].c.report_date,
        pt['fuel_receipts_costs_eia923'].c.fuel_group,
        pt['fuel_receipts_costs_eia923'].c.average_heat_content,
        pt['fuel_receipts_costs_eia923'].c.fuel_quantity,
        pt['fuel_receipts_costs_eia923'].c.fuel_cost_per_mmbtu]).\
        where(sa.sql.and_(
            pt['plants_eia'].c.plant_id ==
            pt['fuel_receipts_costs_eia923'].c.plant_id,
            pt['util_plant_assn'].c.plant_id ==
            pt['plants_eia'].c.plant_id_pudl,
            pt['util_plant_assn'].c.utility_id ==
            pt['utilities_eia'].c.util_id_pudl
        ))

    # There are some quantities that we want pre-calculated based on the
    # columns in the FRC table... let's just do it here so we don't end up
    # doing it over and over again in every goddamned notebook.
    frc_df = pd.read_sql(frc_eia923_select, pudl_engine)
    frc_df['total_heat_content_mmbtu'] = \
        frc_df['average_heat_content'] * frc_df['fuel_quantity']
    frc_df['total_fuel_cost'] = \
        frc_df['total_heat_content_mmbtu'] * frc_df['fuel_cost_per_mmbtu']

    # Standardize the fuel codes (need to fix this in DB ingest!!!)
    frc_df = frc_df.rename(columns={'fuel_group': 'fuel'})
    frc_df['fuel'] = frc_df.fuel.replace(
        to_replace=['Petroleum', 'Natural Gas', 'Coal'],
        value=['oil', 'gas', 'coal'])

    return(frc_df)


def fuel_ferc1_by_pudl(pudl_plant_ids,
                       fuels=['gas', 'oil', 'coal'],
                       cols=['fuel_consumed_total_mmbtu',
                             'fuel_consumed_total_cost_mmbtu',
                             'fuel_consumed_total_cost_unit']):
    """Aggregate FERC Form 1 fuel data by PUDL plant id and, optionally, fuel.

    Arguments:
        pudl_plant_ids: which PUDL plants should we retain for aggregation?
        fuels: Should the columns listed in cols be broken out by each
            individual fuel? If so, which fuels do we want totals for? If
            you want all fuels lumped together, pass in 'all'.
        cols: which columns from the fuel_ferc1 table should be summed.
    Returns:
        fuel_df: a dataframe with pudl_plant_id, year, and the summed values
            specified in cols. If fuels is not 'all' then it also has a column
            specifying fuel type.
    """
    fuel_df = get_fuel_ferc1_df()

    # Calculate the total fuel heat content for the plant by fuel
    fuel_df = fuel_df[fuel_df.plant_id_pudl.isin(pudl_plant_ids)]

    if (fuels == 'all'):
        cols_to_gb = ['plant_id_pudl', 'report_year']
    else:
        # Limit to records that pertain to our fuels of interest.
        fuel_df = fuel_df[fuel_df['fuel'].isin(fuels)]
        # Group by fuel as well, so we get individual fuel totals.
        cols_to_gb = ['plant_id_pudl', 'report_year', 'fuel']

    fuel_df = fuel_df.groupby(cols_to_gb)[cols].sum()
    fuel_df = fuel_df.reset_index()

    return(fuel_df)


def steam_ferc1_by_pudl(pudl_plant_ids, cols=['net_generation_mwh', ]):
    """Aggregate and return data from the steam_ferc1 table by pudl_plant_id.

    Arguments:
        pudl_plant_ids: A list of ids to include in the output.
        cols: The data columns that you want to aggregate and return.
    Returns:
        steam_df: A dataframe with columns for report_year, pudl_plant_id and
            cols, with the values in cols aggregated by plant and year.
    """
    steam_df = get_steam_ferc1_df()
    steam_df = steam_df[steam_df.plant_id_pudl.isin(pudl_plant_ids)]
    steam_df = steam_df.groupby(['plant_id_pudl', 'report_year'])[cols].sum()
    steam_df = steam_df.reset_index()

    return(steam_df)


def frc_by_pudl(pudl_plant_ids,
                fuels=['gas', 'oil', 'coal'],
                cols=['total_fuel_cost', ]):
    """
    Aggregate fuel_receipts_costs_eia923 table for comparison with FERC Form 1.

    In order to correlate informataion between EIA 923 and FERC Form 1, we need
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
        A dataframe with the sums of cols, as grouped by pudl ID, year, and
            (optionally) fuel.
    """
    # Get all the EIA info from generation_fuel_eia923
    frc_df = get_frc_eia923_df()
    # Limit just to the plants we're looking at
    frc_df = frc_df[frc_df.plant_id_pudl.isin(pudl_plant_ids)]
    # Just keep the columns we need for output:
    cols_to_keep = ['plant_id_pudl', 'report_date']
    cols_to_keep = cols_to_keep + cols
    cols_to_gb = [pd.TimeGrouper(freq='A'), 'plant_id_pudl']

    if (fuels != 'all'):
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

    return(frc_totals_df)


def gen_fuel_by_pudl(pudl_plant_ids,
                     fuels=['gas', 'oil', 'coal'],
                     cols=['fuel_consumed_total_mmbtu',
                           'net_generation_mwh']):
    """
    Aggregate generation_fuel_eia923 table for comparison with FERC Form 1.

    In order to correlate informataion between EIA 923 and FERC Form 1, we need
    to aggregate the EIA data annually, and potentially by fuel. This function
    groups generation_fuel_eia923 by pudl_plant_id, fuel, and year, and sums
    the columns of interest specified in cols, and returns a dataframe with
    the totals by pudl_plant_id, fuel, and year.

    Args:
        pudl_plant_ids: list of plant IDs to keep.
        fuels: list of fuel strings that we want to group by. Alternatively,
            this can be set to 'all' in which case fuel is not grouped by.
        cols: List of data columns which we are summing.
    Returns:
        A dataframe with the sums of cols, as grouped by pudl ID, year, and
            (optionally) fuel.
    """
    # Get all the EIA info from generation_fuel_eia923
    gf_df = get_gen_fuel_eia923_df()

    # Standardize the fuel codes (need to fix this in the DB!!!!)
    gf_df = gf_df.rename(columns={'aer_fuel_category': 'fuel'})
    gf_df['fuel'] = gf_df.fuel.replace(to_replace='petroleum', value='oil')

    # Select only the records that pertain to our target IDs
    gf_df = gf_df[gf_df.plant_id_pudl.isin(pudl_plant_ids)]

    cols_to_keep = ['plant_id_pudl', 'report_date']
    cols_to_keep = cols_to_keep + cols
    cols_to_gb = [pd.TimeGrouper(freq='A'), 'plant_id_pudl']

    if (fuels != 'all'):
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

    return(gf_totals_df)


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

    # Set report_date as a DatetimeIndex
    gf_df = gf_df.set_index(pd.DatetimeIndex(gf_df['report_date']))

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
    """Generate all partitions of a set having k elements."""
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
    it = iter(li)
    while True:
        nxt = list(itertools.islice(it, random.randint(min_chunk, max_chunk)))
        if nxt:
            yield nxt
        else:
            break


def zippertestdata(gens=50, max_group_size=6, samples=10,
                   noise=[0.10, 0.10, 0.10]):
    """Generate a test dataset for the datazipper, with known solutions.

    Args:
        gens (int): number of actual atomic units (analogous to generators)
            which may not fully enumerated in either dataset.
        max_group_size (int): Maximum number of atomic units which should
            be allowed to aggregate in the FERC groups.
        samples (int): How many samples should be available in each shared data
            series?
        noise (array): An array-like collection of numbers indicating the
            amount of noise (dispersion) to add between the two synthetic
            datasets. Larger numbers will result in lower correlations. The
            length of the noise array determines how many data series are
            created in the two synthetic datasets.

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

    # Make sure we've got enough plant IDs to work with:
    rpt = 1
    while(len(ascii_lowercase)**rpt < gens):
        rpt = rpt + 1

    # Generate the list of atomic generator IDs for both FERC (upper case) and
    # EIA (lower case) Using the same IDs across both datasets will make it
    # easy for us to tell whether we've correctly inferred the connections
    # between them.
    gen_ids_ferc = [''.join(s) for s in
                    itertools.product(ascii_uppercase, repeat=rpt)]
    gen_ids_ferc = gen_ids_ferc[:gens]
    gen_ids_eia = [''.join(s) for s in
                   itertools.product(ascii_lowercase, repeat=rpt)]
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
        for N in range(0, len(noise)):
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


def aggregate_by_pudl_plant(eia_df, ferc_df):
    """Create all possible candidate aggregations of EIA test data.

    The two input dataframes (eia_df and ferc_df) each contain several
    columns of corresponding synthetic data, with some amaount of noise added
    in to keep them from being identical. However, within the two dataframes,
    this data is aggregated differently.  Both dataframes have pudl_plant_id
    values, and the same data can be found within each pudl_plant_id.

    In eia_df, within each pudl_plant_id group there will be some number of
    individual generator units, each with a data value reported for each of the
    data columns, and its own unique alphabetical generator ID.

    In ferc_df, the full granularity is not available -- some lumping of the
    original generators has already been done, and the data associated with
    those lumps are the sums of the data which was originally associated with
    the individual generators which make up the lumps.

    This function generates all the possible lumpings of the fine-grained
    EIA data which have the same number of elements as the FERC data within the
    same pudl_plant_id group, and aggregates the data series within each of
    those possible lumpings so that the data associated with each possible
    collection of generators can be compared with the (already lumped) data
    associated with the FERC plants.

    The function returns a dataframe which contains all of the data from both
    datasets, with many copies of the FERC data, and a different candidate
    aggregation of the EIA data associated with each one. This can be a very
    big dataframe, if there are lots of generators, and lots of plant entities
    within some of the pudl_plant_id groups.
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


def correlate_by_generators(agg_df, eia_cols, ferc_cols, corr_cols):
    """Calculate EIA vs. FERC correlations for several data series.

    Given a dataframe output by aggregate_by_pudl_plant(), and lists of
    corresponding columns from the input EIA and FERC datasets, and for the
    output dataframe, calculate the correlations between every possible
    candidate lumping of EIA generators, and the existing FERC generating
    units for which data was supplied.

    The shared variables are indicated with eia_cols and ferc_cols
    which contain the names of columns that exist in both of the
    two data sources, which need to be correlated. E.g.
    'net_generation_mwh_eia' and 'net_generation_mwh_ferc'.

    Returns a dataframe containing the per-variable correlations,
    and a bunch of ID fields for grouping and joining on later.
    """
    index_cols = ['pudl_plant_id',
                  'ferc_plant_id',
                  'test_group_id',
                  'eia_gen_subgroup']

    gb = agg_df.groupby(index_cols)

    # We'll accumulate the various correlation results in this DF
    corrs = agg_df[index_cols].drop_duplicates()
    for eia_var, ferc_var, corr_var in zip(eia_cols, ferc_cols, corr_cols):
        # Calculate correlations between the two variables
        newcorr = gb[[eia_var, ferc_var]].corr().reset_index()
        # Need to eliminate extraneous correlation matrix elements.
        newcorr = newcorr.drop(ferc_var, axis=1)
        newcorr = newcorr[newcorr['level_4'] == ferc_var]
        newcorr = newcorr.drop('level_4', axis=1)
        newcorr = newcorr.rename(columns={eia_var: corr_var})
        corrs = corrs.merge(newcorr, on=index_cols)

    return(corrs)


def score_all(df, corr_cols, verbose=False):
    """Score candidate ensembles of EIA generators based on match to FERC.

    Given a datafram output from correlate_by_generators() above, containing
    correlations between potential EIA generator lumpings and the original
    FERC sub-plant groupings, generate all the possible ensembles of lumped
    EIA generators which have the same number of elements as the FERC
    plants we're trying to replicate, with all possible mappings between the
    EIA generator groups and the FERC generator groups.

    Then, calculate the mean correlations for all the data series for each
    entire candidate ensemble. Return a dataframe of winners, with the EIA
    and FERC plant IDs, the candidate_id, and the mean correlation of the
    entire candidate ensemble across all of the data series used to determine
    the mapping between the two data sources.

    Potential improvements:
      - Might need to be able to use a more general scoring function, rather
        than just taking the mean of all the correlations across all the data
        columns within a given candidate grouping. Is there a way to pass in
        an arbitrary number of columns associated with a group in a groupby
        object for array application? Doing two rounds of aggretation and
        mean() calculation seems dumb.
      - The generation of all the candidate groupings of generator ensembles
        is hella kludgy, and also slow -- it seems like there must be a less
        iterative, more vectorized way of doing the same thing. Depending on
        the number of permutations that need to be generated and tested in the
        real data, this may or may not be functional from a speed perspective.
      - Just for scale, assuming 100 generators, 10 data series, and 100
        samples in each data series, as we change the maximum group size
        (which determines both how large a PUDL plant can be, and how large the
        lumpings within a PUDL plant can be), the time to complete the tests
        increased as follows:
          - 5 => 20 seconds
          - 6 => 60 seconds
          - 7 => 150 seconds
          - 8 => 5000 seconds
      - Can this whole process be made more general, so that it can be used
        to zip together other more arbitrary datasets based on shared data
        fields? What would that look like? What additional parameters would
        we need to pass in?
      - Is there a good reason to keep this chain of functions separate, or
        should they be concatenated into one longer function? Are there more
        sensible ways to break the pipeline up?
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

    if(verbose):
        print('{} candidate generator ensembles identified.'.
              format(len(candidates_df)))
    candidates_df.candidate_id = candidates_df.candidate_id.astype(int)
    candidates_df = candidates_df.drop('test_group_id', axis=1)
    cand_gb = candidates_df.groupby(['pudl_plant_id', 'candidate_id'])

    cand_mean_corrs = cand_gb[corr_cols].mean()
    scored = pd.DataFrame(cand_mean_corrs.mean(axis=1),
                          columns=['mean_corr', ])

    idx = scored.groupby(['pudl_plant_id', ])['mean_corr'].\
        transform(max) == scored['mean_corr']

    winners = scored[idx].reset_index()
    winners = winners.merge(candidates_df,
                            how='left',
                            on=['pudl_plant_id', 'candidate_id'])
    winners = winners.drop(corr_cols, axis=1).\
        drop_duplicates(['eia_gen_subgroup', ])
    winners['success'] = \
        winners.eia_gen_subgroup == winners.ferc_plant_id.str.lower()

    return(winners)


def correlation_merge():
    """Merge two datasets based on specified shared data series."""
    # What fields do we need in the data frames to be merged? What's the
    # output that we're expecting?
