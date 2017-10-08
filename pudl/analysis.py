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
from pudl import outputs


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


def simple_select_with_pudl_plant_id(table_name, pudl_engine):
    """
    Pull any PUDL table and inculde the PUDL Plant IDs

    Args:
        table_name: pudl table name
        pudl_engine

    Returns:
        DataFrame from table with PUDL IDs included

    """

    # Shorthand for readability... pt = PUDL Tables
    pt = models.PUDLBase.metadata.tables

    # Pull in the table
    tbl = pt[table_name]
    # Creates a sql Select object
    select = sa.sql.select([tbl, ])
    # Converts sql object to pandas dataframe
    table = pd.read_sql(select, pudl_engine)

    # Get the PUDL Plant ID
    plants_eia_tbl = pt['plants_eia']
    plants_eia_select = sa.sql.select([
        plants_eia_tbl.c.plant_id,
        plants_eia_tbl.c.plant_id_pudl,
    ])
    plants_eia = pd.read_sql(plants_eia_select, pudl_engine)
    out_df = pd.merge(table, plants_eia, how='left', on='plant_id')
    out_df.rename(columns={'plant_id': 'plant_id_eia'}, inplace=True)
    out_df.plant_id_pudl = out_df.plant_id_pudl.astype(int)
    return(out_df)


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
    """Generate a list of PUDL plant IDs that appear in both FERC and EIA."""
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
                   columns=['plant_id_eia',
                            'report_year', 'generator_id']):
    if 'report_date' in table.columns:
        table = table.set_index(pd.DatetimeIndex(table['report_date']).year)
        table.drop('report_date', axis=1, inplace=True)
        table.reset_index(inplace=True)
        table = table.rename(columns={'report_date': 'report_year'})
    gb = table.groupby(by=columns)
    return(gb.agg({sum_by: np.sum}))


def gens_with_bga(bga8, g9_summed, id_col='plant_id_eia'):
    """
    Label EIA generators with boiler generator association.

    Because there are missing generators in the bga table, without lumping all
    of the heat input and generation from these plants together, the heat rates
    were off. The vast majority of missing generators from the bga table seem
    to be the gas tubrine from combine cycle plants. This was generating heat
    rates for the steam generators alone, therefor much too low.
    """
    # All cenerators from the Boiler Generator Association table (860)
    gens8 = bga8.drop_duplicates(subset=[id_col, 'generator_id'])
    # All cenerators from the generation table (923)/
    gens9 = g9_summed.drop_duplicates(
        subset=[id_col, 'generator_id', 'report_year'])

    # See which generators are missing from the bga table
    gens = gens9.merge(gens8, on=[id_col, 'generator_id'], how="left")
    gens.boiler_id = gens.boiler_id.astype(str)
    gens['boiler_generator_assn'] = np.where(
        gens['boiler_id'] == 'nan', False, True)

    # Create a list of plants that include any generators that are not in the
    # bga table
    unassociated_plants = gens[gens['boiler_generator_assn'] == False].\
        drop_duplicates(subset=[id_col, 'report_year']).\
        drop(['generator_id', 'net_generation_mwh',
              'boiler_id', 'boiler_generator_assn'], axis=1)
    unassociated_plants['plant_assn'] = False

    # Using these unassociated_plants, lable all the generators that
    # are a part of plants that have generators that are not included
    # in the bga table
    gens = gens.merge(unassociated_plants, on=[
                      id_col, 'report_year'], how='left')
    gens['plant_assn'] = gens.plant_assn.fillna(value=True)

    # Using the associtated plants, extract the generator/boiler combos
    # that represent complete plants at any time to preserve
    # associations (i.e. if a coal plant had its boilers and generators
    # fully associated in the bga table in 2011 and then adds a
    # combined cycle plant the coal boiler/gen combo will be saved).
    gens_complete = gens[[id_col, 'generator_id',
                          'boiler_id', 'boiler_generator_assn', 'plant_assn']]
    gens_complete = \
        gens_complete[gens_complete['plant_assn'] == True].\
        drop_duplicates(subset=[id_col, 'generator_id', 'boiler_id'])
    gens_complete['complete_assn'] = True
    gens = gens.merge(gens_complete[[id_col,
                                     'generator_id',
                                     'boiler_id',
                                     'complete_assn']],
                      how='left',
                      on=[id_col, 'generator_id', 'boiler_id'])
    gens['complete_assn'] = gens.complete_assn.fillna(value=False)

    return(gens)


def heat_rate(bga8, g9_summed, bf9_summed,
              bf9_plant_summed, pudl_engine, id_col='plant_id_eia'):
    """
    Generate hate rates for all EIA generators.
    """
    # This section pulls the unassociated generators
    gens = gens_with_bga(bga8, g9_summed)
    # Get a list of generators from plants with unassociated plants
    # gens_unassn_plants = gens[gens['plant_assn'] == False
    gens_unassn_plants = gens[gens['complete_assn'] == False]

    # Sum the yearly net generation for these plants
    gup_gb = gens_unassn_plants.groupby(by=[id_col, 'report_year'])
    gens_unassn_plants_summed = gup_gb.agg({'net_generation_mwh': np.sum})
    gens_unassn_plants_summed.reset_index(inplace=True)

    # Pull in mmbtu
    unassn_plants = gens_unassn_plants_summed.merge(
        bf9_plant_summed, on=[id_col, 'report_year'])
    # calculate heat rate by plant
    unassn_plants['heat_rate_mmbtu_mwh'] = \
        unassn_plants['fuel_consumed_mmbtu'] / \
        unassn_plants['net_generation_mwh']

    # Merge these plant level heat heat rates with the unassociated generators
    # Assign heat rates to generators across the plants with unassociated
    # generators
    heat_rate_unassn = gens_unassn_plants.merge(unassn_plants[[
                                                id_col,
                                                'report_year',
                                                'heat_rate_mmbtu_mwh']],
                                                on=[id_col,
                                                    'report_year'],
                                                how='left')
    heat_rate_unassn.drop(
        ['boiler_id', 'boiler_generator_assn'], axis=1, inplace=True)

    # This section generates heat rate from the generators of
    # the plants that have any generators that are included in
    # the boiler generator association table (860)
    generation_w_boilers = g9_summed.merge(
        bga8, how='left', on=[id_col, 'generator_id'])

    # get net generation per boiler
    gb1 = generation_w_boilers.groupby(
        by=[id_col, 'report_year', 'boiler_id'])
    generation_w_boilers_summed = gb1.agg({'net_generation_mwh': np.sum})
    generation_w_boilers_summed.reset_index(inplace=True)
    generation_w_boilers_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler'},
        inplace=True)

    # get the generation per boiler/generator combo
    gb2 = generation_w_boilers.groupby(
        by=[id_col, 'report_year', 'boiler_id', 'generator_id'])
    generation_w_bg_summed = gb2.agg({'net_generation_mwh': np.sum})
    generation_w_bg_summed.reset_index(inplace=True)
    generation_w_bg_summed.rename(
        columns={'net_generation_mwh': 'net_generation_mwh_boiler_gen'},
        inplace=True)

    # squish them together
    generation_w_boilers_summed = \
        generation_w_boilers_summed.merge(generation_w_bg_summed,
                                          how='left',
                                          on=[id_col,
                                              'report_year',
                                              'boiler_id'])

    bg = bf9_summed.merge(bga8, how='left', on=[id_col, 'boiler_id'])
    bg = bg.merge(generation_w_boilers_summed, how='left', on=[
                  id_col, 'report_year', 'boiler_id', 'generator_id'])

    # Use the proportion of the generation of each generator to allot mmBTU
    bg['proportion_of_gen_by_boil_gen'] = \
        bg['net_generation_mwh_boiler_gen'] / bg['net_generation_mwh_boiler']
    bg['fuel_consumed_mmbtu_per_gen'] = \
        bg['proportion_of_gen_by_boil_gen'] * bg['fuel_consumed_mmbtu']

    # Get yearly fuel_consumed_mmbtu by plant_id, year and generator_id
    bg_gb = bg.groupby(by=[id_col,
                           'report_year',
                           'generator_id'])
    bg_summed = bg_gb.agg({'fuel_consumed_mmbtu_per_gen': np.sum})
    bg_summed.reset_index(inplace=True)

    # Calculate heat rate
    heat_rate = bg_summed.merge(g9_summed, how='left', on=[
                                id_col, 'report_year', 'generator_id'])
    heat_rate['heat_rate_mmbtu_mwh'] = \
        heat_rate['fuel_consumed_mmbtu_per_gen'] / \
        heat_rate['net_generation_mwh']

    # Importing the plant association tag to filter out the
    # generators that are a part of plants that aren't in the bga table
    heat_rate = heat_rate.merge(gens[[id_col,
                                      'report_year',
                                      'generator_id',
                                      'complete_assn',
                                      'plant_assn']],
                                on=[id_col,
                                    'report_year',
                                    'generator_id'])
    heat_rate_assn = heat_rate[heat_rate['complete_assn'] == True]

    # Now, let's chuck the incorrect (lower than 5 mmBTU/MWh)
    heat_rate = heat_rate[heat_rate['heat_rate_mmbtu_mwh'] >= 5]

    # Append heat rates for associated and unassociated
    heat_rate_all = heat_rate_assn.append(heat_rate_unassn)
    heat_rate_all.sort_values(
        by=[id_col, 'report_year', 'generator_id'], inplace=True)
    return(heat_rate_all)


def capacity_factor(g9_summed, g8, id_col='plant_id_eia'):
    """
    Generate capacity facotrs for all EIA generators.
    """
    # merge the generation and capacity to calculate capacity fazctor
    # plant_id should be specified as either plant_id_eia or plant_id_pudl
    capacity_factor = g9_summed.merge(g8,
                                      on=[id_col,
                                          'generator_id',
                                          'report_date'])
    capacity_factor['capacity_factor'] = \
        capacity_factor['net_generation_mwh'] / \
        (capacity_factor['nameplate_capacity_mw'] * 8760)

    # Replace unrealistic capacity factors with NaN: < 0 or > 1.5
    capacity_factor.loc[capacity_factor['capacity_factor']
                        < 0, 'capacity_factor'] = np.nan
    capacity_factor.loc[capacity_factor['capacity_factor']
                        >= 1.5, 'capacity_factor'] = np.nan

    return(capacity_factor)


def fuel_cost(g9_summed, g8_es, frc9_summed, heat_rate, id_col='plant_id_eia'):
    """Generate fuel cost for all EIA generators."""
    # Merge generation table with the generator table to include energy_source
    net_gen = g9_summed.merge(g8_es, how='left', on=[
                              id_col, 'generator_id'])
    # Merge this net_gen table with frc9_summed to have
    # fuel_cost_per_mmbtu_total associated with generators
    fuel_cost_per_mmbtu = net_gen.merge(frc9_summed,
                                        how='left',
                                        on=[id_col,
                                            'report_date',
                                            'energy_source'])

    fuel_cost = fuel_cost_per_mmbtu.merge(heat_rate[[id_col,
                                                     'report_date',
                                                     'generator_id',
                                                     'net_generation_mwh',
                                                     'heat_rate_mmbtu_mwh']],
                                          on=[id_col,
                                              'report_date',
                                              'generator_id',
                                              'net_generation_mwh'])

    # Calculate fuel cost per mwh using average fuel cost given year, plant,
    # fuel type; divide by generator-specific heat rate
    fuel_cost['fuel_cost_per_mwh'] = (fuel_cost['fuel_cost_per_mmbtu_average']
                                      * fuel_cost['heat_rate_mmbtu_mwh'])

    return(fuel_cost)


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


def ferc_expenses(pudl_engine, pudl_plant_ids=[], require_eia=True,
                  min_capfac=0.6, min_corr=0.5):
    """
    Gather operating expense data for a selection of FERC plants by PUDL ID.

    Args:
        pudl_engine: a connection to the PUDL database.
        pudl_plant_ids: list of PUDL plant IDs for which to pull expenses out
            of the FERC dataset. If it's an empty list, get all the plants.
        require_eia: Boolean (True/False). If True, then only return FERC
            plants which also appear in the EIA dataset.  Useful for when you
            want to merge the FERC expenses with other EIA data.
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
    # All of the large steam plants from FERC:
    steam_df = outputs.plants_steam_ferc1_df(pudl_engine)

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

    # If we are only looking at a specified subset of the FERC plants, then
    # here is where we limit the information that's returned:
    if len(pudl_plant_ids) > 0:
        steam_df = steam_df[steam_df.plant_id_pudl.isin(pudl_plant_ids)]

    if require_eia:
        # All of the EIA PUDL plant IDs
        eia_pudl = eia_pudl_plant_ids(pudl_engine)
        steam_df = steam_df[
            steam_df.plant_id_pudl.isin(eia_pudl.plant_id_pudl)]

    # Pass back both the expense correlations, and the plant data.
    return(expns_corrs, steam_df)


def fuel_ferc1_by_pudl(pudl_plant_ids, pudl_engine,
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
    fuel_df = outputs.fuel_ferc1_df(pudl_engine)

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


def steam_ferc1_by_pudl(pudl_plant_ids, pudl_engine,
                        cols=['net_generation_mwh', ]):
    """Aggregate and return data from the steam_ferc1 table by pudl_plant_id.

    Arguments:
        pudl_plant_ids: A list of ids to include in the output.
        cols: The data columns that you want to aggregate and return.
    Returns:
        steam_df: A dataframe with columns for report_year, pudl_plant_id and
            cols, with the values in cols aggregated by plant and year.
    """
    steam_df = outputs.plants_steam_ferc1_df(pudl_engine)
    steam_df = steam_df[steam_df.plant_id_pudl.isin(pudl_plant_ids)]
    steam_df = steam_df.groupby(['plant_id_pudl', 'report_year'])[cols].sum()
    steam_df = steam_df.reset_index()

    return(steam_df)


def frc_by_pudl(pudl_plant_ids, pudl_engine,
                fuels=['gas', 'oil', 'coal'],
                cols=['total_fuel_cost', ]):
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
        A dataframe with the sums of cols, as grouped by pudl ID, year, and
            (optionally) fuel.
    """
    # Get all the EIA info from generation_fuel_eia923
    frc_df = outputs.frc_eia923_df(pudl_engine)
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


def gen_fuel_by_pudl(pudl_plant_ids, pudl_engine,
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
    gf_df = outputs.gf_eia923_df(pudl_engine)

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


def generator_proportion_eia923(g, id_col='plant_id_eia'):
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
    g_yr = g.groupby([pd.TimeGrouper(freq='A'), id_col, 'generator_id'])
    # sum net_gen by year by plant
    g_net_generation_per_generator = pd.DataFrame(
        g_yr.net_generation_mwh.sum())
    g_net_generation_per_generator = \
        g_net_generation_per_generator.reset_index(level=['generator_id'])

    # groupby plant_id and by year
    g_net_generation_per_plant = g.groupby(
        [pd.TimeGrouper(freq='A'), id_col])
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


def plant_fuel_proportions_frc_eia923(frc_df, id_col='plant_id_eia'):
    """Calculate annual fuel proportions by plant from EIA923 fuel receipts."""
    frc_df = frc_df.copy()

    # Add a column with total fuel heat content per delivery
    frc_df['total_mmbtu'] = frc_df.fuel_quantity * frc_df.average_heat_content

    # Drop everything but report_date, plant_id, fuel_group, total_mmbtu
    frc_df = frc_df[['report_date', id_col, 'fuel_group', 'total_mmbtu']]

    # Group by report_date(annual), plant_id, fuel_group
    frc_gb = frc_df.groupby(
        [id_col, pd.TimeGrouper(freq='A'), 'fuel_group'])

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
        index=['year', id_col],
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
