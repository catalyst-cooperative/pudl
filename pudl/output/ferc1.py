"""Functions for pulling FERC Form 1 data out of the PUDL DB."""

import sqlalchemy as sa
import pandas as pd

import pudl.models.entities
from pudl import init, helpers

pt = pudl.models.entities.PUDLBase.metadata.tables


def plants_utils_ferc1(testing=False):
    """Build a dataframe of useful FERC Plant & Utility information."""
    pudl_engine = init.connect_db(testing=testing)

    utils_ferc_tbl = pt['utilities_ferc']
    utils_ferc_select = sa.sql.select([utils_ferc_tbl, ])
    utils_ferc = pd.read_sql(utils_ferc_select, pudl_engine)

    plants_ferc_tbl = pt['plants_ferc']
    plants_ferc_select = sa.sql.select([plants_ferc_tbl, ])
    plants_ferc = pd.read_sql(plants_ferc_select, pudl_engine)

    out_df = pd.merge(plants_ferc, utils_ferc, on='respondent_id')
    return out_df


def plants_steam_ferc1(testing=False):
    """
    Select and join some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    Args:
    -----
    testing (bool) : True if we're using the pudl_test DB, False if we're
                     using the live PUDL DB.  False by default.

    Returns:
    --------
    steam_df : a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
    steam_ferc1_tbl = pt['plants_steam_ferc1']
    steam_ferc1_select = sa.sql.select([steam_ferc1_tbl, ])
    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    pu_ferc = plants_utils_ferc1(testing=testing)

    out_df = pd.merge(steam_df, pu_ferc, on=['respondent_id', 'plant_name'])

    first_cols = [
        'report_year',
        'respondent_id',
        'util_id_pudl',
        'respondent_name',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = helpers.organize_cols(out_df, first_cols)

    return out_df


def fuel_ferc1(testing=False):
    """
    Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant,
    allowing integration with other PUDL tables.

    Also calculates the total heat content consumed for each fuel, and the
    total cost for each fuel. Total cost is calculated in two different ways,
    on the basis of fuel units consumed (e.g. tons of coal, mcf of gas) and
    on the basis of heat content consumed. In theory these should give the
    same value for total cost, but this is not always the case.

    TODO: Check whether this includes all of the fuel_ferc1 fields...

    Args:
    -----
    testing (bool): True if we're using the pudl_test DB, False if we're
                    using the live PUDL DB.  False by default.

    Returns:
    --------
        fuel_df: a pandas dataframe.

    """
    pudl_engine = init.connect_db(testing=testing)
    fuel_ferc1_tbl = pt['fuel_ferc1']
    fuel_ferc1_select = sa.sql.select([fuel_ferc1_tbl, ])
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

    pu_ferc = plants_utils_ferc1(testing=testing)

    out_df = pd.merge(fuel_df, pu_ferc, on=['respondent_id', 'plant_name'])
    out_df = out_df.drop('id', axis=1)

    first_cols = [
        'report_year',
        'respondent_id',
        'util_id_pudl',
        'respondent_name',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = helpers.organize_cols(out_df, first_cols)

    return out_df
