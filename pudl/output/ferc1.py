"""Functions for pulling FERC Form 1 data out of the PUDL DB."""

import sqlalchemy as sa
import pandas as pd

import pudl
import pudl.models.entities

pt = pudl.models.entities.PUDLBase.metadata.tables


def plants_utils_ferc1(testing=False):
    """Build a dataframe of useful FERC Plant & Utility information."""
    pudl_engine = pudl.init.connect_db(testing=testing)

    utils_ferc_tbl = pt['utilities_ferc']
    utils_ferc_select = sa.sql.select([utils_ferc_tbl, ])
    utils_ferc = pd.read_sql(utils_ferc_select, pudl_engine)

    plants_ferc_tbl = pt['plants_ferc']
    plants_ferc_select = sa.sql.select([plants_ferc_tbl, ])
    plants_ferc = pd.read_sql(plants_ferc_select, pudl_engine)

    out_df = pd.merge(plants_ferc, utils_ferc, on='utility_id_ferc1')
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
    pudl_engine = pudl.init.connect_db(testing=testing)
    steam_ferc1_tbl = pt['plants_steam_ferc1']
    steam_ferc1_select = sa.sql.select([steam_ferc1_tbl, ])
    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    pu_ferc = plants_utils_ferc1(testing=testing)

    out_df = pd.merge(steam_df, pu_ferc, on=['utility_id_ferc1', 'plant_name'])

    first_cols = [
        'report_year',
        'utility_id_ferc1',
        'utility_id_pudl',
        'utility_name_ferc1',
        'plant_id_pudl',
        'plant_id_ferc1',
        'plant_name'
    ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)

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
    pudl_engine = pudl.init.connect_db(testing=testing)
    fuel_ferc1_tbl = pt['fuel_ferc1']
    fuel_ferc1_select = sa.sql.select([fuel_ferc1_tbl, ])
    fuel_df = pd.read_sql(fuel_ferc1_select, pudl_engine)

    # In theory there are two different ways that we can figure out the total
    # cost of the fuel:
    #  * based on the cost per unit (e.g. ton) burned, and the total number
    #    of units burned,
    #  * based on the cost per mmbtu, the mmbtu per unit, and the total
    #    number of units burned.
    # In theory, these two unmbers should be the same, and they should both be
    # the same as the opex_fuel cost that is reported in the steam table, when
    # all the fuel costs for a given plant are added up across the different
    # fuels.  However, in practice, the simpler calculation based only on the
    # number of units burned and the cost per unit, gives a value that is more
    # consistent with the steam table value, so we will only calculate that
    # value for the outputs.
    fuel_df['fuel_consumed_mmbtu'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_mmbtu_per_unit']
    fuel_df['fuel_consumed_total_cost'] = \
        fuel_df['fuel_qty_burned'] * fuel_df['fuel_cost_per_unit_burned']

    pu_ferc = plants_utils_ferc1(testing=testing)

    out_df = pd.merge(fuel_df, pu_ferc, on=['utility_id_ferc1', 'plant_name'])
    out_df = out_df.drop('id', axis=1)

    first_cols = [
        'report_year',
        'utility_id_ferc1',
        'utility_id_pudl',
        'utility_name_ferc1',
        'plant_id_pudl',
        'plant_id_ferc1',
        'plant_name'
    ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)

    return out_df


def merge_ferc_fuel_steam():
    """
    FERC Form 1 reports various plant-level costs and other attributes in one
    table, and the associated fuel consumption in another table. In many cases
    it is very useful to have all of this information in a single table. For
    example:
     * The proportion of various fuels used by a plant is a strong indicator
       of which plant records should be associated with each other in the FERC
       data across years, so having that information available to the FERC
       plant ID generation algorithm is valuable.
     * Being able to filter plants based on the primary fuel that they consume
       for an analysis of, e.g., only coal or only gas plants.

    Fields we're working with in Fuel or both Steam/Fuel

    KEYS:
     * utility_id_ferc1 (KEY)
     * report_year (KEY)
     * plant_name (KEY)

    DROP:
     * record_id (DROP)
       - only for bookkeeping purposes
     * fuel_unit (DROP?)

    KEEP:
     * plant_id_ferc1 (KEEP but do not use)
       - eventually needs to be generated w/ contents of the merge in ETL

    CALCULATE PER-FUEL BEFORE PIVOT:
     * total fuel cost (for each fuel)
     * total fuel heat content (for each fuel)

    PIVOT on fuel_type_code_pudl

    CALCULATE AFTER PIVOT:
     * total fuel cost (for all fuels)
     * total fuel heat content (for all fuels)

    MERGE with steam on KEYS

    CALCULATE AFTER MERGE WITH STEAM:
     * total fuel cost per MWh (USE ONLY STEAM VALUES)
     * total fuel heat rate in MMBTU/MWh

    STEAM FIELDS:
     * opex_fuel (for comparison w/ fuel table costs)
     * net_generation_mwh (for heat rates, cost per MWh)
     * key fields as above

    ===========================
    What do we want in the end?
    ===========================
    ALL FUELS:
    * TOTAL heat content of fuel consumed
    * TOTAL cost of fuel consumed (check for consistency w/ steam table)
    * Overall heat rate (MMBTU/MWh)
    * Overall fuel cost per MWh

    BY FUEL:
    * Share of heat content
    * Share of fuel cost
    * Cost per MMBTU consumed
    """
