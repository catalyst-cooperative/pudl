"""Functions for pulling FERC Form 1 data out of the PUDL DB."""

import pandas as pd
import sqlalchemy as sa

import pudl


def plants_utils_ferc1(pudl_engine, pt):
    """
    Build a dataframe of useful FERC Plant & Utility information.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.
        pt (immutabledict): a sqlalchemy metadata dictionary of pudl tables

    Returns:
        pandas.DataFrame: A DataFrame containing useful FERC Form 1 Plant and
        Utility information.
    """
    utils_ferc_tbl = pt['utilities_ferc']
    utils_ferc_select = sa.sql.select([utils_ferc_tbl, ])
    utils_ferc = pd.read_sql(utils_ferc_select, pudl_engine)

    plants_ferc_tbl = pt['plants_ferc']
    plants_ferc_select = sa.sql.select([plants_ferc_tbl, ])
    plants_ferc = pd.read_sql(plants_ferc_select, pudl_engine)

    out_df = pd.merge(plants_ferc, utils_ferc, on='utility_id_ferc1')
    return out_df


def plants_steam_ferc1(pudl_engine, pt):
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.
        pt (immutabledict): a sqlalchemy metadata dictionary of pudl tables

    Returns:
        pandas.DataFrame: A DataFrame containing useful fields from the FERC
        Form 1 steam table.

    """
    steam_ferc1_tbl = pt['plants_steam_ferc1']
    steam_ferc1_select = sa.sql.select([steam_ferc1_tbl, ])
    steam_df = pd.read_sql(steam_ferc1_select, pudl_engine)

    pu_ferc = plants_utils_ferc1(pudl_engine, pt)

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


def fuel_ferc1(pudl_engine, pt):
    """Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant,
    allowing integration with other PUDL tables.

    Also calculates the total heat content consumed for each fuel, and the
    total cost for each fuel. Total cost is calculated in two different ways,
    on the basis of fuel units consumed(e.g. tons of coal, mcf of gas) and
    on the basis of heat content consumed. In theory these should give the
    same value for total cost, but this is not always the case.

    Todo:
        Check whether this includes all of the fuel_ferc1 fields...

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.
        pt (immutabledict): a sqlalchemy metadata dictionary of pudl tables

    Returns:
        pandas.DataFrame: A DataFrame containing useful FERC Form 1 fuel
        information.

    """
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

    pu_ferc = plants_utils_ferc1(pudl_engine, pt)

    out_df = pd.merge(fuel_df, pu_ferc, on=['utility_id_ferc1', 'plant_name'])
    out_df = out_df.drop('id', axis=1)

    first_cols = [
        'report_year',
        'utility_id_ferc1',
        'utility_id_pudl',
        'utility_name_ferc1',
        'plant_id_pudl',
        'plant_name'
    ]

    out_df = pudl.helpers.organize_cols(out_df, first_cols)

    return out_df


def fuel_by_plant_ferc1(pudl_engine, pt, thresh=0.5):
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around pudl.transform.ferc1.fuel_by_plant_ferc1
    which calculates some summary values on a per-plant basis (as indicated
    by utility_id_ferc1 and plant_name) related to fuel consumption.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.
        pt (immutabledict): a sqlalchemy metadata dictionary of pudl tables
        thresh (float): Minimum fraction of fuel (cost and mmbtu) required in
            order for a plant to be assigned a primary fuel. Must be between
            0.5 and 1.0. default value is 0.5.

    Returns:
        pandas.DataFrame: A DataFrame with fuel use summarized by plant.

    """
    first_cols = [
        'report_year',
        'utility_id_ferc1',
        'utility_id_pudl',
        'utility_name_ferc1',
        'plant_id_pudl',
        'plant_name'
    ]

    fbp_df = (
        pd.read_sql_table('fuel_ferc1', pudl_engine).
        drop(['id'], axis=1).
        pipe(pudl.transform.ferc1.fuel_by_plant_ferc1, thresh=thresh).
        merge(plants_utils_ferc1(pudl_engine, pt),
              on=['utility_id_ferc1', 'plant_name']).
        pipe(pudl.helpers.organize_cols, first_cols)
    )
    return fbp_df
