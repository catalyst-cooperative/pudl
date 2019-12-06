"""Functions for pulling FERC Form 1 data out of the PUDL DB."""

import pandas as pd

import pudl


def plants_utils_ferc1(pudl_engine):
    """
    Build a dataframe of useful FERC Plant & Utility information.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        pandas.DataFrame: A DataFrame containing useful FERC Form 1 Plant and
        Utility information.

    """
    pu_df = pd.merge(
        pd.read_sql("plants_ferc1", pudl_engine),
        pd.read_sql("utilities_ferc1", pudl_engine),
        on="utility_id_ferc1")
    return pu_df


def plants_steam_ferc1(pudl_engine):
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    Also calculates:
    * capacity_factor (based on net_generation_mwh and capacity_mw)

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        pandas.DataFrame: A DataFrame containing useful fields from the FERC
        Form 1 steam table.

    """
    steam_df = (
        pd.read_sql("plants_steam_ferc1", pudl_engine).
        drop('id', axis="columns").
        merge(plants_utils_ferc1(pudl_engine),
              on=['utility_id_ferc1', 'plant_name_ferc1']).
        assign(capacity_factor=lambda x: x.net_generation_mwh / (8760 * x.capacity_mw),
               opex_fuel_per_mwh=lambda x: x.opex_fuel / x.net_generation_mwh,
               opex_nonfuel_per_mwh=lambda x: (x.opex_production_total - x.opex_fuel) / x.net_generation_mwh).
        pipe(pudl.helpers.organize_cols, ['report_year',
                                          'utility_id_ferc1',
                                          'utility_id_pudl',
                                          'utility_name_ferc1',
                                          'plant_id_pudl',
                                          'plant_id_ferc1',
                                          'plant_name_ferc1'])
    )
    return steam_df


def fuel_ferc1(pudl_engine):
    """Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant,
    allowing integration with other PUDL tables.

    Useful derived values include:
    * fuel_consumed_mmbtu (total fuel heat content consumed)
    * fuel_consumed_total_cost (total cost of that fuel)

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        pandas.DataFrame: A DataFrame containing useful FERC Form 1 fuel
        information.

    """
    fuel_df = (
        pd.read_sql("fuel_ferc1", pudl_engine).
        drop('id', axis="columns").
        assign(fuel_consumed_mmbtu=lambda x: x["fuel_qty_burned"] * x["fuel_mmbtu_per_unit"],
               fuel_consumed_total_cost=lambda x: x["fuel_qty_burned"] * x["fuel_cost_per_unit_burned"]).
        merge(plants_utils_ferc1(pudl_engine),
              on=['utility_id_ferc1', 'plant_name_ferc1']).
        pipe(pudl.helpers.organize_cols, ['report_year',
                                          'utility_id_ferc1',
                                          'utility_id_pudl',
                                          'utility_name_ferc1',
                                          'plant_id_pudl',
                                          'plant_name_ferc1'])
    )
    return fuel_df


def fuel_by_plant_ferc1(pudl_engine, thresh=0.5):
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around pudl.transform.ferc1.fuel_by_plant_ferc1
    which calculates some summary values on a per-plant basis (as indicated
    by utility_id_ferc1 and plant_name_ferc1) related to fuel consumption.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.
        thresh (float): Minimum fraction of fuel (cost and mmbtu) required in
            order for a plant to be assigned a primary fuel. Must be between
            0.5 and 1.0. default value is 0.5.

    Returns:
        :mod:`pandas.DataFrame`: A DataFrame with fuel use summarized by plant.

    """
    fbp_df = (
        pd.read_sql_table('fuel_ferc1', pudl_engine).
        drop(['id'], axis="columns").
        pipe(pudl.transform.ferc1.fuel_by_plant_ferc1, thresh=thresh).
        merge(plants_utils_ferc1(pudl_engine),
              on=['utility_id_ferc1', 'plant_name_ferc1']).
        pipe(pudl.helpers.organize_cols, ['report_year',
                                          'utility_id_ferc1',
                                          'utility_id_pudl',
                                          'utility_name_ferc1',
                                          'plant_id_pudl',
                                          'plant_name_ferc1'])
    )
    return fbp_df
