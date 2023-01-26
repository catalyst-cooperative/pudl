"""Functions for pulling FERC Form 1 data out of the PUDL DB."""
import numpy as np
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.metadata.fields import apply_pudl_dtypes

logger = pudl.logging_helpers.get_logger(__name__)


def select_table_with_start_end_dates(table_select, start_date, end_date):
    """Augment a SQL table selection with start/end date restrictions."""
    if start_date is not None:
        table_select = table_select.where(
            table_select.columns.report_year >= f"{start_date.year}"
        )
    if end_date is not None:
        table_select = table_select.where(
            table_select.columns.report_year <= f"{end_date.year}"
        )
    return table_select


def select_table_with_start_end_dates2(table_name, pudl_engine, start_date, end_date):
    """Augment a SQL table selection with start/end date restrictions."""
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    table_select = sa.sql.select(pt[table_name])
    # table_cte = table_select.cte(table_name)
    if start_date is not None:
        table_select = table_select.where(
            table_select.columns.report_year >= start_date.year
        )
    if end_date is not None:
        table_select = table_select.where(
            table_select.columns.report_year <= end_date.year
        )
    return table_select


def plants_utils_ferc1(pudl_engine):
    """Build a dataframe of useful FERC Plant & Utility information.

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
        on="utility_id_ferc1",
    )
    return pu_df


def plants_steam_ferc1(pudl_engine, start_date, end_date):
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.

    Also calculates ``capacity_factor`` (based on ``net_generation_mwh`` &
    ``capacity_mw``)

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        pandas.DataFrame: A DataFrame containing useful fields from the FERC
        Form 1 steam table.
    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    steam_select = sa.sql.select(pt["plants_steam_ferc1"])
    steam_select = select_table_with_start_end_dates(steam_select, start_date, end_date)

    steam_df = (
        pd.read_sql(steam_select, pudl_engine)
        .merge(
            plants_utils_ferc1(pudl_engine),
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            capacity_factor=lambda x: x.net_generation_mwh / (8760 * x.capacity_mw),
            opex_fuel_per_mwh=lambda x: x.opex_fuel / x.net_generation_mwh,
            opex_total_nonfuel=lambda x: x.opex_production_total
            - x.opex_fuel.fillna(0),
            opex_nonfuel_per_mwh=lambda x: np.where(
                x.net_generation_mwh > 0,
                x.opex_total_nonfuel / x.net_generation_mwh,
                np.nan,
            ),
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_id_ferc1",
                "plant_name_ferc1",
            ],
        )
    )
    return steam_df


def fuel_ferc1(pudl_engine, start_date, end_date):
    """Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant,
    allowing integration with other PUDL tables.

    Useful derived values include:

    * ``fuel_consumed_mmbtu`` (total fuel heat content consumed)
    * ``fuel_consumed_total_cost`` (total cost of that fuel)

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        pandas.DataFrame: A DataFrame containing useful FERC Form 1 fuel
        information.
    """
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    fuel_select = sa.sql.select(pt["fuel_ferc1"])
    fuel_select = select_table_with_start_end_dates(fuel_select, start_date, end_date)
    fuel_df = (
        pd.read_sql(fuel_select, pudl_engine)
        .assign(
            fuel_consumed_mmbtu=lambda x: x["fuel_consumed_units"]
            * x["fuel_mmbtu_per_unit"],
            fuel_consumed_total_cost=lambda x: x["fuel_consumed_units"]
            * x["fuel_cost_per_unit_burned"],
        )
        .merge(
            plants_utils_ferc1(pudl_engine), on=["utility_id_ferc1", "plant_name_ferc1"]
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
            ],
        )
    )
    return fuel_df


def fuel_by_plant_ferc1(pudl_engine, start_date, end_date, thresh=0.5):
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around
    :func:`pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1`
    which calculates some summary values on a per-plant basis (as indicated
    by ``utility_id_ferc1`` and ``plant_name_ferc1``) related to fuel
    consumption.

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.
        thresh (float): Minimum fraction of fuel (cost and mmbtu) required in
            order for a plant to be assigned a primary fuel. Must be between
            0.5 and 1.0. default value is 0.5.

    Returns:
        pandas.DataFrame: A DataFrame with fuel use summarized by plant.
    """

    def drop_other_fuel_types(df):
        """Internal function to drop other fuel type.

        Fuel type other indicates we didn't know how to categorize the reported fuel
        type, which leads to records with incomplete and unsable data.
        """
        return df[df.fuel_type_code_pudl != "other"].copy()

    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    fuel_select = sa.sql.select(pt["fuel_ferc1"])
    fuel_select = select_table_with_start_end_dates(fuel_select, start_date, end_date)

    fuel_categories = list(
        pudl.transform.ferc1.FuelFerc1TableTransformer()
        .params.categorize_strings["fuel_type_code_pudl"]
        .categories.keys()
    )
    fbp_df = (
        pd.read_sql_table(fuel_select, pudl_engine)
        .pipe(drop_other_fuel_types)
        .pipe(
            pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1,
            fuel_categories=fuel_categories,
            thresh=thresh,
        )
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_float_nulls)
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_string_nulls)
        .merge(
            plants_utils_ferc1(pudl_engine), on=["utility_id_ferc1", "plant_name_ferc1"]
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
            ],
        )
    )
    return fbp_df


def plants_small_ferc1(pudl_engine, start_date, end_date):
    """Pull a useful dataframe related to the FERC Form 1 small plants."""
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    small_select = sa.sql.select(pt["plants_small_ferc1"])
    small_select = select_table_with_start_end_dates(small_select, start_date, end_date)
    plants_small_df = (
        pd.read_sql_table(small_select, pudl_engine)
        .merge(
            plants_utils_ferc1(pudl_engine),
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            opex_total=lambda x: (
                x[["opex_fuel", "opex_maintenance", "opex_operations"]]
                .fillna(0)
                .sum(axis=1)
            ),
            opex_total_nonfuel=lambda x: (x.opex_total - x.opex_fuel.fillna(0)),
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
                "record_id",
            ],
        )
    )
    return plants_small_df


def plants_hydro_ferc1(pudl_engine, start_date, end_date):
    """Pull a useful dataframe related to the FERC Form 1 hydro plants."""
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    hydro_select = sa.sql.select(pt["plants_hydro_ferc1"])
    hydro_select = select_table_with_start_end_dates(hydro_select, start_date, end_date)
    plants_hydro_df = (
        pd.read_sql_table(hydro_select, pudl_engine)
        .merge(
            plants_utils_ferc1(pudl_engine),
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            capacity_factor=lambda x: (x.net_generation_mwh / (8760 * x.capacity_mw)),
            opex_total_nonfuel=lambda x: x.opex_total,
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_name_ferc1",
                "record_id",
            ],
        )
    )
    return plants_hydro_df


def plants_pumped_storage_ferc1(pudl_engine, start_date, end_date):
    """Pull a dataframe of FERC Form 1 Pumped Storage plant data."""
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    pump_select = sa.sql.select(pt["plants_pumped_storage_ferc1"])
    pump_select = select_table_with_start_end_dates(pump_select, start_date, end_date)
    pumped_storage_df = (
        pd.read_sql_table(pump_select, pudl_engine)
        .merge(
            pudl.output.ferc1.plants_utils_ferc1(pudl_engine),
            on=["utility_id_ferc1", "plant_name_ferc1"],
            how="left",
        )
        .assign(
            capacity_factor=lambda x: x.net_generation_mwh / (8760 * x.capacity_mw),
            opex_total_nonfuel=lambda x: x.opex_total,
        )
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_name_ferc1",
                "record_id",
            ],
        )
    )
    return pumped_storage_df


def purchased_power_ferc1(pudl_engine, start_date, end_date):
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    purc_select = sa.sql.select(pt["purchased_power_ferc1"])
    purc_select = select_table_with_start_end_dates(purc_select, start_date, end_date)
    purchased_power_df = (
        pd.read_sql_table(purc_select, pudl_engine)
        .merge(pd.read_sql_table("utilities_ferc1", pudl_engine), on="utility_id_ferc1")
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "seller_name",
                "record_id",
            ],
        )
    )
    return purchased_power_df


def plant_in_service_ferc1(pudl_engine, start_date, end_date):
    """Pull a dataframe of FERC Form 1 Electric Plant in Service data."""
    pt = pudl.output.pudltabl.get_table_meta(pudl_engine)
    pis_select = sa.sql.select(pt["plant_in_service_ferc1"])
    pis_select = select_table_with_start_end_dates(pis_select, start_date, end_date)
    pis_df = (
        pd.read_sql_table(pis_select, pudl_engine)
        .merge(pd.read_sql_table("utilities_ferc1", pudl_engine), on="utility_id_ferc1")
        .pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
            ],
        )
    )
    return pis_df


def plants_all_ferc1(pudl_engine, start_date, end_date):
    """Combine the steam, small generators, hydro, and pumped storage tables.

    While this table may have many purposes, the main one is to prepare it for
    integration with the EIA Master Unit List (MUL). All subtables included in this
    output table must have pudl ids. Table prepping involves ensuring that the
    individual tables can merge correctly (like columns have the same name) both with
    each other and the EIA MUL.
    """
    steam_df = plants_steam_ferc1(pudl_engine, start_date, end_date)
    small_df = plants_small_ferc1(pudl_engine, start_date, end_date)
    hydro_df = plants_hydro_ferc1(pudl_engine, start_date, end_date)
    pump_df = plants_pumped_storage_ferc1(pudl_engine, start_date, end_date)

    # Prep steam table
    logger.debug("prepping steam table")
    steam_df = steam_df.rename(columns={"opex_plants": "opex_plant"}).pipe(
        apply_pudl_dtypes, group="ferc1"
    )

    # Prep hydro tables (Add this to the meta data later)
    logger.debug("prepping hydro tables")
    hydro_df = hydro_df.rename(columns={"project_num": "ferc_license_id"})
    pump_df = pump_df.rename(columns={"project_num": "ferc_license_id"})

    # Combine all the tables together
    logger.debug("combining all tables")
    all_df = (
        pd.concat([steam_df, small_df, hydro_df, pump_df])
        .rename(
            columns={
                "fuel_cost": "total_fuel_cost",
                "fuel_mmbtu": "total_mmbtu",
                "opex_fuel_per_mwh": "fuel_cost_per_mwh",
                "primary_fuel_by_mmbtu": "fuel_type_code_pudl",
            }
        )
        .replace({"": np.nan})
    )

    return all_df
