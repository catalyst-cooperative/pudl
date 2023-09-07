"""A collection of denormalized FERC assets and helper functions."""
import importlib
import re
from typing import Literal, NamedTuple, Self

import networkx as nx
import numpy as np
import pandas as pd
from dagster import AssetIn, AssetsDefinition, Field, Mapping, asset
from matplotlib import pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from pandas._libs.missing import NAType as pandas_NAType
from pydantic import BaseModel, validator

import pudl

logger = pudl.logging_helpers.get_logger(__name__)

MAX_MULTIVALUE_WEIGHT_FRAC: dict[str, float] = {
    "income_statement_ferc1": 0.12,
    "balance_sheet_assets_ferc1": 0.02,
    "balance_sheet_liabilities_ferc1": 0.0,
}


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_utilities_ferc1(
    plants_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized table containing FERC plant and utility names and IDs."""
    return pd.merge(plants_ferc1, utilities_ferc1, on="utility_id_ferc1")


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_steam_ferc1(
    denorm_plants_utilities_ferc1: pd.DataFrame, plants_steam_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.
    Also calculates ``capacity_factor`` (based on ``net_generation_mwh`` &
    ``capacity_mw``)

    Args:
        denorm_plants_utilities_ferc1: Denormalized dataframe of FERC Form 1 plants and
            utilities data.
        plants_steam_ferc1: The normalized FERC Form 1 steam table.

    Returns:
        A DataFrame containing useful fields from the FERC Form 1 steam table.
    """
    steam_df = (
        plants_steam_ferc1.merge(
            denorm_plants_utilities_ferc1,
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
        .pipe(calc_annual_capital_additions_ferc1)
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


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_small_ferc1(
    plants_small_ferc1: pd.DataFrame, denorm_plants_utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 small plants."""
    plants_small_df = (
        plants_small_ferc1.merge(
            denorm_plants_utilities_ferc1,
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


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_hydro_ferc1(
    plants_hydro_ferc1: pd.DataFrame, denorm_plants_utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 hydro plants."""
    plants_hydro_df = (
        plants_hydro_ferc1.merge(
            denorm_plants_utilities_ferc1,
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


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_pumped_storage_ferc1(
    plants_pumped_storage_ferc1: pd.DataFrame,
    denorm_plants_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Pumped Storage plant data."""
    pumped_storage_df = (
        plants_pumped_storage_ferc1.merge(
            denorm_plants_utilities_ferc1,
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


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_fuel_ferc1(
    fuel_ferc1: pd.DataFrame, denorm_plants_utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant, allowing
    integration with other PUDL tables. Useful derived values include:

    * ``fuel_consumed_mmbtu`` (total fuel heat content consumed)
    * ``fuel_consumed_total_cost`` (total cost of that fuel)

    Args:
        pudl_engine (sqlalchemy.engine.Engine): Engine for connecting to the
            PUDL database.

    Returns:
        A DataFrame containing useful FERC Form 1 fuel
        information.
    """
    fuel_df = (
        fuel_ferc1.assign(
            fuel_consumed_mmbtu=lambda x: x["fuel_consumed_units"]
            * x["fuel_mmbtu_per_unit"],
            fuel_consumed_total_cost=lambda x: x["fuel_consumed_units"]
            * x["fuel_cost_per_unit_burned"],
        )
        .merge(
            denorm_plants_utilities_ferc1,
            on=["utility_id_ferc1", "plant_name_ferc1"],
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


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_purchased_power_ferc1(
    purchased_power_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    purchased_power_df = purchased_power_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
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
    return purchased_power_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plant_in_service_ferc1(
    plant_in_service_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Electric Plant in Service data."""
    pis_df = plant_in_service_ferc1.merge(utilities_ferc1, on="utility_id_ferc1").pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
        ],
    )
    return pis_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_balance_sheet_assets_ferc1(
    balance_sheet_assets_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 balance sheet assets data."""
    denorm_balance_sheet_assets_ferc1 = balance_sheet_assets_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "asset_type",
        ],
    )
    return denorm_balance_sheet_assets_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_balance_sheet_liabilities_ferc1(
    balance_sheet_liabilities_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 balance_sheet liabilities data."""
    denorm_balance_sheet_liabilities_ferc1 = balance_sheet_liabilities_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "liability_type",
        ],
    )
    return denorm_balance_sheet_liabilities_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_cash_flow_ferc1(
    cash_flow_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 cash flow data."""
    denorm_cash_flow_ferc1 = cash_flow_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "amount_type",
        ],
    )
    return denorm_cash_flow_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_depreciation_amortization_summary_ferc1(
    depreciation_amortization_summary_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 depreciation amortization data."""
    denorm_depreciation_amortization_summary_ferc1 = (
        depreciation_amortization_summary_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "plant_function",
                "ferc_account_label",
            ],
        )
    )
    return denorm_depreciation_amortization_summary_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_energy_dispositions_ferc1(
    electric_energy_dispositions_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 energy dispositions data."""
    denorm_electric_energy_dispositions_ferc1 = (
        electric_energy_dispositions_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "energy_disposition_type",
            ],
        )
    )
    return denorm_electric_energy_dispositions_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_energy_sources_ferc1(
    electric_energy_sources_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_energy_sources_ferc1 = electric_energy_sources_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "energy_source_type",
        ],
    )
    return denorm_electric_energy_sources_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_operating_expenses_ferc1(
    electric_operating_expenses_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_operating_expenses_ferc1 = electric_operating_expenses_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "expense_type",
        ],
    )
    return denorm_electric_operating_expenses_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_operating_revenues_ferc1(
    electric_operating_revenues_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_operating_revenues_ferc1 = electric_operating_revenues_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "revenue_type",
        ],
    )
    return denorm_electric_operating_revenues_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_plant_depreciation_changes_ferc1(
    electric_plant_depreciation_changes_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_plant_depreciation_changes_ferc1 = (
        electric_plant_depreciation_changes_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "depreciation_type",
                "plant_status",
                "utility_type",
            ],
        )
    )
    return denorm_electric_plant_depreciation_changes_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electric_plant_depreciation_functional_ferc1(
    electric_plant_depreciation_functional_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electric_plant_depreciation_functional_ferc1 = (
        electric_plant_depreciation_functional_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "plant_function",
                "plant_status",
                "utility_type",
            ],
        )
    )
    return denorm_electric_plant_depreciation_functional_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_electricity_sales_by_rate_schedule_ferc1(
    electricity_sales_by_rate_schedule_ferc1: pd.DataFrame,
    utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_electricity_sales_by_rate_schedule_ferc1 = (
        electricity_sales_by_rate_schedule_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
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
    return denorm_electricity_sales_by_rate_schedule_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_income_statement_ferc1(
    income_statement_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_income_statement_ferc1 = income_statement_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "utility_type",
            "income_type",
        ],
    )
    return denorm_income_statement_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_other_regulatory_liabilities_ferc1(
    other_regulatory_liabilities_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_other_regulatory_liabilities_ferc1 = (
        other_regulatory_liabilities_ferc1.merge(
            utilities_ferc1, on="utility_id_ferc1"
        ).pipe(
            pudl.helpers.organize_cols,
            [
                "report_year",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
            ],
        )
    )
    return denorm_other_regulatory_liabilities_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_retained_earnings_ferc1(
    retained_earnings_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_retained_earnings_ferc1 = retained_earnings_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "earnings_type",
        ],
    )
    return denorm_retained_earnings_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_transmission_statistics_ferc1(
    transmission_statistics_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_transmission_statistics_ferc1 = transmission_statistics_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
        ],
    )
    return denorm_transmission_statistics_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_utility_plant_summary_ferc1(
    utility_plant_summary_ferc1: pd.DataFrame, utilities_ferc1: pd.DataFrame
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    denorm_utility_plant_summary_ferc1 = utility_plant_summary_ferc1.merge(
        utilities_ferc1, on="utility_id_ferc1"
    ).pipe(
        pudl.helpers.organize_cols,
        [
            "report_year",
            "utility_id_ferc1",
            "utility_id_pudl",
            "utility_name_ferc1",
            "record_id",
            "utility_type",
            "utility_plant_asset_type",
        ],
    )
    return denorm_utility_plant_summary_ferc1


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_all_ferc1(
    denorm_plants_steam_ferc1: pd.DataFrame,
    denorm_plants_small_ferc1: pd.DataFrame,
    denorm_plants_hydro_ferc1: pd.DataFrame,
    denorm_plants_pumped_storage_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Combine the steam, small generators, hydro, and pumped storage tables.

    While this table may have many purposes, the main one is to prepare it for
    integration with the EIA Master Unit List (MUL). All subtables included in this
    output table must have pudl ids. Table prepping involves ensuring that the
    individual tables can merge correctly (like columns have the same name) both with
    each other and the EIA MUL.
    """
    # Prep steam table
    logger.debug("prepping steam table")
    steam_df = denorm_plants_steam_ferc1.rename(columns={"opex_plants": "opex_plant"})

    # Prep hydro tables (Add this to the meta data later)
    logger.debug("prepping hydro tables")
    hydro_df = denorm_plants_hydro_ferc1.rename(
        columns={"project_num": "ferc_license_id"}
    )
    pump_df = denorm_plants_pumped_storage_ferc1.rename(
        columns={"project_num": "ferc_license_id"}
    )

    # Combine all the tables together
    logger.debug("combining all tables")
    all_df = (
        pd.concat([steam_df, denorm_plants_small_ferc1, hydro_df, pump_df])
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


@asset(
    io_manager_key="pudl_sqlite_io_manager",
    config_schema={
        "thresh": Field(
            float,
            default_value=0.5,
            description=(
                "Minimum fraction of fuel (cost and mmbtu) required in order for a "
                "plant to be assigned a primary fuel. Must be between 0.5 and 1.0. "
                "Default value is 0.5."
            ),
        )
    },
    compute_kind="Python",
)
def denorm_fuel_by_plant_ferc1(
    context,
    fuel_ferc1: pd.DataFrame,
    denorm_plants_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around
    :func:`pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1`
    which calculates some summary values on a per-plant basis (as indicated
    by ``utility_id_ferc1`` and ``plant_name_ferc1``) related to fuel
    consumption.

    Args:
        context: Dagster context object
        fuel_ferc1: Normalized FERC fuel table.
        denorm_plants_utilities_ferc1: Denormalized table of FERC1 plant & utility IDs.

    Returns:
        A DataFrame with fuel use summarized by plant.
    """

    def drop_other_fuel_types(df):
        """Internal function to drop other fuel type.

        Fuel type other indicates we didn't know how to categorize the reported fuel
        type, which leads to records with incomplete and unsable data.
        """
        return df[df.fuel_type_code_pudl != "other"].copy()

    thresh = context.op_config["thresh"]
    # The existing function expects `fuel_type_code_pudl` to be an object, rather than
    # a category. This is a legacy of pre-dagster code, and we convert here to prevent
    # further retooling in the code-base.
    fuel_ferc1["fuel_type_code_pudl"] = fuel_ferc1["fuel_type_code_pudl"].astype(str)

    fuel_categories = list(
        pudl.transform.ferc1.FuelFerc1TableTransformer()
        .params.categorize_strings["fuel_type_code_pudl"]
        .categories.keys()
    )

    fbp_df = (
        fuel_ferc1.pipe(drop_other_fuel_types)
        .pipe(
            pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1,
            fuel_categories=fuel_categories,
            thresh=thresh,
        )
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_float_nulls)
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_string_nulls)
        .merge(
            denorm_plants_utilities_ferc1, on=["utility_id_ferc1", "plant_name_ferc1"]
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


###########################################################################
# HELPER FUNCTIONS
###########################################################################


def calc_annual_capital_additions_ferc1(
    steam_df: pd.DataFrame, window: int = 3
) -> pd.DataFrame:
    """Calculate annual capital additions for FERC1 steam records.

    Convert the capex_total column into annual capital additons the
    `capex_total` column is the cumulative capital poured into the plant over
    time. This function takes the annual difference should generate the annual
    capial additions. It also want generates a rolling average, to smooth out
    the big annual fluxuations.

    Args:
        steam_df: result of `prep_plants_ferc()`
        window: number of years for window to generate rolling average. Argument for
            :func:`pudl.helpers.generate_rolling_avg`

    Returns:
        Augemented version of steam_df with two additional columns:
        ``capex_annual_addition`` and ``capex_annual_addition_rolling``.
    """
    idx_steam_no_date = ["utility_id_ferc1", "plant_id_ferc1"]
    # we need to sort the df so it lines up w/ the groupby
    steam_df = steam_df.assign(
        report_date=lambda x: pd.to_datetime(x.report_year, format="%Y")
    ).sort_values(idx_steam_no_date + ["report_date"])
    steam_df = steam_df.assign(
        capex_wo_retirement_total=lambda x: x.capex_equipment.fillna(0)
        + x.capex_land.fillna(0)
        + x.capex_structures.fillna(0)
    )
    # we group on everything but the year so the groups are multi-year unique
    # plants the shift happens within these multi-year plant groups
    steam_df["capex_total_shifted"] = steam_df.groupby(idx_steam_no_date)[
        ["capex_wo_retirement_total"]
    ].shift()
    steam_df = steam_df.assign(
        capex_annual_addition=lambda x: x.capex_wo_retirement_total
        - x.capex_total_shifted
    )

    addts = pudl.helpers.generate_rolling_avg(
        steam_df,
        group_cols=idx_steam_no_date,
        data_col="capex_annual_addition",
        window=window,
    )
    steam_df_w_addts = pd.merge(
        steam_df,
        addts[
            idx_steam_no_date
            + [
                "report_date",
                "capex_wo_retirement_total",
                "capex_annual_addition_rolling",
            ]
        ],
        on=idx_steam_no_date + ["report_date", "capex_wo_retirement_total"],
        how="left",
    ).assign(
        capex_annual_per_mwh=lambda x: x.capex_annual_addition / x.net_generation_mwh,
        capex_annual_per_mw=lambda x: x.capex_annual_addition / x.capacity_mw,
        capex_annual_per_kw=lambda x: x.capex_annual_addition / x.capacity_mw / 1000,
        capex_annual_per_mwh_rolling=lambda x: x.capex_annual_addition_rolling
        / x.net_generation_mwh,
        capex_annual_per_mw_rolling=lambda x: x.capex_annual_addition_rolling
        / x.capacity_mw,
    )

    steam_df_w_addts = add_mean_cap_additions(steam_df_w_addts)
    # bb tests for volumne of negative annual capex
    neg_cap_addts = len(
        steam_df_w_addts[steam_df_w_addts.capex_annual_addition_rolling < 0]
    ) / len(steam_df_w_addts)
    neg_cap_addts_mw = (
        steam_df_w_addts[
            steam_df_w_addts.capex_annual_addition_rolling < 0
        ].net_generation_mwh.sum()
        / steam_df_w_addts.net_generation_mwh.sum()
    )
    message = (
        f"{neg_cap_addts:.02%} records have negative capitial additions"
        f": {neg_cap_addts_mw:.02%} of capacity"
    )
    if neg_cap_addts > 0.1:
        logger.warning(message)
    else:
        logger.info(message)
    return steam_df_w_addts.drop(
        columns=[
            "report_date",
            "capex_total_shifted",
            "capex_annual_addition_gen_mean",
            "capex_annual_addition_gen_std",
            "capex_annual_addition_diff_mean",
        ]
    )


def add_mean_cap_additions(steam_df):
    """Add mean capital additions over lifetime of plant."""
    idx_steam_no_date = ["utility_id_ferc1", "plant_id_ferc1"]
    gb_cap_an = steam_df.groupby(idx_steam_no_date)[["capex_annual_addition"]]
    # calcuate the standard deviatoin of each generator's capex over time
    df = (
        steam_df.merge(
            gb_cap_an.std()
            .add_suffix("_gen_std")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="m:1",
        )
        .merge(
            gb_cap_an.mean()
            .add_suffix("_gen_mean")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="m:1",
        )
        .assign(
            capex_annual_addition_diff_mean=lambda x: x.capex_annual_addition
            - x.capex_annual_addition_gen_mean,
        )
    )
    return df


#########
# Explode
#########
class NodeId(NamedTuple):
    """The primary keys which identify a node in a calculation tree.

    Since NodeId is just a :class:`NamedTuple` a list of NodeId instances can also be
    used to index into a :class:pandas.DataFrame` that uses these fields as its index.
    This is convenient since many :mod:`networkx` functions and methods return iterable
    containers of graph nodes, which can be turned into lists and used directly to index
    into dataframes.

    The additional dimensions (``utility_type``, ``plant_status``, and
    ``plant_function``) each have a small number of allowable values, which we could
    further impose as constraints on the values here using Pydantic if we wanted.
    """

    table_name: str
    xbrl_factoid: str
    utility_type: str | pandas_NAType
    plant_status: str | pandas_NAType
    plant_function: str | pandas_NAType


@asset
def _out_tags_ferc1(table_dimensions_ferc1) -> pd.DataFrame:
    """Grab the stored table of tags and add infered dimension."""
    # NOTE: there are a bunch of duplicate records in xbrl_factoid_rate_base_tags.csv
    # Also, these tags are only applicable to the balance_sheet_assets_ferc1 table, but
    # we need to pass in a dataframe with the right structure to all of the exploders,
    # so we're just re-using this one for the moment.
    tags_csv = (
        importlib.resources.files("pudl.package_data.ferc1")
        / "xbrl_factoid_rate_base_tags.csv"
    )
    tags_df = (
        pd.read_csv(
            tags_csv,
            usecols=[
                "table_name",
                "xbrl_factoid",
                "in_rate_base",
                "utility_type",
                "plant_function",
                "plant_status",
            ],
        )
        .drop_duplicates()
        .dropna(subset=["table_name", "xbrl_factoid"], how="any")
        .pipe(
            pudl.transform.ferc1.make_calculation_dimensions_explicit,
            table_dimensions_ferc1,
            dimensions=["utility_type", "plant_function", "plant_status"],
        )
        .astype(pd.StringDtype())
    )
    return tags_df


def exploded_table_asset_factory(
    root_table: str,
    table_names_to_explode: list[str],
    seed_nodes: list[NodeId] = [],
    calculation_tolerance: float = 0.05,
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """Create an exploded table based on a set of related input tables."""
    ins: Mapping[str, AssetIn] = {
        "metadata_xbrl_ferc1": AssetIn("metadata_xbrl_ferc1"),
        "calculation_components_xbrl_ferc1": AssetIn(
            "calculation_components_xbrl_ferc1"
        ),
        "_out_tags_ferc1": AssetIn("_out_tags_ferc1"),
    }
    ins |= {table_name: AssetIn(table_name) for table_name in table_names_to_explode}

    @asset(name=f"exploded_{root_table}", ins=ins, io_manager_key=io_manager_key)
    def exploded_tables_asset(
        **kwargs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        metadata_xbrl_ferc1 = kwargs["metadata_xbrl_ferc1"]
        calculation_components_xbrl_ferc1 = kwargs["calculation_components_xbrl_ferc1"]
        tags = kwargs["_out_tags_ferc1"]
        tables_to_explode = {
            name: df
            for (name, df) in kwargs.items()
            if name
            not in [
                "metadata_xbrl_ferc1",
                "calculation_components_xbrl_ferc1",
                "_out_tags_ferc1",
            ]
        }
        return Exploder(
            table_names=tables_to_explode.keys(),
            root_table=root_table,
            metadata_xbrl_ferc1=metadata_xbrl_ferc1,
            calculation_components_xbrl_ferc1=calculation_components_xbrl_ferc1,
            seed_nodes=seed_nodes,
            tags=tags,
        ).boom(
            tables_to_explode=tables_to_explode,
            calculation_tolerance=calculation_tolerance,
        )

    return exploded_tables_asset


def create_exploded_table_assets() -> list[AssetsDefinition]:
    """Create a list of exploded FERC Form 1 assets.

    Returns:
        A list of :class:`AssetsDefinitions` where each asset is an exploded FERC Form 1
        table.
    """
    explosion_args = [
        {
            "root_table": "income_statement_ferc1",
            "table_names_to_explode": [
                "income_statement_ferc1",
                "depreciation_amortization_summary_ferc1",
                "electric_operating_expenses_ferc1",
                "electric_operating_revenues_ferc1",
            ],
            "calculation_tolerance": 0.12,
            "seed_nodes": [
                NodeId(
                    table_name="income_statement_ferc1",
                    xbrl_factoid="net_income_loss",
                    utility_type="total",
                    plant_status=pd.NA,
                    plant_function=pd.NA,
                ),
            ],
        },
        {
            "root_table": "balance_sheet_assets_ferc1",
            "table_names_to_explode": [
                "balance_sheet_assets_ferc1",
                "balance_sheet_assets_ferc1",
                "utility_plant_summary_ferc1",
                "plant_in_service_ferc1",
                "electric_plant_depreciation_functional_ferc1",
            ],
            "calculation_tolerance": 0.55,
            "seed_nodes": [
                NodeId(
                    table_name="balance_sheet_assets_ferc1",
                    xbrl_factoid="assets_and_other_debits",
                    utility_type=pd.NA,
                    plant_status=pd.NA,
                    plant_function=pd.NA,
                )
            ],
        },
        {
            "root_table": "balance_sheet_liabilities_ferc1",
            "table_names_to_explode": [
                "balance_sheet_liabilities_ferc1",
                "balance_sheet_liabilities_ferc1",
                "retained_earnings_ferc1",
            ],
            "calculation_tolerance": 0.07,
            "seed_nodes": [
                NodeId(
                    table_name="balance_sheet_liabilities_ferc1",
                    xbrl_factoid="liabilities_and_other_credits",
                    utility_type=pd.NA,
                    plant_status=pd.NA,
                    plant_function=pd.NA,
                )
            ],
        },
    ]
    return [exploded_table_asset_factory(**kwargs) for kwargs in explosion_args]


exploded_ferc1_assets = create_exploded_table_assets()


class MetadataExploder:
    """Combine a set of inter-related, nested table's metadata."""

    def __init__(
        self,
        table_names: list[str],
        metadata_xbrl_ferc1: pd.DataFrame,
        calculation_components_xbrl_ferc1: pd.DataFrame,
    ):
        """Instantiate MetadataExploder."""
        self.table_names = table_names
        self.calculation_components_xbrl_ferc1 = calculation_components_xbrl_ferc1
        self.metadata_xbrl_ferc1 = metadata_xbrl_ferc1

    @property
    def calculations(self: Self):
        """Remove any calculation components that aren't relevant to the explosion.

        At the end of this process several things should be true:

        - Only parents with table_name in the explosion tables should be retained.
        - All calculations in which *any* components were outside of the tables in
          the explosion should be turned into leaves -- i.e. they should be replaced
          with a single calculation component filled with NA values.
        - Every remaining calculation component must also appear as a parent (if it
          is a leaf, then it will have a single null calculation component)
        - There should be no records where only one of table_name or xbrl_factoid
          are null. They should either both or neither be null.
        - table_name_parent and xbrl_factoid_parent should be non-null.
        """
        calc_cols = list(NodeId._fields)
        parent_cols = [col + "_parent" for col in calc_cols]

        # Keep only records where both parent and child facts are in exploded tables:
        calc_explode = self.calculation_components_xbrl_ferc1[
            self.calculation_components_xbrl_ferc1.table_name_parent.isin(
                self.table_names
            )
        ].copy()
        # Groupby parent factoids
        gb = calc_explode.groupby(
            ["table_name_parent", "xbrl_factoid_parent"], as_index=False
        )
        # Identify groups where **all** calculation components are within the explosion
        calc_explode["is_in_explosion"] = gb.table_name.transform(
            lambda x: x.isin(self.table_names).all()
        )
        # Keep only calculations in which ALL calculation components are in explosion
        # Restrict columns to the ones we actually need. Drop duplicates and order
        # things for legibility.
        calc_explode = (
            calc_explode[calc_explode.is_in_explosion]
            .loc[:, parent_cols + calc_cols + ["weight", "is_within_table_calc"]]
            .drop_duplicates()
            .set_index(parent_cols + calc_cols)
            .sort_index()
            .reset_index()
        )

        ##############################################################################
        # Everything below here is error checking / debugging / temporary fixes
        dupes = calc_explode.duplicated(subset=parent_cols + calc_cols, keep=False)
        if dupes.any():
            logger.warning(
                "Consolidating non-unique associations found in exploded_calcs:\n"
                f"{calc_explode.loc[dupes]}"
            )
        # Drop all duplicates with null weights -- this is a temporary fix to an issue
        # from upstream.
        # TODO: remove once things are fixed in calculation_components_xbrl_ferc1
        calc_explode = calc_explode.loc[~(dupes & calc_explode.weight.isna())]
        assert not calc_explode.duplicated(
            subset=parent_cols + calc_cols, keep=False
        ).any()

        # There should be no cases where only one of table_name or xbrl_factoid is NA:
        partially_null = calc_explode[
            calc_explode.table_name.isna() ^ calc_explode.xbrl_factoid.isna()
        ]
        if not partially_null.empty:
            logger.error(
                "Found unacceptably null calculation components. Dropping!\n"
                f"{partially_null[['table_name', 'xbrl_factoid']]}"
            )
            calc_explode = calc_explode.drop(index=partially_null)

        # These should never be NA:
        assert calc_explode.xbrl_factoid_parent.notna().all()
        assert calc_explode.table_name_parent.notna().all()

        return calc_explode

    @property
    def metadata(self):
        """Combine a set of interrelated table's metatada for use in :class:`Exploder`.

        Any calculations containing components that are part of tables outside the
        set of exploded tables will be converted to reported values with an empty
        calculation. Then we verify that all referenced calculation components actually
        appear as their own records within the concatenated metadata dataframe.

        Args:
            clean_xbrl_metadata_json: cleaned XRBL metadata.
        """
        calc_cols = list(NodeId._fields)
        exploded_metadata = (
            self.metadata_xbrl_ferc1[
                self.metadata_xbrl_ferc1.table_name.isin(self.table_names)
            ]
            .set_index(calc_cols)
            .sort_index()
            .reset_index()
        )
        # At this point all remaining calculation components should exist within the
        # exploded metadata.
        calc_comps = self.calculations
        missing_from_calcs_idx = calc_comps.set_index(calc_cols).index.difference(
            calc_comps.set_index(calc_cols).index
        )
        assert missing_from_calcs_idx.empty
        return exploded_metadata


class Exploder:
    """Get unique, granular datapoints from a set of related, nested FERC1 tables."""

    def __init__(
        self: Self,
        table_names: list[str],
        root_table: str,
        metadata_xbrl_ferc1: pd.DataFrame,
        calculation_components_xbrl_ferc1: pd.DataFrame,
        seed_nodes: list[NodeId] = [],
        tags: pd.DataFrame = pd.DataFrame(),
    ):
        """Instantiate an Exploder class.

        Args:
            table_names: list of table names to explode.
            root_table: the table at the base of the tree of tables_to_explode.
            metadata_xbrl_ferc1: table of factoid-level metadata.
            calculation_components_xbrl_ferc1: table of calculation components.
            seed_nodes: NodeIds to use as seeds for the calculation forest.
            tags: Additional metadata to merge onto the exploded dataframe.
        """
        self.table_names: list[str] = table_names
        self.root_table: str = root_table
        self.meta_exploder = MetadataExploder(
            self.table_names,
            metadata_xbrl_ferc1,
            calculation_components_xbrl_ferc1,
        )
        self.metadata_exploded: pd.DataFrame = self.meta_exploder.metadata
        self.calculations_exploded: pd.DataFrame = self.meta_exploder.calculations

        # If we don't get any explicit seed nodes, use all nodes from the root table
        # that have calculations associated with them:
        if len(seed_nodes) == 0:
            logger.info(
                "No seeds provided. Using all calculated nodes in root table: "
                f"{self.root_table}"
            )
            seed_nodes = [
                NodeId(seed)
                for seed in self.metadata_exploded[
                    (self.metadata_exploded.table_name == self.root_table)
                    & (self.metadata_exploded.calculations != "[]")
                ]
                .set_index(["table_name", "xbrl_factoid"])
                .index
            ]
            logger.info(f"Identified {seed_nodes=}")
        self.seed_nodes = seed_nodes
        self.tags = tags

    @property
    def calculation_forest(self: Self) -> "XbrlCalculationForestFerc1":
        """Construct a calculation forest based on class attributes."""
        return XbrlCalculationForestFerc1(
            exploded_calcs=self.calculations_exploded,
            exploded_meta=self.metadata_exploded,
            seeds=self.seed_nodes,
            tags=self.tags,
        )

    @property
    def other_dimensions(self) -> list[str]:
        """Get all of the column names for the other dimensions."""
        return pudl.transform.ferc1.other_dimensions(table_names=self.table_names)

    @property
    def exploded_pks(self) -> list[str]:
        """Get the joint primary keys of the exploded tables."""
        pks = []
        for table_name in self.table_names:
            xbrl_factoid_name = pudl.transform.ferc1.table_to_xbrl_factoid_name()[
                table_name
            ]
            pks.append(
                [
                    col
                    for col in pudl.metadata.classes.Resource.from_id(
                        table_name
                    ).schema.primary_key
                    if col != xbrl_factoid_name
                ]
            )
        # Some xbrl_factoid names are the same in more than one table, so we also add
        # table_name here.
        pks = [
            "table_name",
            "xbrl_factoid",
        ] + pudl.helpers.dedupe_n_flatten_list_of_lists(pks)
        return pks

    @property
    def value_col(self) -> str:
        """Get the value column for the exploded tables."""
        value_cols = []
        for table_name in self.table_names:
            value_cols.append(
                pudl.transform.ferc1.FERC1_TFR_CLASSES[
                    table_name
                ]().params.reconcile_table_calculations.column_to_check
            )
        if len(set(value_cols)) != 1:
            raise ValueError(
                "Exploding FERC tables requires tables with only one value column. Got: "
                f"{set(value_cols)}"
            )
        value_col = list(set(value_cols))[0]
        return value_col

    def boom(
        self,
        tables_to_explode: dict[str, pd.DataFrame],
        calculation_tolerance: float = 0.05,
    ) -> pd.DataFrame:
        """Explode a set of nested tables.

        There are five main stages of this process:

        #. Prep all of the individual tables for explosion.
        #. Concatenate all of the tabels together.
        #. Remove duplication in the concatenated exploded table.
        #. Annotate the fine-grained data with additional metadata.
        #. Validate that calculated top-level values are correct. (not implemented)

        Args:
            tables_to_explode: dictionary of table name (key) to transfomed table (value).
            calculation_tolerance: What proportion (0-1) of calculated values are
              allowed to be incorrect without raising an AssertionError.
        """
        exploded = (
            self.initial_explosion_concatenation(tables_to_explode)
            .pipe(self.generate_intertable_calculations)
            .pipe(self.reconcile_intertable_calculations, calculation_tolerance)
            .pipe(self.calculation_forest.leafy_data, value_col=self.value_col)
        )
        # Identify which columns should be kept in the output...
        # TODO: Define schema for the tables explicitly.
        cols_to_keep = list(
            set(self.exploded_pks + self.other_dimensions + [self.value_col])
        )
        if ("utility_type" in cols_to_keep) and (
            "utility_type_other" in exploded.columns
        ):
            cols_to_keep += ["utility_type_other"]
        cols_to_keep += exploded.filter(regex="tags.*").columns.to_list()
        cols_to_keep += [
            "ferc_account",
            "row_type_xbrl",
        ]

        # TODO: Validate the root node calculations.
        # Verify that we get the same values for the root nodes using only the input
        # data from the leaf nodes:
        # root_calcs = self.calculation_forest.root_calculations
        return exploded[cols_to_keep].convert_dtypes()

    def initial_explosion_concatenation(
        self, tables_to_explode: dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Concatenate all of the tables for the explosion.

        Merge in some basic pieces of the each table's metadata and add ``table_name``.
        At this point in the explosion, there will be a lot of duplicaiton in the
        output.
        """
        logger.info("Explode: CONCAT!")
        explosion_tables = []
        # GRAB/PREP EACH TABLE
        for table_name, table_df in tables_to_explode.items():
            xbrl_factoid_name = pudl.transform.ferc1.FERC1_TFR_CLASSES[
                table_name
            ]().params.xbrl_factoid_name
            tbl = table_df.assign(table_name=table_name).rename(
                columns={xbrl_factoid_name: "xbrl_factoid"}
            )
            explosion_tables.append(tbl)

        exploded = pd.concat(explosion_tables)

        # Identify which dimensions apply to the curent explosion -- not all collections
        # of tables have all dimensions.
        meta_idx = list(NodeId._fields)
        missing_dims = list(set(meta_idx).difference(exploded.columns))
        # Missing dimensions SHOULD be entirely null in the metadata, if so we can drop
        if not self.metadata_exploded.loc[:, missing_dims].isna().all(axis=None):
            raise AssertionError(
                f"Expected missing metadata dimensions {missing_dims} to be null."
            )
        metadata_exploded = self.metadata_exploded.drop(columns=missing_dims)
        meta_idx = list(set(meta_idx).difference(missing_dims))

        # drop any metadata columns that appear in the data tables, because we may have
        # edited them in the metadata table, and want the edited version to take
        # precedence
        cols_to_keep = list(
            set(exploded.columns).difference(metadata_exploded.columns).union(meta_idx)
        )
        exploded = pd.merge(
            left=exploded.loc[:, cols_to_keep],
            right=metadata_exploded,
            how="left",
            on=meta_idx,
            validate="m:1",
        )
        return exploded

    def generate_intertable_calculations(
        self: Self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Generate calculated values for inter-table calculated factoids.

        This function sums components of calculations for a given factoid when the
        components originate entirely or partially outside of the table. It also
        accounts for components that only sum to a factoid within a particular dimension
        (e.g., for an electric utility or for plants whose plant_function is
        "in_service"). This returns a dataframe with a "calculated_amount" column.

        Args:
            exploded: concatenated tables for table explosion.
        """
        calculations_intertable = self.calculations_exploded[
            ~self.calculations_exploded.is_within_table_calc
        ]
        if calculations_intertable.empty:
            return exploded
        else:
            logger.info(
                f"{self.root_table}: Reconcile inter-table calculations: "
                f"{list(calculations_intertable.xbrl_factoid.unique())}."
            )
        # compile the lists of columns we are going to use later
        calc_component_idx = ["table_name", "xbrl_factoid"] + self.other_dimensions
        # Merge the reported data and the calculation component metadata to enable
        # validation of calculated values. Here the data table exploded is supplying the
        # values associated with individual calculation components, and the table_name
        # and xbrl_factoid to which we aggregate are coming from the calculation
        # components table. After merging we use the weights to adjust the reported
        # values so they can be summed directly. This gives us aggregated calculated
        # values that can later be compared to the higher level reported values.

        # the validation is one_many in all instances expect for the xbrl_factoid
        # construction_work_in_progress in the balance_sheet_assets_ferc1 explosion.
        # this may be a problem in the calculations that we should track down in #2717
        validate = (
            "one_to_many"
            if self.root_table != "balance_sheet_assets_ferc1"
            else "many_to_many"
        )
        # we are going to merge the data onto the calc components with the _parent
        # column names, so the groupby after the merge needs a set of by cols with the
        # _parent suffix
        meta_idx = [col for col in list(NodeId._fields) if col in self.exploded_pks]
        gby_parent = [
            f"{col}_parent" if col in meta_idx else col for col in self.exploded_pks
        ]
        calc_df = (
            pd.merge(
                calculations_intertable,
                exploded,
                validate=validate,
                on=calc_component_idx,
            )
            # apply the weight from the calc to convey the sign before summing.
            .assign(calculated_amount=lambda x: x[self.value_col] * x.weight)
            .groupby(gby_parent, as_index=False, dropna=False)[["calculated_amount"]]
            .sum(min_count=1)
        )
        # remove the _parent suffix so we can merge these calculated values back onto
        # the data using the original pks
        calc_df.columns = calc_df.columns.str.removesuffix("_parent")
        calculated_df = pd.merge(
            exploded,
            calc_df,
            on=self.exploded_pks,
            how="outer",
            validate="1:1",
            indicator=True,
        )

        assert calculated_df[
            (calculated_df._merge == "right_only")
            & (calculated_df[self.value_col].notnull())
        ].empty

        calculated_df = calculated_df.drop(columns=["_merge"])
        # Force value_col to be a float to prevent any hijinks with calculating differences.
        calculated_df[self.value_col] = calculated_df[self.value_col].astype(float)

        return calculated_df

    def reconcile_intertable_calculations(
        self: Self, calculated_df: pd.DataFrame, calculation_tolerance: float = 0.05
    ):
        """Ensure inter-table calculated values match reported values within a tolerance.

        In addition to checking whether all reported "calculated" values match the output
        of our repaired calculations, this function adds a correction record to the
        dataframe that is included in the calculations so that after the fact the
        calculations match exactly. This is only done when the fraction of records that
        don't match within the tolerances of :func:`numpy.isclose` is below a set
        threshold.

        Note that only calculations which are off by a significant amount result in the
        creation of a correction record. Many calculations are off from the reported values
        by exaclty one dollar, presumably due to rounding errrors. These records typically
        do not fail the :func:`numpy.isclose()` test and so are not corrected.

        Args:
            calculated_df: table with calculated fields
            calculation_tolerance: What proportion (0-1) of calculated values are
              allowed to be incorrect without raising an AssertionError.
        """
        if "calculated_amount" not in calculated_df.columns:
            return calculated_df

        # Data types were very messy here, including pandas Float64 for the
        # calculated_amount columns which did not work with the np.isclose(). Not sure
        # why these are cropping up.
        calculated_df = calculated_df.convert_dtypes(convert_floating=False).astype(
            {self.value_col: "float64", "calculated_amount": "float64"}
        )
        calculated_df = calculated_df.assign(
            abs_diff=lambda x: abs(x[self.value_col] - x.calculated_amount),
            rel_diff=lambda x: np.where(
                (x[self.value_col] != 0.0),
                abs(x.abs_diff / x[self.value_col]),
                np.nan,
            ),
        )
        off_df = calculated_df[
            ~np.isclose(calculated_df.calculated_amount, calculated_df[self.value_col])
            & (calculated_df["abs_diff"].notnull())
        ]
        calculated_values = calculated_df[(calculated_df.abs_diff.notnull())]
        if calculated_values.empty:
            # Will only occur if all reported values are NaN when calculated values
            # exist, or vice versa.
            logger.warning(
                "Warning: No calculated values have a corresponding reported value in the table."
            )
            off_ratio = np.nan
        else:
            off_ratio = len(off_df) / len(calculated_values)
            if off_ratio > calculation_tolerance:
                raise AssertionError(
                    f"Calculations in {self.root_table} are off by {off_ratio:.2%}. Expected tolerance "
                    f"of {calculation_tolerance:.1%}."
                )

        # # We'll only get here if the proportion of calculations that are off is acceptable
        if off_ratio > 0 or np.isnan(off_ratio):
            logger.info(
                f"{self.root_table}: has {len(off_df)} ({off_ratio:.02%}) records whose "
                "calculations don't match. Adding correction records to make calculations "
                "match reported values."
            )
            corrections = off_df.copy()

            corrections[self.value_col] = (
                corrections[self.value_col].fillna(0.0)
                - corrections["calculated_amount"]
            )
            corrections["original_factoid"] = corrections["xbrl_factoid"]
            corrections["xbrl_factoid"] = corrections["xbrl_factoid"] + "_correction"
            corrections["row_type_xbrl"] = "correction"
            corrections["is_within_table_calc"] = False
            corrections["record_id"] = pd.NA

            calculated_df = pd.concat(
                [calculated_df, corrections], axis="index"
            ).reset_index(drop=True)
        return calculated_df


def in_explosion_tables(table_name: str, in_explosion_table_names: list[str]) -> bool:
    """Determine if any of a list of table_names in the list of thre explosion tables.

    Args:
        table_name: tables name. Typically from the ``source_tables`` element from an
            xbrl calculation component
        in_explosion_table_names: list of tables involved in a particular set of
            exploded tables.
    """
    return table_name in in_explosion_table_names


################################################################################
# XBRL Calculation Forests
################################################################################
class XbrlCalculationForestFerc1(BaseModel):
    """A class for manipulating groups of hierarchically nested XBRL calculations.

    We expect that the facts reported in high-level FERC tables like
    :ref:`income_statement_ferc1` and :ref:`balance_sheet_assets_ferc1` should be
    calculable from many individually reported granular values, based on the
    calculations encoded in the XBRL Metadata, and that these relationships should have
    a hierarchical tree structure. Several individual values from the higher level
    tables will appear as root nodes at the top of each hierarchy, and the leaves in
    the underlying tree structure are the individually reported non-calculated values
    that make them up. Because the top-level tables have several distinct values in
    them, composed of disjunct sets of reported values, we have a forest (a group of
    several trees) rather than a single tree.

    The information required to build a calculation forest is most readily found in the
    data produced by :meth:`MetadataExploder.boom`  A list of seed nodes can also be
    supplied, indicating which nodes must be present in the resulting forest. This can
    be used to prune irrelevant portions of the overall forest out of the exploded
    metadata. If no seeds are provided, then all of the nodes referenced in the
    exploded_calcs input dataframe will be used as seeds.

    This class makes heavy use of :mod:`networkx` to manage the graph that we build
    from calculation relationships.
    """

    # Not sure if dynamically basing this on NodeId is really a good idea here.
    calc_cols: list[str] = list(NodeId._fields)
    parent_cols: list[str] | None = None
    exploded_meta: pd.DataFrame = pd.DataFrame()
    exploded_calcs: pd.DataFrame = pd.DataFrame()
    seeds: list[NodeId] = []
    tags: pd.DataFrame = pd.DataFrame()

    class Config:
        """Allow the class to store a dataframe."""

        arbitrary_types_allowed = True

    @validator("parent_cols", always=True)
    def set_parent_cols(cls, v, values) -> list[str]:
        """A convenience property to generate parent column."""
        return [col + "_parent" for col in values["calc_cols"]]

    @validator("exploded_calcs")
    def unique_associations(cls, v: pd.DataFrame, values) -> pd.DataFrame:
        """Ensure parent-child associations in exploded calculations are unique."""
        pks = values["calc_cols"] + values["parent_cols"]
        dupes = v.duplicated(subset=pks, keep=False)
        if dupes.any():
            logger.warning(
                "Consolidating non-unique associations found in exploded_calcs:\n"
                f"{v.loc[dupes]}"
            )
        # Drop all duplicates with null weights -- this is a temporary fix to an issue
        # from upstream.
        assert not v.duplicated(subset=pks, keep=False).any()
        return v

    @validator("exploded_calcs")
    def single_valued_weights(cls, v: pd.DataFrame, values) -> pd.DataFrame:
        """Ensure that every calculation component has a uniquely specified weight."""
        multi_valued_weights = (
            v.groupby(values["calc_cols"], dropna=False)["weight"]
            .transform("nunique")
            .gt(1)
        )
        if multi_valued_weights.any():
            logger.warning(
                f"Found {sum(multi_valued_weights)} calculations with conflicting "
                "weights."
            )
        return v

    @validator("exploded_calcs")
    def calcs_have_required_cols(cls, v: pd.DataFrame, values) -> pd.DataFrame:
        """Ensure exploded calculations include all required columns."""
        required_cols = values["parent_cols"] + values["calc_cols"] + ["weight"]
        missing_cols = [col for col in required_cols if col not in v.columns]
        if missing_cols:
            raise ValueError(
                f"Exploded calculations missing expected columns: {missing_cols=}"
            )
        return v[required_cols]

    @validator("exploded_calcs")
    def calc_parents_notna(cls, v: pd.DataFrame) -> pd.DataFrame:
        """Ensure that parent table_name and xbrl_factoid columns are non-null."""
        if v[["table_name_parent", "xbrl_factoid_parent"]].isna().any(axis=None):
            raise AssertionError("Null parent table name or xbrl_factoid found.")
        return v

    @validator("tags")
    def tags_have_required_cols(cls, v: pd.DataFrame, values) -> pd.DataFrame:
        """Ensure tagging dataframe contains all required index columns."""
        missing_cols = [col for col in values["calc_cols"] if col not in v.columns]
        if missing_cols:
            raise ValueError(
                f"Tagging dataframe was missing expected columns: {missing_cols=}"
            )
        return v

    @validator("tags")
    def tags_cols_notnull(cls, v: pd.DataFrame) -> pd.DataFrame:
        """Ensure all tags have non-null table_name and xbrl_factoid."""
        null_idx_rows = v[v.table_name.isna() | v.xbrl_factoid.isna()]
        if not null_idx_rows.empty:
            logger.warning(
                f"Dropping {len(null_idx_rows)} tag rows with null values for "
                "table_name or xbrl_factoid:\n"
                f"{null_idx_rows}"
            )
        v = v.dropna(subset=["table_name", "xbrl_factoid"])
        return v

    @validator("tags")
    def single_valued_tags(cls, v: pd.DataFrame, values) -> pd.DataFrame:
        """Ensure all tags have unique values."""
        dupes = v.duplicated(subset=values["calc_cols"], keep=False)
        if dupes.any():
            logger.warning(
                f"Found {dupes.sum()} duplicate tag records:\n{v.loc[dupes]}"
            )
        return v

    @validator("seeds")
    def seeds_within_bounds(cls, v: pd.DataFrame, values) -> pd.DataFrame:
        """Ensure that all seeds are present within exploded_calcs index.

        For some reason this validator is being run before exploded_calcs has been
        added to the values dictionary, which doesn't make sense, since "seeds" is
        defined after exploded_calcs in the model.
        """
        all_nodes = values["exploded_calcs"].set_index(values["parent_cols"]).index
        bad_seeds = [seed for seed in v if seed not in all_nodes]
        if bad_seeds:
            raise ValueError(f"Seeds missing from exploded_calcs index: {bad_seeds=}")
        return v

    def exploded_calcs_to_digraph(
        self: Self,
        exploded_calcs: pd.DataFrame,
    ) -> nx.DiGraph:
        """Construct :class:`networkx.DiGraph` of all calculations in exploded_calcs.

        First we construct a directed graph based on the calculation components. The
        "parent" or "source" nodes are the results of the calculations, and the "child"
        or "target" nodes are the individual calculation components. The structure of
        the directed graph is determined entirely by the primary key columns in the
        calculation components table.

        Then we compile a dictionary of node attributes, based on the individual
        calculation components in the exploded calcs dataframe.
        """
        source_nodes = list(
            exploded_calcs.loc[:, self.parent_cols]
            .rename(columns=lambda x: x.removesuffix("_parent"))
            .itertuples(name="NodeId", index=False)
        )
        target_nodes = list(
            exploded_calcs.loc[:, self.calc_cols].itertuples(name="NodeId", index=False)
        )
        edgelist = pd.DataFrame({"source": source_nodes, "target": target_nodes})
        forest = nx.from_pandas_edgelist(edgelist, create_using=nx.DiGraph)
        return forest

    @property
    def annotated_forest(self: Self) -> nx.DiGraph:
        """Calculation forest annotated with node calculation weights and tags."""
        # Reshape the tags to turn them into a dictionary of values per-node. This
        # will make it easier to add arbitrary sets of tags later on.
        tags_dict = (
            self.tags.convert_dtypes().set_index(self.calc_cols).to_dict(orient="index")
        )
        tags_dict_df = pd.DataFrame(
            index=pd.MultiIndex.from_tuples(tags_dict.keys(), names=self.calc_cols),
            data={"tags": list(tags_dict.values())},
        ).reset_index()

        # There are a few rare instances where a particular node is specified with more
        # than one weight (-1 vs. 1) and in those cases, we always want to keep the
        # weight of -1, since it affects the overall root->leaf calculation outcome.
        multi_valued_weights = (
            self.exploded_calcs.groupby(self.calc_cols, dropna=False)["weight"]
            .transform("nunique")
            .gt(1)
        )
        calcs_to_drop = multi_valued_weights & (self.exploded_calcs.weight == 1)
        # Check that there are only a *few* multi-valued weights.
        mv_wt_frac = sum(multi_valued_weights) / len(self.exploded_calcs)
        for table in self.table_names:
            if table in MAX_MULTIVALUE_WEIGHT_FRAC:
                max_mv_wt_frac = MAX_MULTIVALUE_WEIGHT_FRAC[table]

        logger.info(
            f"Found {sum(multi_valued_weights)}/{len(self.exploded_calcs)} "
            f"({mv_wt_frac:.2%}; max allowed: {max_mv_wt_frac:.2%}) "
            "nodes having conflicting weights. Dropping "
            f"{sum(calcs_to_drop)/len(self.exploded_calcs):.2%} where weight == 1"
        )

        if mv_wt_frac > max_mv_wt_frac:
            raise ValueError(
                "Unexpectedly high fraction of multivalued weights: "
                f"{mv_wt_frac} > {max_mv_wt_frac}"
            )

        deduplicated_calcs = self.exploded_calcs.loc[~calcs_to_drop]

        # Add metadata tags to the calculation components and reset the index.
        attr_cols = ["weight"]
        node_attrs = (
            pd.merge(
                left=self.exploded_meta,
                right=tags_dict_df,
                how="left",
                validate="m:1",
            )
            .reset_index(drop=True)
            .drop(columns=["xbrl_factoid_original", "is_within_table_calc"])
            .merge(
                deduplicated_calcs[self.calc_cols + attr_cols].drop_duplicates(),
                how="left",
                validate="1:1",
            )
            .astype({col: pd.StringDtype() for col in self.calc_cols})
            .set_index(self.calc_cols)
        )
        # Fill NA tag dictionaries with an empty dict so the type is uniform:
        node_attrs["tags"] = node_attrs["tags"].apply(lambda x: {} if x != x else x)
        forest = self.forest
        nx.set_node_attributes(forest, node_attrs.to_dict(orient="index"))
        return forest

    @property
    def full_digraph(self: Self) -> nx.DiGraph:
        """A digraph of all calculations described by the exploded metadata."""
        full_digraph = self.exploded_calcs_to_digraph(
            exploded_calcs=self.exploded_calcs,
        )
        connected_components = list(
            nx.connected_components(full_digraph.to_undirected())
        )
        logger.debug(
            f"Full digraph contains {len(connected_components)} connected components."
        )
        if not nx.is_directed_acyclic_graph(full_digraph):
            logger.critical(
                "Calculations in Exploded Metadata contain cycles, which is invalid."
            )
        return full_digraph

    def prune_unrooted(self: Self, graph: nx.DiGraph) -> nx.DiGraph:
        """Prune any portions of the digraph that aren't reachable from the roots."""
        seeded_nodes = set(self.seeds)
        for seed in self.seeds:
            # the seeds and all of their descendants from the graph
            seeded_nodes = list(
                seeded_nodes.union({seed}).union(nx.descendants(graph, seed))
            )
        # any seeded node that is also a parent
        seeded_parents = [
            node
            for node, degree in dict(graph.out_degree(seeded_nodes)).items()
            if degree > 0
        ]
        seeded_calcs = (
            self.exploded_calcs.set_index(self.parent_cols)
            .loc[seeded_parents]
            .reset_index()
        )
        seeded_child_nodes = list(
            set(
                seeded_calcs[self.calc_cols].itertuples(index=False, name="NodeId")
            ).intersection(graph.nodes)
        )
        seeded_calcs = (
            seeded_calcs.set_index(self.calc_cols).loc[seeded_child_nodes].reset_index()
        )
        seeded_digraph: nx.DiGraph = self.exploded_calcs_to_digraph(
            exploded_calcs=seeded_calcs
        )
        return seeded_digraph

    @property
    def seeded_digraph(self: Self) -> nx.DiGraph:
        """A digraph of all calculations that contribute to the seed values.

        Prune the full digraph to contain only those nodes in the :meth:`full_digraph`
        that are descendants of the seed nodes -- i.e. that are reachable along the
        directed edges, and thus contribute to the values reported to the XBRL facts
        associated with the seed nodes.

        We compile a list of all the :class:`NodeId` values that should be included in
        the pruned graph, and then use that list to select a subset of the exploded
        metadata to pass to :meth:`exploded_meta_to_digraph`, so that all of the
        associated metadata is also added to the pruned graph.
        """
        return self.prune_unrooted(self.full_digraph)

    @property
    def forest(self: Self) -> nx.DiGraph:
        """A pruned version of the seeded digraph that should be one or more trees.

        This method contains any special logic that's required to convert the
        :meth:`seeded_digraph` into a collection of trees. The main issue we currently
        have to deal with is passthrough calculations that we've added to avoid having
        duplicated calculations in the graph.

        In practice this method will probably return a single tree rather than a forest,
        but a forest with several root nodes might also be appropriate, since the root
        table may or may not have a top level summary value that includes all underlying
        calculated values of interest.
        """
        forest = self.seeded_digraph
        # Remove any node that has only one parent and one child, and add an edge
        # between its parent and child.
        for node in self.passthroughs:
            parent = list(forest.predecessors(node))
            assert len(parent) == 1
            successors = forest.successors(node)
            assert len(list(successors)) == 2
            child = [
                n
                for n in forest.successors(node)
                if not n.xbrl_factoid.endswith("_correction")
            ]
            correction = [
                n
                for n in forest.successors(node)
                if n.xbrl_factoid.endswith("_correction")
            ]
            assert len(child) == 1
            logger.debug(
                f"Replacing passthrough node {node} with edge from "
                f"{parent[0]} to {child[0]}"
            )
            forest.remove_nodes_from(correction + [node])
            forest.add_edge(parent[0], child[0])

        connected_components = list(nx.connected_components(forest.to_undirected()))
        logger.debug(
            f"Calculation forest contains {len(connected_components)} connected components."
        )

        # Remove any node that:
        # - ONLY has stepchildren.
        pure_stepparents = []
        stepparents = sorted(self.stepparents(forest))
        logger.info(f"Investigating {len(stepparents)=}")
        for node in stepparents:
            children = set(forest.successors(node))
            stepchildren = set(self.stepchildren(forest)).intersection(children)
            if (children == stepchildren) & (len(children) > 0):
                pure_stepparents.append(node)
                forest.remove_node(node)
        logger.info(f"Removed {len(pure_stepparents)} redundant/stepparent nodes.")
        logger.debug(f"Removed redunant/stepparent nodes: {sorted(pure_stepparents)}")

        # Prune any newly disconnected nodes resulting from the above removal of
        # pure stepparents. We expect the set of newly disconnected nodes to be empty.
        nodes_before_pruning = forest.nodes
        forest = self.prune_unrooted(forest)
        nodes_after_pruning = forest.nodes

        if pruned_nodes := set(nodes_before_pruning).difference(nodes_after_pruning):
            raise AssertionError(f"Unexpectedly pruned stepchildren: {pruned_nodes=}")

        # HACK alter.
        # two different parents. those parents have different sets of dimensions.
        # sharing some but not all of their children so they weren't caught from in the
        # only stepchildren node removal from above. a generalization here would be good
        almost_pure_stepparents = [
            NodeId(
                "utility_plant_summary_ferc1",
                "depreciation_amortization_and_depletion_utility_plant_leased_to_others",
                "total",
                pd.NA,
                pd.NA,
            ),
            NodeId(
                "utility_plant_summary_ferc1",
                "depreciation_and_amortization_utility_plant_held_for_future_use",
                "total",
                pd.NA,
                pd.NA,
            ),
            NodeId(
                "utility_plant_summary_ferc1",
                "utility_plant_in_service_classified_and_unclassified",
                "total",
                pd.NA,
                pd.NA,
            ),
        ]
        forest.remove_nodes_from(almost_pure_stepparents)

        # Ensure that we haven't removed any calculation components that *would* have
        # altered the final root-to-leaf calculations.
        # * Weights pertain to child nodes.
        # * But the nodes we're removing above are guaranteed to be parents (Though
        #   many them will also be children)
        # * We want to check that any of them that *are* children have a weight of 1.0
        # * Any node that gets removed by prune_unrooted() below isn't a concern, since
        #   it couldn't have been part of a calculation path leading to our chosen root.
        # * Need to look at the calc_cols columns because we care about children.
        # * Only care about the intersection of our pure / almost pure stepparents and
        #   the nodes that show up in calc_cols.
        # * This was probably failing before because the almost_pure_stepparents are
        #   table-specific, but we were trying to check them in all explosions. Duh.

        removed_stepparents = pure_stepparents

        if "utility_plant_summary_ferc1" in self.table_names:
            removed_stepparents = removed_stepparents + almost_pure_stepparents

        if (
            self.exploded_calcs.set_index(self.calc_cols)
            .loc[removed_stepparents, "weight"]
            .ne(1)
            .any()
        ):
            removed_with_weights = self.exploded_calcs.set_index(self.calc_cols).loc[
                removed_stepparents, "weight"
            ]
            logger.error(
                "Stepparent nodes with weights other than 1.0 were removed, altering "
                "the final root-to-leaf calculations and spacetime continuum.\n"
                f"{removed_with_weights[removed_with_weights != 1]}"
            )

        forest = self.prune_unrooted(forest)
        if not nx.is_forest(forest):
            logger.error(
                "Calculations in Exploded Metadata can not be represented as a forest!"
            )
        remaining_stepparents = set(self.stepparents(forest))
        if remaining_stepparents:
            logger.error(f"{remaining_stepparents=}")

        return forest

    @staticmethod
    def roots(graph: nx.DiGraph) -> list[NodeId]:
        """Identify all root nodes in a digraph."""
        return [n for n, d in graph.in_degree() if d == 0]

    @property
    def full_digraph_roots(self: Self) -> list[NodeId]:
        """Find all roots in the full digraph described by the exploded metadata."""
        return self.roots(graph=self.full_digraph)

    @property
    def seeded_digraph_roots(self: Self) -> list[NodeId]:
        """Find all roots in the seeded digraph."""
        return self.roots(graph=self.seeded_digraph)

    @property
    def forest_roots(self: Self) -> list[NodeId]:
        """Find all roots in the pruned calculation forest."""
        return self.roots(graph=self.forest)

    @staticmethod
    def leaves(graph: nx.DiGraph) -> list[NodeId]:
        """Identify all leaf nodes in a digraph."""
        return [n for n, d in graph.out_degree() if d == 0]

    @property
    def full_digraph_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the full digraph."""
        return self.leaves(graph=self.full_digraph)

    @property
    def seeded_digraph_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the seeded digraph."""
        return self.leaves(graph=self.seeded_digraph)

    @property
    def forest_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the pruned forest."""
        return self.leaves(graph=self.forest)

    @property
    def orphans(self: Self) -> list[NodeId]:
        """Identify all nodes that appear in metadata but not in the full digraph."""
        nodes = self.full_digraph.nodes
        return [
            NodeId(*n)
            for n in self.exploded_meta.set_index(self.calc_cols).index
            if n not in nodes
        ]

    @property
    def pruned(self: Self) -> list[NodeId]:
        """List of all nodes that appear in the DAG but not in the pruned forest."""
        return list(set(self.full_digraph.nodes).difference(self.forest.nodes))

    def stepchildren(self: Self, graph: nx.DiGraph) -> list[NodeId]:
        """Find all nodes in the graph that have more than one parent."""
        return [n for n, d in graph.in_degree() if d > 1]

    def stepparents(self: Self, graph: nx.DiGraph) -> list[NodeId]:
        """Find all nodes in the graph with children having more than one parent."""
        stepchildren = self.stepchildren(graph)
        stepparents = set()
        for stepchild in stepchildren:
            stepparents = stepparents.union(graph.predecessors(stepchild))
        return list(stepparents)

    @property
    def passthroughs(self: Self) -> list[NodeId]:
        """All nodes in the seeded digraph with a single parent and a single child.

        These nodes can be pruned, hopefully converting the seeded digraph into a
        forest. Note that having a "single child" really means having 2 children, one
        of which is a _correction to the calculation. We verify that the two children
        are one real child node, and one appropriate correction.
        """
        # In theory every node should have only one parent, but just to be safe, since
        # that's not always true right now:
        has_one_parent = {n for n, d in self.seeded_digraph.in_degree() if d == 1}
        # Calculated fields always have both the reported child and a correction that
        # we have added, so having "one" child really means having 2 successor nodes.
        may_have_one_child: set[NodeId] = {
            n for n, d in self.seeded_digraph.out_degree() if d == 2
        }
        # Check that one of these successors is the correction.
        has_one_child = []
        for node in may_have_one_child:
            children: set[NodeId] = set(self.seeded_digraph.successors(node))
            for child in children:
                if (node.table_name == child.table_name) and (
                    child.xbrl_factoid == node.xbrl_factoid + "_correction"
                ):
                    has_one_child.append(node)

        return list(has_one_parent.intersection(has_one_child))

    @property
    def leafy_meta(self: Self) -> pd.DataFrame:
        """Identify leaf facts and compile their metadata.

        - identify the root and leaf nodes of those minimal trees
        - adjust the weights associated with the leaf nodes to equal the
          product of the weights of all their ancestors.
        - Set leaf node tags to be the union of all the tags associated
          with all of their ancestors.

        Leafy metadata in the output dataframe includes:

        - The ID of the leaf node itself (this is the index).
        - The ID of the root node the leaf is descended from.
        - What tags the leaf has inherited from its ancestors.
        - The leaf node's xbrl_factoid_original
        - The weight associated with the leaf, in relation to its root.
        """
        # Construct a dataframe that links the leaf node IDs to their root nodes:
        pruned_forest = self.annotated_forest
        leaves = self.forest_leaves
        roots = self.forest_roots
        leaf_to_root_map = {
            leaf: root
            for leaf in leaves
            for root in roots
            if leaf in nx.descendants(pruned_forest, root)
        }
        leaves_df = pd.DataFrame(list(leaf_to_root_map.keys()))
        roots_df = pd.DataFrame(list(leaf_to_root_map.values())).rename(
            columns={col: col + "_root" for col in self.calc_cols}
        )
        leafy_meta = pd.concat([leaves_df, roots_df], axis="columns")

        # Propagate tags and weights to leaf nodes
        leaf_rows = []
        for leaf in leaves:
            leaf_tags = {}
            leaf_weight = pruned_forest.nodes[leaf].get("weight", 1.0)
            for node in nx.ancestors(pruned_forest, leaf):
                # TODO: need to check that there are no conflicts between tags that are
                # being propagated, e.g. if two different ancestors have been tagged
                # rate_base: yes and rate_base: no.
                leaf_tags |= pruned_forest.nodes[node]["tags"]
                # Root nodes have no weight because they don't come from calculations
                # We assign them a weight of 1.0
                # if not pruned_forest.nodes[node].get("weight", False):
                if pd.isna(pruned_forest.nodes[node]["weight"]):
                    assert node in roots
                    node_weight = 1.0
                else:
                    node_weight = pruned_forest.nodes[node]["weight"]
                leaf_weight *= node_weight

            # Construct a dictionary describing the leaf node and convert it into a
            # single row DataFrame. This makes adding arbitrary tags easy.
            leaf_attrs = {
                "weight": leaf_weight,
                "tags": leaf_tags,
                "table_name": leaf.table_name,
                "xbrl_factoid": leaf.xbrl_factoid,
                "utility_type": leaf.utility_type,
                "plant_status": leaf.plant_status,
                "plant_function": leaf.plant_function,
            }
            leaf_rows.append(pd.json_normalize(leaf_attrs, sep="_"))

        # Combine the two dataframes we've constructed above:
        return (
            pd.merge(leafy_meta, pd.concat(leaf_rows), validate="one_to_one")
            .reset_index(drop=True)
            .convert_dtypes()
        )

    @property
    def root_calculations(self: Self) -> pd.DataFrame:
        """Produce a calculation components dataframe containing only roots and leaves.

        This dataframe has a format similar to exploded_calcs and can be used with the
        exploded data to verify that the root values can still be correctly calculated
        from the leaf values.
        """
        return self.leafy_meta.rename(columns=lambda x: re.sub("_root$", "_parent", x))

    @property
    def table_names(self: Self) -> list[str]:
        """Produce the list of tables involved in this explosion."""
        return list(self.exploded_calcs["table_name_parent"].unique())

    def plot_graph(self: Self, graph: nx.DiGraph) -> None:
        """Visualize a CalculationForest graph."""
        colors = ["red", "yellow", "green", "blue", "orange", "cyan", "purple"]
        color_map = {
            table: color
            for table, color in zip(self.table_names, colors[: len(self.table_names)])
        }

        pos = graphviz_layout(graph, prog="dot", args='-Grankdir="LR"')
        for table, color in color_map.items():
            nodes = [node for node in graph.nodes if node.table_name == table]
            nx.draw_networkx_nodes(nodes, pos, node_color=color, label=table)
        nx.draw_networkx_edges(graph, pos)
        # The labels are currently unwieldy
        # nx.draw_networkx_labels(nx_forest, pos)
        # Use this to draw everything if/once labels are fixed
        # nx.draw_networkx(nx_forest, pos, node_color=node_color)
        plt.legend(scatterpoints=1)
        plt.show()

    def plot_nodes(self: Self, nodes: list[NodeId]) -> None:
        """Plot a list of nodes based on edges found in exploded_calcs."""
        graph_to_plot = self.full_digraph
        nodes_to_remove = set(graph_to_plot.nodes()).difference(nodes)
        # NOTE: this doesn't and can't revise the graph to identify which nodes are
        # still connecteded to the root we care about after remvoing these nodes.
        # We might want to use a more intelligent method of building the graph.
        graph_to_plot.remove_nodes_from(nodes_to_remove)
        self.plot_graph(graph_to_plot)

    def plot(
        self: Self, graph: Literal["full_digraph", "seeded_digraph", "forest"]
    ) -> None:
        """Visualize various stages of the calculation forest."""
        self.plot_graph(self.__getattribute__(graph))

    def leafy_data(
        self: Self, exploded_data: pd.DataFrame, value_col: str
    ) -> pd.DataFrame:
        """Use the calculation forest to prune the exploded dataframe.

        - Drop all rows that don't correspond to either root or leaf facts.
        - Verify that the reported root values can still be generated by calculations
          that only refer to leaf values. (not yet implemented)
        - Merge the leafy metadata onto the exploded data, keeping only those rows
          which refer to the leaf facts.
        - Use the leaf weights to adjust the reported data values.

        TODO: This method could either live here, or in the Exploder class, which would
        also have access to exploded_meta, exploded_data, and the calculation forest.

        - There are a handful of NA values for ``report_year`` and ``utility_id_ferc1``
          because of missing correction records in data. Why are those correction
          records missing? Should we be doing an inner merge instead of a left merge?
        - Still need to validate the root node calculations.

        """
        leafy_data = pd.merge(
            left=self.leafy_meta,
            right=exploded_data,
            how="left",
            validate="one_to_many",
        )
        # Scale the data column of interest:
        leafy_data[value_col] = leafy_data[value_col] * leafy_data["weight"]
        return leafy_data.reset_index(drop=True).convert_dtypes()

    def forest_as_table(self: Self) -> pd.DataFrame:
        """Draft version of table representation of forest.

        Build a table that represents that forest strucutre. This table will have
        multiple sets of columns cooresponding to the ``calc_cols`` with a suffix
        representing which level of the tree the columns represent.
        """
        forest = self.forest  # so we don't have to build this a gazzilion times.
        seed_node = self.seeds
        if len(seed_node) != 1:
            raise AssertionError(
                "Generating the forest as a table is only enabled with one seed node."
            )
        seed_node = seed_node[0]
        # not sure if this is correct but it seems like it is working.
        simple_paths = []
        for leaf in self.leaves(forest):
            simple_paths.append(nx.shortest_simple_paths(forest, seed_node, leaf))
        max_seed_to_child_paths = max([len(list(sp)[0]) for sp in simple_paths])

        # get the table started with just the seed node.
        forest_as_table = self._forest_as_table_level_from_nodes(
            self.seeds, forest, n=0
        )
        for n in range(1, max_seed_to_child_paths):
            logger.info(n)
            forest_as_table = self._add_next_level_into_forest_as_table(
                forest=forest,
                forest_as_table=forest_as_table,
                n=n,
            )
        # drop all columns that are full of nulls. for readability mostly.
        return forest_as_table.dropna(axis=1, how="all")

    def _forest_as_table_level_from_nodes(self, nodes_level_n, forest, n):
        """Build a table of parent nodes and their children as level n and leven n+1."""
        level_n_cols = [f"{col}_level{n}" for col in self.calc_cols]
        level_next_cols = [f"{col}_level{n+1}" for col in self.calc_cols]
        # get the successors of this level so we can add a whole new level!
        nodes_level_next = []
        for node in nodes_level_n:
            nodes_level_next.append(list(forest.successors(node)))
        nodes_level_next = pudl.helpers.dedupe_n_flatten_list_of_lists(nodes_level_next)

        calc_comps_next = (
            # Get the current level as parents
            self.exploded_calcs.set_index(self.parent_cols)
            .loc[nodes_level_n]
            .reset_index()
            # rename those parent to _leveln
            .rename(
                columns={
                    col: col.replace("_parent", f"_level{n}")
                    for col in self.parent_cols
                }
            )
            # Get the next level as calc components
            .set_index(self.calc_cols)
            .loc[nodes_level_next]
            .reset_index()
            # rename those as next level
            .rename(
                columns={
                    col: f"{col}_level{n+1}" for col in self.calc_cols + ["weight"]
                }
            )[level_n_cols + level_next_cols + [f"weight_level{n+1}"]]
        )
        return calc_comps_next

    def _add_next_level_into_forest_as_table(
        self: Self,
        forest: nx.digraph,
        forest_as_table: pd.DataFrame,
        n: int,
    ) -> pd.DataFrame:
        """Add another level onto the ``forest_as_table`` table."""
        level_n_cols = [f"{col}_level{n}" for col in self.calc_cols]
        # get the list of children from the current level in the forest_as_table
        # is there a better way to go from df -> list of nodes?
        nodes_level_n = [
            NodeId(*node)
            for node in forest_as_table.dropna(subset=level_n_cols, how="all")
            .set_index(level_n_cols)
            .index
        ]
        # but some nodes never show up as parents in the calc comps table
        # so we need to restrict the nodes that we actually need to get
        # sucessors from the parents in calc_comps
        nodes_level_n = self.exploded_calcs.set_index(
            self.parent_cols
        ).index.intersection(nodes_level_n)
        calc_comps_next = self._forest_as_table_level_from_nodes(
            nodes_level_n, forest, n
        )
        null_level_mask = forest_as_table[level_n_cols].isnull().all(axis="columns")
        forest_as_table_n = forest_as_table[~null_level_mask].merge(
            calc_comps_next,
            on=level_n_cols,
            how="outer",
            indicator=True,
            validate="1:m",
        )
        merge_vc = forest_as_table_n._merge.value_counts()
        if not merge_vc.eq(0).right_only:
            raise AssertionError(
                f"AHH! We have {merge_vc.right_only} next level nodes that we couldn't "
                "match up to parents."
            )
        forest_as_table_n.drop(columns=["_merge"], inplace=True)
        return pd.concat(
            [forest_as_table_n, forest_as_table[null_level_mask]]
        ).reset_index(drop=True)
