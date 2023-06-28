"""A collection of denormalized FERC assets and helper functions."""
import json
from typing import NamedTuple, Self

import networkx as nx
import numpy as np
import pandas as pd
from dagster import AssetIn, AssetsDefinition, Field, Mapping, asset
from pydantic import BaseModel, validator

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


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
def exploded_table_asset_factory(
    root_table: str,
    table_names_to_explode: list[str],
    io_manager_key: str | None = None,  # TODO: Add metadata for tables
) -> AssetsDefinition:
    """Create an exploded table based on a set of related input tables."""
    ins: Mapping[str, AssetIn] = {}
    ins = {"clean_xbrl_metadata_json": AssetIn("clean_xbrl_metadata_json")}
    ins |= {table_name: AssetIn(table_name) for table_name in table_names_to_explode}

    @asset(name=f"exploded_{root_table}", ins=ins, io_manager_key=io_manager_key)
    def exploded_tables_asset(
        **kwargs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        clean_xbrl_metadata_json = kwargs["clean_xbrl_metadata_json"]
        tables_to_explode = {
            name: df
            for (name, df) in kwargs.items()
            if name != "clean_xbrl_metadata_json"
        }
        return Exploder(
            table_names=tables_to_explode.keys(), root_table=root_table
        ).boom(
            tables_to_explode=tables_to_explode,
            clean_xbrl_metadata_json=clean_xbrl_metadata_json,
        )

    return exploded_tables_asset


def create_exploded_table_assets() -> list[AssetsDefinition]:
    """Create a list of exploded FERC1 assets.

    Returns:
        A list of AssetsDefinitions where each asset is a clean ferc form 1 table.
    """
    explosion_tables = {
        "income_statement_ferc1": [
            "income_statement_ferc1",
            "depreciation_amortization_summary_ferc1",
            "electric_operating_expenses_ferc1",
            "electric_operating_revenues_ferc1",
        ],
        "balance_sheet_assets_ferc1": [
            "utility_plant_summary_ferc1",
            "plant_in_service_ferc1",
            # "electric_plant_depreciation_changes_ferc1", this tbl isnt calc checked
        ],
        # "balance_sheet_liabilities_ferc1": [
        #     "retained_earnings_ferc1", this tbl isnt calc checked so there is no tree
        # ],
    }
    assets = []
    for root_table, table_names_to_explode in explosion_tables.items():
        assets.append(exploded_table_asset_factory(root_table, table_names_to_explode))
    return assets


exploded_ferc1_assets = create_exploded_table_assets()


class MetadataExploder:
    """Combine a set of inter-related, nested table's metadata."""

    def __init__(self, table_names: list[str]):
        """Instantiate MetadataExploder."""
        self.table_names = table_names

    def boom(self, clean_xbrl_metadata_json: dict):
        """Combine a set of inter-realted table's metatada for use in :class:`Exploder`.

        Args:
            clean_xbrl_metadata_json: cleaned XRBL metadata.
        """
        tbl_metas = []
        for table_name in self.table_names:
            tbl_meta = (
                pudl.transform.ferc1.FERC1_TFR_CLASSES[table_name](
                    xbrl_metadata_json=clean_xbrl_metadata_json[table_name]
                )
                .xbrl_metadata[
                    [
                        "xbrl_factoid",
                        "calculations",
                        "row_type_xbrl",
                        "xbrl_factoid_original",
                        "intra_table_calc_flag",
                    ]
                ]
                .assign(table_name=table_name)
            )
            tbl_metas.append(tbl_meta)
        return (
            pd.concat(tbl_metas)
            .reset_index(drop=True)
            .pipe(self.redefine_calculations_with_components_out_of_explosion)
        )

    def redefine_calculations_with_components_out_of_explosion(
        self, meta_explode: pd.DataFrame
    ) -> pd.DataFrame:
        """Overwrite the calculations with calculation components not in explosion."""
        calc_explode = convert_calculations_into_calculation_component_table(
            meta_explode
        )
        calc_explode["in_explosion"] = calc_explode.source_tables.apply(
            in_explosion_tables, in_explosion_table_names=self.table_names
        )
        not_in_explosion_xbrl_factoids = list(
            calc_explode.loc[~calc_explode.in_explosion, "xbrl_factoid"].unique()
        )
        # this is a temporary variable. Remove when we migrate the pruning/leaf
        # identification into the Tree/Forest builders. Right now this will be used in
        # Exploder.remove_inter_table_calc_duplication. Because we are changing the
        # metadata, this variable can only be generated BEFORE we reclassify these
        # calculations.
        self.inter_table_components_in_intra_table_calc = calc_explode.loc[
            ~calc_explode.in_explosion, "name"
        ].unique()
        meta_explode.loc[
            meta_explode.xbrl_factoid.isin(not_in_explosion_xbrl_factoids),
            ["calculations", "row_type_xbrl"],
        ] = ("[]", "reported_value")
        return meta_explode


class Exploder:
    """Get unique, granular datapoints from a set of related, nested FERC1 tables."""

    def __init__(self, table_names: list[str], root_table: str):
        """Instantiate an Exploder class."""
        self.table_names = table_names
        self.root_table = root_table
        self.meta_exploder = MetadataExploder(self.table_names)

    @property
    def other_dimensions(self) -> list[str]:
        """Get all of the column names for the other dimensions."""
        other_dimensions = []
        for table_name in self.table_names:
            other_dimensions.append(
                pudl.transform.ferc1.FERC1_TFR_CLASSES[
                    table_name
                ]().params.reconcile_table_calculations.subtotal_column
            )
        other_dimensions = [sub for sub in other_dimensions if sub]
        return other_dimensions

    @property
    def exploded_pks(self) -> list[str]:
        """Get the joint primary keys of the exploded tables."""
        pks = []
        for table_name in self.table_names:
            xbrl_factoid_name = pudl.transform.ferc1.FERC1_TFR_CLASSES[
                table_name
            ]().params.xbrl_factoid_name
            pks.append(
                [
                    col
                    for col in pudl.metadata.classes.Resource.from_id(
                        table_name
                    ).schema.primary_key
                    if col != xbrl_factoid_name
                ]
            )
        pks = pudl.helpers.dedupe_n_flatten_list_of_lists(pks) + ["xbrl_factoid"]
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
        clean_xbrl_metadata_json: dict,
    ) -> pd.DataFrame:
        """Explode a set of nested tables.

        There are five main stages of this process:

        #. Prep all of the individual tables for explosion.
        #. Concatenate all of the tabels together.
        #. Remove duplication in the concatenated exploded table.
        #. Add in metadata for the remaining ``xbrl_factoid`` s regarding linage (not
           implemented yet).
        #. Validate (not implemented yet).

        Args:
            tables_to_explode: dictionary of table name (key) to transfomed table (value).
            root_table: the table at the base of the tree of tables_to_explode.
            clean_xbrl_metadata_json: json version of the XBRL-metadata.
        """
        exploded = (
            self.initial_explosion_concatenation(
                tables_to_explode, clean_xbrl_metadata_json
            )
            # REMOVE THE DUPLICATION
            .pipe(self.remove_factoids_from_mutliple_tables, tables_to_explode)
            .pipe(self.remove_totals_from_other_dimensions)
            .pipe(self.remove_inter_table_calc_duplication)
            .pipe(remove_intra_table_calculated_values)
        )
        return exploded

    def initial_explosion_concatenation(
        self, tables_to_explode: dict[str, pd.DataFrame], clean_xbrl_metadata_json: dict
    ) -> pd.DataFrame:
        """Concatenate all of the tables for the explosion.

        Merge in some basic pieces of the each table's metadata and add ``table_name``. At
        this point in the explosion, there will be a lot of duplicaiton in the output.
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
        metadata_exploded = self.meta_exploder.boom(clean_xbrl_metadata_json)
        exploded = pd.concat(explosion_tables)
        # drop any metadata columns coming from the tbls bc we may have edited the
        # metadata df so we want to grab that directly
        meta_idx = ["xbrl_factoid", "table_name"]
        meta_columns = [
            col
            for col in exploded
            if col
            in [meta_col for meta_col in metadata_exploded if meta_col not in meta_idx]
        ]
        exploded = exploded.drop(columns=meta_columns).merge(
            metadata_exploded,
            how="left",
            on=meta_idx,
            validate="m:1",
        )
        return exploded

    def remove_factoids_from_mutliple_tables(
        self, exploded, tables_to_explode
    ) -> pd.DataFrame:
        """Remove duplicate factoids that have references in multiple tables."""
        # add the table-level so we know which inter-table duped factoid is downstream
        logger.info("Explode: Remove factoids from multiple tables.")
        exploded = pd.merge(
            exploded,
            get_table_levels(tables_to_explode, self.root_table),
            on="table_name",
            how="left",
            validate="m:1",
        )
        # ensure all tbls have level references
        assert ~exploded.table_level.isnull().any()

        # deal w/ factoids that show up in
        inter_table_facts = (
            exploded[
                [
                    "xbrl_factoid_original",
                    "table_name",
                    "row_type_xbrl",
                    "intra_table_calc_flag",
                    "table_level",
                ]
            ]
            .dropna(subset=["xbrl_factoid_original"])
            .drop_duplicates(["xbrl_factoid_original", "table_name"])
            .sort_values(["xbrl_factoid_original", "table_level"], ascending=False)
        )
        # its the combo of xbrl_factoid_original and table_name that we really care about
        # in terms of dropping bc we want to drop the higher-level/less granular table.
        inter_table_facts_to_drop = inter_table_facts[
            inter_table_facts.duplicated(["xbrl_factoid_original"], keep="first")
        ]
        logger.info(
            f"Explode: Preparing to drop: {inter_table_facts_to_drop.xbrl_factoid_original}"
        )
        # TODO: We need to fill in the other_dimensions columns before doing this check.
        # check to see if there are different values in the values that show up in two tables
        inter_table_ref_check = (
            # these are all the dupes not just the ones we are axing
            exploded[
                exploded.xbrl_factoid_original.isin(
                    inter_table_facts_to_drop.xbrl_factoid_original
                )
            ]
            .groupby(
                [col for col in self.exploded_pks if col != "xbrl_factoid"]
                + ["xbrl_factoid_original"],
                dropna=False,
            )[[self.value_col]]
            .nunique()
        )
        assert inter_table_ref_check[inter_table_ref_check[self.value_col] > 1].empty

        factoid_idx = ["xbrl_factoid_original", "table_name"]
        exploded = exploded.set_index(factoid_idx)
        inter_table_refs = exploded.loc[
            inter_table_facts_to_drop.set_index(factoid_idx).index
        ]
        if len(inter_table_refs) > 1:
            logger.info(
                f"Explode: Dropping {len(inter_table_refs)} ({len(inter_table_refs)/len(exploded):.01%}) inter-table references."
            )
            exploded = exploded.loc[
                exploded.index.difference(
                    inter_table_facts_to_drop.set_index(factoid_idx).index
                )
            ].reset_index()
        else:
            logger.info("Explode: Found no inter-table references. Dropping nothing.")
            # reset the index bc our index rn is tbl name and factoid name og.
            exploded = exploded.reset_index()
        return exploded

    def remove_totals_from_other_dimensions(
        self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Remove the totals from the other dimensions."""
        logger.info("Explode: Interdimensional time.")
        # bc we fill in some of the other dimension columns for
        # ensure we are only taking totals from table_name's that have more than one value
        # for their other dimensions.
        # find the totals from the other dimensions
        exploded = exploded.assign(
            **{
                f"{dim}_nunique": exploded.groupby(["table_name"])[dim].transform(
                    "nunique"
                )
                for dim in self.other_dimensions
            }
        )
        exploded = exploded.assign(
            **{
                f"{dim}_total": (exploded[dim] == "total")
                & (exploded[f"{dim}_nunique"] != 1)
                for dim in self.other_dimensions
            }
        )
        total_mask = (exploded[[f"{dim}_total" for dim in self.other_dimensions]]).any(
            axis=1
        )
        total_len = len(exploded[total_mask])
        logger.info(
            f"Removing {total_len} ({total_len/len(exploded):.1%}) of records which are "
            f"totals of the following dimensions {self.other_dimensions}"
        )
        # remove the totals & drop the cols we used to make em
        drop_cols = [
            f"{dim}{suff}"
            for suff in ["_nunique", "_total"]
            for dim in self.other_dimensions
        ]
        exploded = exploded[~total_mask].drop(columns=drop_cols)
        return exploded

    def remove_inter_table_calc_duplication(
        self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Treat the duplication in the inter-table calculations.

        There are several possible ways to remove the duplication in the inter table calcs.
        See issue #2622. Right now we are doing the simplest option which removes some level
        of detail.
        """
        logger.info("Explode: Doing inter-table calc deduplications stuff.")
        inter_table_components_in_intra_table_calc = list(
            self.meta_exploder.inter_table_components_in_intra_table_calc
        )
        if inter_table_components_in_intra_table_calc:
            logger.info(
                "Explode: Removing intra-table calculation components in inter-table "
                f"calcution ({inter_table_components_in_intra_table_calc})."
            )
            # remove the calcuation components that are a part of an inter-table calculation
            exploded = exploded[
                ~exploded.xbrl_factoid.isin(inter_table_components_in_intra_table_calc)
            ]
        return exploded


def in_explosion_tables(
    source_tables: list[str], in_explosion_table_names: list[str]
) -> bool:
    """Determine if any of a list of source_tables in the list of thre explosion tables.

    Args:
        source_tables: the list of tables. Typically from the ``source_tables`` element
            from an xbrl calculation component
        in_explosion_table_names: list of tables involved in a particular set of
            exploded tables.
    """
    return any([True for tbl in source_tables if tbl in in_explosion_table_names])


def convert_calculations_into_calculation_component_table(
    metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Convert xbrl metadata calculations into a table of calculation components."""
    calc_dfs = []
    for calc, tbl, factoid in zip(
        metadata.calculations, metadata.table_name, metadata.xbrl_factoid
    ):
        calc_dfs.append(
            pd.DataFrame(json.loads(calc)).assign(table_name=tbl, xbrl_factoid=factoid)
        )
    calcs = pd.concat(calc_dfs).reset_index(drop=True)

    return calcs.merge(
        metadata.drop(columns=["calculations"]),
        on=["xbrl_factoid", "table_name"],
        how="left",
    )


def get_table_level(table_name: str, top_table: str) -> int:
    """Get a table level."""
    # we may be able to infer this nesting from the metadata
    table_nesting = {
        "balance_sheet_assets_ferc1": {
            "utility_plant_summary_ferc1": {
                "plant_in_service_ferc1": None,
                "electric_plant_depreciation_changes_ferc1": None,
            },
        },
        "balance_sheet_liabilities_ferc1": {"retained_earnings_ferc1": None},
        "income_statement_ferc1": {
            "depreciation_amortization_summary_ferc1": None,
            "electric_operating_expenses_ferc1": None,
            "electric_operating_revenues_ferc1": None,
        },
    }
    if table_name == top_table:
        level = 1
    elif table_name in table_nesting[top_table].keys():
        level = 2
    elif table_name in pudl.helpers.dedupe_n_flatten_list_of_lists(
        [values.keys() for values in table_nesting[top_table].values()]
    ):
        level = 3
    else:
        raise AssertionError(
            f"AH we didn't find yer table name {table_name} in the nested group of "
            "tables. Be sure all the tables you are trying to explode are related."
        )
    return level


def get_table_levels(tables_to_explode: list[str], top_table: str) -> pd.DataFrame:
    """Get a set of table's level in the explosion.

    Level in this context means where it sits in the tree of the relationship of these
    tables. Level 1 is at the root while the higher numbers are towards the leaves of
    the trees.
    """
    table_levels = {"table_name": [], "table_level": []}
    for table_name in tables_to_explode:
        table_levels["table_name"].append(table_name)
        table_levels["table_level"].append(get_table_level(table_name, top_table))
    return pd.DataFrame(table_levels)


def find_intra_table_components_to_remove(
    inter_table_calc: str, table_name: str
) -> list[str]:
    """Find all xbrl_factoid's within a calc which are native to the source table.

    For all calculations which contain any component's that are natively reported in a
    different table than the calculated factoid, we label those as "inter-table"
    calculations. Sometimes those calculations have components with mix sources - from
    the native table and from other tables. For those mixed-source inter-table
    calculated values, we are going to remove all of the components which are native to
    the source table. This removes some detail but enables us to keep the calculated
    value in the table without any duplication.

    Returns:
        a list of ``xbrl_factoid`` names.
    """
    return [
        component["name"]
        for component in json.loads(inter_table_calc)
        if len(component.get("source_tables")) == 1
        and component.get("source_tables")[0] == table_name
    ]


def remove_intra_table_calculated_values(exploded: pd.DataFrame) -> pd.DataFrame:
    """Remove all of the intra-table calculated values.

    This is assuming that all of these calculated values have been validated as a part
    of the table transform step.
    """
    exploded = exploded[
        (exploded.row_type_xbrl != "calculated_value")
        | (exploded.row_type_xbrl.isnull())
        # keep in the inter-table calced value
        | (
            (exploded.row_type_xbrl == "calculated_value")
            & (~exploded.intra_table_calc_flag)
        )
    ]
    return exploded


################################################################################
# XBRL Calculation Tree
################################################################################
class XbrlCalculationTreeFerc1(BaseModel):
    """A FERC Form 1 XBRL calculation tree.

    In the special case of our exploded table calculations, each XBRL fact can be
    treated as a node in a tree, with calculated values being composed of other facts,
    and reported values showing up as leaves in the tree, with no subcomponents. The
    tree can also be pruned to exclude tables outside of those being included in the
    explosion.

    In general, XBRL facts can show up in more than one source table, but in the case of
    the explosions, we need to select a single source table to use. When building a
    calculation tree from the exploded metadata, we need to be able to select the most
    granular / detailed / "leafy" source table among several.

    """

    xbrl_factoid: str
    """The XBRL factoid that we're using to identify the value.

    This value should correspond to the ``name`` field of a calculation. It may have
    been renamed from the original reported XBRL fact name. Along with ``source_table``
    this attribute uniquely identifies the node within the tree.
    """
    source_table: str
    """The unique source table chosen for the purposes of the explosion.

    Along with ``xbrl_factoid`` this attribute uniquely identifies the node within the
    tree.
    """
    xbrl_factoid_original: str
    """The original reported XBRL Fact name."""
    weight: float
    """The weight associated with the XBRL Fact for calculation purposes.

    Initially this is the reported weight from the XBRL metadata, but in order to ensure
    that the value reported for the root fact in the tree can be validated using only
    the values associated with the leaf facts, we have to update the weights when we
    remove all the intervening layers of the tree. See
    :meth:`XbrlCalculationTreeFerc1.propagate_weights`.
    """
    children: list["XbrlCalculationTreeFerc1"] = []
    """The subcomponents required to calculate the value associated with this fact.

    This list will be empty for leaf nodes (reported values) and for calculated values
    that lie outside the list of tables being used in the explosion.
    """
    tags: dict[str, str] = {}
    """A dictionary of categorical metadata tags associated with a node in the tree."""

    def pprint(self: Self, indent=4):
        """Print a legible JSONified version of the calculation tree."""
        print(json.dumps(self.dict(), indent=indent))

    @staticmethod
    def _check_index(df: pd.DataFrame):
        """Check that an input meta dataframe is appropriately indexed."""
        idx_cols = ["table_name", "xbrl_factoid"]
        if df.index.names != idx_cols:
            raise AssertionError(
                f"Exploded metadataframes must be indexed by {idx_cols}, but found "
                f"{df.index.names=}"
            )
        if not df.index.is_unique:
            raise AssertionError("Found non-unique index in exploded metadata.")

    @classmethod
    def from_exploded_meta(
        cls,
        source_table: str,
        xbrl_factoid: str,
        exploded_meta: pd.DataFrame,
        weight: float = np.nan,
        tags_df: pd.DataFrame | None = None,
        propagate_weights: bool = True,
    ) -> "XbrlCalculationTreeFerc1":
        """Construct a complete calculation tree based on exploded metadata.

        Args:
            source_table: The table within which we are searching for the XBRL fact,
                required as many appear in multiple tables.
            xbrl_factoid: Equivalent to the "name" field in the calculation metadata.
                Potentially renamed from the originally reported XBRL value.
            exploded_meta: A dataframe containing metadata for all of the facts
                referenced by the exploded tables. Must be indexed by the columns
                (table_name, xbrl_factoid) and the index must be unique.
            weight: The weight associated with this node in its calculation. Must be
                passed in because it is only available when we have access to the
                calculation metadata.

        Returns:
            The root node of a calculation tree, potentially referencing child nodes.
        """
        cls._check_index(exploded_meta)

        # Index to look up the particular factoid we're building the node for:
        idx = (source_table, xbrl_factoid)
        # Read in the calculations (if any) that define the node:
        calculations = json.loads(exploded_meta.at[idx, "calculations"])
        children = []
        for calc in calculations:
            if len(calc["source_tables"]) != 1:
                raise AssertionError(
                    "Generating the calculation tree for exploded tables requires all "
                    "xbrl_factoids to have a unique source table, but found "
                    f"{calc=}"
                )
            children.append(
                XbrlCalculationTreeFerc1.from_exploded_meta(
                    weight=calc["weight"],
                    xbrl_factoid=calc["name"],
                    source_table=calc["source_tables"][0],
                    exploded_meta=exploded_meta,
                )
            )

        tree = XbrlCalculationTreeFerc1(
            source_table=source_table,
            xbrl_factoid=xbrl_factoid,
            weight=weight,
            xbrl_factoid_original=exploded_meta.at[idx, "xbrl_factoid_original"],
            children=children,
        )
        if propagate_weights:
            tree = tree.propagate_weights()
        if tags_df is not None:
            tree = tree.add_tags_from_df(tags_df).propagate_tags()

        return tree

    def propagate_weights(
        self: Self, parent_weight: float = 1.0
    ) -> "XbrlCalculationTreeFerc1":
        """Multiply child node weights by the product of the weights of all parents.

        Because we are going to remove all intermediate calculations and work only with
        the values reported in leaf-nodes, we need to distribute any calculation weights
        from parent nodes to their children. E.g. if a calculated value contains several
        positive numbers, but itself has a weight of -1.0 then each of the subcomponents
        needs to be multiplied by -1.0 in order to preserve the appropriate sign of the
        child values in the higher level calculation.
        """
        new_weight = parent_weight * self.weight
        new_children = [
            node.propagate_weights(parent_weight=new_weight) for node in self.children
        ]
        return XbrlCalculationTreeFerc1(
            **self.dict() | {"weight": new_weight, "children": new_children}
        )

    def add_tags_from_df(
        self: Self, tags_df: pd.DataFrame
    ) -> "XbrlCalculationTreeFerc1":
        """Read tags from dataframe and apply to appropriate nodes in the tree.

        Args:
            tags_df: A dataframe indexed by ("table_name", "xbrl_factoid") potentially
                containing several additional categorical columns. The name of each
                column will be used as the key in the metadata tag dictionary. For each
                node in the tree, the row in this dataframe corresponding to the node's
                ``source_table`` and ``xbrl_factoid`` will be used to look up the value
                of the tags associated with the node.
        """
        self._check_index(tags_df)
        try:
            # Look up the tags to apply to this node
            new_tags = tags_df.loc[(self.source_table, self.xbrl_factoid)].to_dict()
        #  If it has no tags that's fine. Catch the key error and continue.
        except KeyError:
            new_tags = {}
        logger.debug(
            f"Found {new_tags=} for node {(self.source_table, self.xbrl_factoid)}"
        )

        # Construct a copy of this node with the new tags applied.
        new_tags = self.tags | new_tags
        new_children = [node.add_tags_from_df(tags_df) for node in self.children]
        return XbrlCalculationTreeFerc1(
            **self.dict() | {"tags": new_tags, "children": new_children}
        )

    def propagate_tags(
        self: Self, tags: dict[str, str] = {}
    ) -> "XbrlCalculationTreeFerc1":
        """Propagate metadata tags to all descendants of tagged notes in the tree.

        TODO: Check that there are no inconsistencies between parent & child node tags.
        """
        new_tags = self.tags | tags
        new_children = [node.propagate_tags(new_tags) for node in self.children]
        return XbrlCalculationTreeFerc1(
            **self.dict() | {"tags": new_tags, "children": new_children}
        )

    def to_networkx(self: Self) -> nx.DiGraph:
        """Convert the tree to a an undirected NetworkX graph.

        Given a node, treat that node as the root of a tree, and construct an undirected
        :class:`networkx.Graph` representing the node and all of its children. The
        tuple of strings (source_table, xbrl_factoid) is used as the ID for each node in
        the graph.
        """
        digraph = nx.DiGraph()  # noqa: N806
        node_id = (self.source_table, self.xbrl_factoid)
        # Add the current node and all of its attributes:
        digraph.add_node(
            node_id,
            xbrl_factoid_original=self.xbrl_factoid_original,
            weight=self.weight,
            tags=self.tags,
        )
        for child in self.children:
            # Add an edge from the current node to each of its children:
            child_id = (child.source_table, child.xbrl_factoid)
            digraph.add_edge(node_id, child_id)
            # Recursively add all the child nodes and edges:
            child_graph = child.to_networkx()
            digraph.add_nodes_from(child_graph.nodes)
            nx.set_node_attributes(digraph, dict(child_graph.nodes))
            digraph.add_edges_from(child_graph.edges)
        return digraph

    def to_leafy_meta(self: Self) -> pd.DataFrame:
        """Convert an XbrlCalculationTree back into a pandas dataframe.

        - Indexed by (source table, xbrl_factoid)
        - Each family of tags gets its own column
        - Weights should reflect updated / propagated values
        """
        G = self.to_networkx()  # noqa: N806

        leaves = [n for n, d in G.out_degree() if d == 0]
        H = nx.DiGraph()  # noqa: N806
        H.add_nodes_from(leaves)
        nx.set_node_attributes(H, dict(G.nodes))

        rows = []
        for node in H.nodes:
            row = pd.concat(
                [
                    pd.DataFrame(
                        {"source_table": [node[0]], "xbrl_factoid": [node[1]]}
                    ),
                    pd.json_normalize(H.nodes(data=True)[node]),
                ],
                axis="columns",
            )
            rows.append(row)
        leafy_meta = pd.concat(rows)

        return (
            leafy_meta.set_index(["source_table", "xbrl_factoid"], drop=True)
            .sort_index()
            .convert_dtypes()
        )


class XbrlCalculationForestFerc1(BaseModel):
    """A class representing a collection of :class:`XbrlCalculationTreeFerc1`."""

    trees: list[XbrlCalculationTreeFerc1] = []

    @classmethod
    def from_exploded_meta(
        cls,
        source_tables: list[str],
        xbrl_factoids: list[str],
        exploded_meta: pd.DataFrame,
        propagate_weights: bool = True,
        tags_df: pd.DataFrame | None = None,
    ) -> "XbrlCalculationForestFerc1":
        """Build several :class:`XbrlCalculationTreeFerc1`'s from exploded metadata.

        - Build trees from all of the seeds.
        - Convert all of the trees into NX graphs.
        - Add all the nodes from those graphs into a single graph.
        - Verify that this graph is a forest (one or more trees)
        - Identify all connected components of this graph.
        - Verify that the root nodes of each of these trees is one of the seeds.
        - Convert these Graphs back into Calculation Trees.
        """
        trees = [
            XbrlCalculationTreeFerc1.from_exploded_meta(
                source_table=source_table,
                xbrl_factoid=xbrl_factoid,
                exploded_meta=exploded_meta,
            )
            for source_table, xbrl_factoid in zip(source_tables, xbrl_factoids)
        ]
        forest = cls.to_networkx(trees)

        # Identify the roots of each tree and verify it's one of the input seeds
        roots = [n for n, in_deg in forest.in_degree() if in_deg == 0]
        logger.debug(f"{roots=}")
        leaves = [n for n, out_deg in forest.out_degree() if out_deg == 0]
        logger.debug(f"{leaves=}")

        # - The easiest way to "convert" the NX Graph back to our class may be to just
        #   rebuild the trees, but using only the newly identified root nodes as seeds.
        # - For visualization purposes it will be helpful to have all of the calculation
        #   metadata available within the NX Graph
        minimal_trees = [
            XbrlCalculationTreeFerc1.from_exploded_meta(
                source_table=source_table,
                xbrl_factoid=xbrl_factoid,
                exploded_meta=exploded_meta,
                weight=1.0,
                propagate_weights=propagate_weights,
                tags_df=tags_df,
            )
            for source_table, xbrl_factoid in roots
        ]
        return XbrlCalculationForestFerc1(trees=minimal_trees)

    @classmethod
    def to_networkx(cls, trees: list["XbrlCalculationTreeFerc1"]) -> nx.DiGraph:
        """Convert a calculation forest into a :class:`networkx.DiGraph`.

        It's possible that used nodes from multiple levels within a single calculation
        tree as seeds when building the trees. This method adds all the nodes from all
        of the calculation trees into the same graph, which should be a collection of
        several distinct, disconnected trees (a forest). Adding all of the nodes to the
        same graph will deduplicate nodes, and allow us to identify the minimal set of
        root nodes required to reproduce the tree which involves all of the nodes we
        used as seeds. The leaf nodes which are part of that forest represent all of the
        reported facts that are required to calculate the root nodes which we are
        interested in.
        """
        forest = nx.DiGraph()
        for tree in trees:
            nx_tree = tree.to_networkx()
            assert nx.is_tree(nx_tree)
            forest.add_nodes_from(nx_tree.nodes)
            nx.set_node_attributes(nx_tree, dict(nx_tree.nodes))
            forest.add_edges_from(nx_tree.edges)

        assert nx.is_forest(forest)
        components = list(nx.connected_components(forest.to_undirected()))
        logger.info(f"Found {len(components)} calculation trees")
        return forest

    def to_leafy_meta(self: Self) -> pd.DataFrame:
        """Generate a metadataframe from the leaves of an XBRL Calculation Forest."""
        dfs = []
        for tree in self.trees:
            new_df = tree.to_leafy_meta()
            new_df["root_table"] = tree.source_table
            new_df["root_xbrl_factoid"] = tree.xbrl_factoid
            dfs.append(new_df)
        return pd.concat(dfs).sort_index()


class NodeId(NamedTuple):
    """The source table and XBRL factoid identifying a node in a calculation tree."""

    source_table: str
    xbrl_factoid: str


class NewXbrlCalcuationForestFerc1(BaseModel):
    """A class for manipulating groups of hierarchically nested XBRL calculations."""

    exploded_meta: pd.DataFrame
    seeds: list[NodeId] = []
    tags: pd.DataFrame = pd.DataFrame()

    class Config:
        """Allow the class to store a dataframe."""

        arbitrary_types_allowed = True

    @validator("exploded_meta", "tags")
    def ensure_correct_dataframe_index(cls, v):
        """Ensure that dataframe is indexed by table_name and xbrl_factoid."""
        idx_cols = ["table_name", "xbrl_factoid"]
        if v.index.names == idx_cols:
            return v
        missing_idx_cols = [col for col in idx_cols if col not in v.columns]
        if missing_idx_cols:
            raise ValueError(
                f"Exploded metadataframes must be indexed by {idx_cols}, but these "
                f"columns were missing: {missing_idx_cols=}"
            )
        drop = v.index.names is None
        return v.set_index(idx_cols, drop=drop)

    @validator("exploded_meta", "tags")
    def dataframe_has_unique_index(cls, v):
        """Ensure that exploded_meta has a unique index."""
        if not v.index.is_unique:
            raise ValueError("DataFrame has non-unique index values.")
        return v

    @validator("seeds")
    def seeds_within_bounds(cls, v, values):
        """Ensure that all seeds are present within exploded_meta index."""
        bad_seeds = [seed for seed in v if seed not in values["exploded_meta"].index]
        if bad_seeds:
            raise ValueError(f"Seeds missing from exploded_meta index: {bad_seeds=}")
        return v

    @staticmethod
    def exploded_meta_to_nx_forest(  # noqa: C901
        exploded_meta: pd.DataFrame,
        tags: pd.DataFrame,
    ) -> nx.DiGraph:
        """Construct a :class:`networkx.DiGraph` of all calculations in exploded_meta.

        - Add all edges implied by the calculations found in exploded_meta.
        - Compile node attributes from exploded_meta and add it the the nodes in the
          forest that has been compiled.
        """
        forest: nx.DiGraph = nx.DiGraph()
        attrs = {}
        for row in exploded_meta.itertuples():
            from_node = NodeId(*row.Index)
            if not attrs.get(from_node, False):
                attrs[from_node] = {}
            if not attrs[from_node].get("xbrl_factoid_original", False):
                attrs[from_node] |= {"xbrl_factoid_original": row.xbrl_factoid_original}
            else:
                assert (
                    attrs[from_node]["xbrl_factoid_original"]
                    == row.xbrl_factoid_original
                )
            try:
                attrs[from_node]["tags"] = dict(tags.loc[from_node])
            except KeyError:
                attrs[from_node]["tags"] = {}
            calcs = json.loads(row.calculations)
            for calc in calcs:
                assert len(calc["source_tables"]) == 1
                to_node = NodeId(calc["source_tables"][0], calc["name"])
                if not attrs.get(to_node, False):
                    attrs[to_node] = {}
                if not attrs[to_node].get("weight", False):
                    attrs[to_node] |= {"weight": calc["weight"]}
                else:
                    assert attrs[to_node]["weight"] == calc["weight"]
                try:
                    attrs[to_node]["tags"] = dict(tags.loc[to_node])
                except KeyError:
                    attrs[to_node]["tags"] = {}
                forest.add_edge(from_node, to_node)
        nx.set_node_attributes(forest, attrs)

        # This is a temporary hack. These xbrl_factoid values need to have metadata
        # created and injected by the process_xbrl_metadata() method in the FERC 1
        # table transformers... We created them to refer to data that only appears in
        # the DBF data.
        bad_nodes = [
            NodeId("balance_sheet_assets_ferc1", "special_funds_all"),
        ]
        forest.remove_nodes_from(bad_nodes)
        # The resulting graph should always be a collection of several trees, however
        # # it turns out that it's not, so we need to fix something.
        if not nx.is_forest(forest):
            logger.error("Calculations in Exploded Metadata do not describe a forest!")
        return forest

    @property
    def nx_forest(self: Self) -> nx.DiGraph:
        """Construct a minimal calculation forest that includes all of our seed nodes.

        - Identify the connected components within the calculation forest described by
          the full exploded_meta dataframe.
        - Remove nodes from any connected component that doesn't intersect with our
          seed nodes.
        - Regenerate a pruned forest with directed edges and full node attributes that
          only includes connected components that intersect with our seed nodes.
        """
        full_forest: nx.DiGraph = self.exploded_meta_to_nx_forest(
            exploded_meta=self.exploded_meta,
            tags=self.tags,
        )
        undirected_trees = list(nx.connected_components(full_forest.to_undirected()))
        logger.info(f"Full calculation forest contains {len(undirected_trees)} trees.")
        pruned_forest_nodes = set()
        for tree in undirected_trees:
            if set(self.seeds).intersection(tree):
                pruned_forest_nodes = pruned_forest_nodes.union(tree)
        pruned_forest: nx.DiGraph = self.exploded_meta_to_nx_forest(
            exploded_meta=self.exploded_meta.loc[list(pruned_forest_nodes)],
            tags=self.tags,
        )
        pruned_trees = list(nx.connected_components(pruned_forest.to_undirected()))
        logger.info(f"Pruned calculation forest contains {len(pruned_trees)} trees.")
        return pruned_forest

    @property
    def leafy_meta(self: Self) -> pd.DataFrame:
        """Identify leaf facts and compile their metadata.

        - identify the root and leaf nodes of those minimal trees
        - adjust the weights associated with the leaf nodes to equal the
          product of the weights of all their ancestors.
        - Set leaf node tags to be the union of all the tags associated
          with all of their ancestors.

        Leaf metadata in the output dataframe includes:

        - The ID of the leaf node itself (this is the index).
        - The ID of the root node the leaf is descended from.
        - What tags the leaf has inherited from its ancestors.
        - The leaf node's xbrl_factoid_original
        - The weight associated with the leaf, in relation to its root.
        """
        # Create a copy of the graph representation since we are going to mutate it.
        nx_forest = self.nx_forest
        pruned_forest = nx.DiGraph()
        pruned_forest.add_nodes_from(nx_forest.nodes(data=True))
        pruned_forest.add_edges_from(nx_forest.edges(data=True))

        # Construct a dataframe that links the leaf node IDs to their root nodes:
        leaves = [n for n, out_deg in pruned_forest.out_degree() if out_deg == 0]
        roots = [n for n, in_deg in pruned_forest.in_degree() if in_deg == 0]
        leaf_to_root_map = {
            leaf: root
            for leaf in leaves
            for root in roots
            if leaf in nx.descendants(pruned_forest, root)
        }
        leaves_df = pd.DataFrame(list(leaf_to_root_map.keys()))
        roots_df = pd.DataFrame(list(leaf_to_root_map.values())).rename(
            columns={"source_table": "root_table", "xbrl_factoid": "root_xbrl_factoid"}
        )
        leafy_meta = pd.concat([leaves_df, roots_df], axis="columns")

        # Propagate tags and weights to leaf nodes
        leaf_rows = []
        for leaf in leaves:
            leaf_tags = {}
            leaf_weight = pruned_forest.nodes[leaf].get("weight", 1.0)
            for node in nx.ancestors(pruned_forest, leaf):
                leaf_tags |= pruned_forest.nodes[node]["tags"]
                # Root nodes have no weight because they don't come from calculations
                # We assign them a weight of 1.0
                if not pruned_forest.nodes[node].get("weight", False):
                    assert node in roots
                    node_weight = 1.0
                else:
                    node_weight = pruned_forest.nodes[node]["weight"]
                leaf_weight *= node_weight

            # Construct a dictionary describing the leaf node and convert it into a
            # single row DataFrame. This makes adding arbitrary tags easy.
            leaf_attrs = {
                "xbrl_factoid_original": pruned_forest.nodes[leaf][
                    "xbrl_factoid_original"
                ],
                "weight": leaf_weight,
                "tags": leaf_tags,
                "source_table": leaf.source_table,
                "xbrl_factoid": leaf.xbrl_factoid,
            }
            leaf_rows.append(pd.json_normalize(leaf_attrs))

        # Combine the two dataframes we've constructed above:
        return (
            pd.merge(leafy_meta, pd.concat(leaf_rows), validate="one_to_one")
            .convert_dtypes()
            .set_index(["source_table", "xbrl_factoid"])
        )

    @property
    def root_calculations(self: Self) -> pd.DataFrame:
        """Produce an exploded metadataframe containing only roots and leaves.

        This dataframe has a format similar to exploded_meta and can be used in
        conjunction with the exploded data to verify that the root values can still
        be correctly calculated from the leaf values.

        """

        def leafy_meta_to_calculations(df: pd.DataFrame) -> str:
            return json.dumps(
                [
                    {
                        "name": row.xbrl_factoid,
                        "weight": float(row.weight),
                        "xbrl_factoid_original": row.xbrl_factoid_original,
                        "source_tables": [row.source_table],
                    }
                    for row in df.itertuples()
                ]
            )

        root_calcs: pd.DataFrame = (
            self.leafy_meta.reset_index()
            .groupby(["root_table", "root_xbrl_factoid"], as_index=False)
            .apply(leafy_meta_to_calculations)
        )
        root_calcs.columns = ["root_table", "root_xbrl_factoid", "calculations"]
        return root_calcs

    def plot(self: Self) -> None:
        """Visualize the calculation forest and its attributes."""
        ...

    def prune_and_annotate_exploded_data(
        self: Self,
        exploded_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """Use the calculation forest to prune the exploded dataframe.

        - Drop all rows that don't correspond to either root or leaf facts.
        - Verify that the reported root values can still be generated by calculations
          that only refer to leaf values.
        - Merge the leafy metadata onto the exploded data, keeping only those rows
          which refer to the leaf facts.
        - Use the leaf weights to adjust the reported data values, and then drop the
          leaf weights.

        This method could either live here, or in the Exploder class, which would also
        have access to exploded_meta, exploded_data, and the calculation forest.

        """
        ...
