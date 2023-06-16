"""A collection of denormalized FERC assets and helper functions."""
import json
from typing import Self

import networkx as nx
import numpy as np
import pandas as pd
from dagster import AssetIn, AssetsDefinition, Field, Mapping, asset
from pydantic import BaseModel

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
                        "xbrl_factoid_original",
                        "intra_table_calc_flag",
                    ]
                ]
                .assign(table_name=table_name)
            )
            tbl_metas.append(tbl_meta)
        return pd.concat(tbl_metas).reset_index()


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
            .pipe(remove_inter_table_calc_duplication)
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
        exploded = pd.concat(explosion_tables).merge(
            metadata_exploded,
            how="left",
            on=["xbrl_factoid", "table_name"],
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


def remove_inter_table_calc_duplication(exploded: pd.DataFrame) -> pd.DataFrame:
    """Treat the duplication in the inter-table calculations.

    There are several possible ways to remove the duplication in the inter table calcs.
    See issue #2622. Right now we are doing the simplest option which removes some level
    of detail.
    """
    logger.info("Explode: Doing inter-table calc deduplications stuff.")
    inter_table_calcs = (
        exploded[
            (exploded.row_type_xbrl == "calculated_value")
            & ~exploded.intra_table_calc_flag
        ][["table_name", "xbrl_factoid", "calculations"]]
        .drop_duplicates()
        .set_index("xbrl_factoid")
    )
    inter_table_components_in_intra_table_calc = (
        pudl.helpers.dedupe_n_flatten_list_of_lists(
            inter_table_calcs.apply(
                lambda x: find_intra_table_components_to_remove(
                    x.calculations, x.table_name
                ),
                axis=1,
            )
        )
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
class Ferc1XbrlCalculationNode(BaseModel):
    """A node in a FERC Form 1 XBRL calculation tree.

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
    """The weight associated with the XBRL Fact in its source table calculation.

    This metadata about the is not currently used for performing calculations.
    """
    children: list["Ferc1XbrlCalculationNode"] = []
    """The subcomponents required to calculate the value of this fact, if any.

    This list will be empty for leaf nodes (reported values) and for calculated values
    that lie outside the list of tables being used in the explosion.
    """

    @classmethod
    def from_exploded_meta(
        cls,
        xbrl_factoid: str,
        source_table: str,
        exploded_meta: pd.DataFrame,
        weight: float = 1.0,
    ) -> "Ferc1XbrlCalculationNode":
        """Construct a complete calculation tree based on exploded metadata.

        Args:
            xbrl_factoid: Equivalent to the "name" field in the calculation metadata.
                Potentially renamed from the originally reported XBRL value.
            source_table: The table within which we are searching for the XBRL fact,
                required as many appear in multiple tables.
            exploded_meta: A dataframe containing metadata for all of the facts
                referenced by the exploded tables. Must be indexed by the columns
                (table_name, xbrl_factoid) and the index must be unique.
            weight: The weight associated with this node in its calculation. Must be
                passed in because it is only available when we have access to the
                calculation metadata. Defaults to 1.0 so that we can create the root
                node in a tree (which is not embedded in any calculation) without having
                to specify the weight.

        Returns:
            A single node of the calculation tree, potentially referencing child nodes.
        """
        idx_cols = ["table_name", "xbrl_factoid"]
        if exploded_meta.index.names != idx_cols:
            raise AssertionError(
                f"Exploded metadata must be indexed by {idx_cols}, but found "
                f"{exploded_meta.index.names=}"
            )
        if not exploded_meta.index.is_unique:
            raise AssertionError("Found non-unique index in exploded metadata.")

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
                Ferc1XbrlCalculationNode.from_exploded_meta(
                    weight=calc["weight"],
                    xbrl_factoid=calc["name"],
                    source_table=calc["source_tables"][0],
                    exploded_meta=exploded_meta,
                )
            )

        return Ferc1XbrlCalculationNode(
            source_table=source_table,
            weight=weight,
            xbrl_factoid=xbrl_factoid,
            xbrl_factoid_original=exploded_meta.at[idx, "xbrl_factoid_original"],
            children=children,
        )

    def to_networkx(self: Self) -> nx.Graph:
        """Convert the tree to a an undirected NetworkX graph.

        Given a node, treat that node as the root of a tree, and construct an undirected
        :class:`networkx.Graph` representing the node and all of its children. The
        tuple of strings (source_table, xbrl_factoid) is used as the ID for each node in
        the graph.
        """
        ...

    def is_inter_table(self: Self) -> bool:
        """Determine if the node refers to values from multiple tables.

        Enumerate all the source tables associated with the node, including its own
        source table, and those of all of its immediate descendants. If there is only
        one unique value, return False. If there are multiple values, return True.
        """
        ...
