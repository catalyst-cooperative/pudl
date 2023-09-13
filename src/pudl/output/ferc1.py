"""A collection of denormalized FERC assets and helper functions."""
import numpy as np
import pandas as pd
from dagster import Field, asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def _out_ferc1__yearly_plants_utilities(
    core_pudl__assn_plants_ferc1: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized table containing FERC plant and utility names and IDs."""
    return pd.merge(
        core_pudl__assn_plants_ferc1,
        core_pudl__assn_utilities_ferc1,
        on="utility_id_ferc1",
    )


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def _out_ferc1__yearly_steam_plants(
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
    core_ferc1__yearly_plants_steam: pd.DataFrame,
) -> pd.DataFrame:
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting
    utility's name, and the PUDL ID for the plant and utility for readability
    and integration with other tables that have PUDL IDs.
    Also calculates ``capacity_factor`` (based on ``net_generation_mwh`` &
    ``capacity_mw``)

    Args:
        _out_ferc1__yearly_plants_utilities: Denormalized dataframe of FERC Form 1 plants and
            utilities data.
        core_ferc1__yearly_plants_steam: The normalized FERC Form 1 steam table.

    Returns:
        A DataFrame containing useful fields from the FERC Form 1 steam table.
    """
    steam_df = (
        core_ferc1__yearly_plants_steam.merge(
            _out_ferc1__yearly_plants_utilities,
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
def _out_ferc1__yearly_small_plants(
    core_ferc1__yearly_plants_small: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 small plants."""
    plants_small_df = (
        core_ferc1__yearly_plants_small.merge(
            _out_ferc1__yearly_plants_utilities,
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
def _out_ferc1__yearly_hydro_plants(
    core_ferc1__yearly_plants_hydro: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 hydro plants."""
    plants_hydro_df = (
        core_ferc1__yearly_plants_hydro.merge(
            _out_ferc1__yearly_plants_utilities,
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
def _out_ferc1__yearly_pumped_storage_plants(
    core_ferc1__yearly_plants_pumped_storage: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Pumped Storage plant data."""
    pumped_storage_df = (
        core_ferc1__yearly_plants_pumped_storage.merge(
            _out_ferc1__yearly_plants_utilities,
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
def out_ferc1__yearly_fuel(
    core_ferc1__yearly_fuel: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
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
        core_ferc1__yearly_fuel.assign(
            fuel_consumed_mmbtu=lambda x: x["fuel_consumed_units"]
            * x["fuel_mmbtu_per_unit"],
            fuel_consumed_total_cost=lambda x: x["fuel_consumed_units"]
            * x["fuel_cost_per_unit_burned"],
        )
        .merge(
            _out_ferc1__yearly_plants_utilities,
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
def out_ferc1__yearly_purchased_power(
    core_ferc1__yearly_purchased_power: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    purchased_power_df = core_ferc1__yearly_purchased_power.merge(
        core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
def out_ferc1__yearly_plant_in_service(
    core_ferc1__yearly_plant_in_service: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Electric Plant in Service data."""
    pis_df = core_ferc1__yearly_plant_in_service.merge(
        core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return pis_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_balance_sheet_assets(
    core_ferc1__yearly_balance_sheet_assets: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 balance sheet assets data."""
    out_ferc1__yearly_balance_sheet_assets = (
        core_ferc1__yearly_balance_sheet_assets.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    )
    return out_ferc1__yearly_balance_sheet_assets


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_balance_sheet_liabilities(
    core_ferc1__yearly_balance_sheet_liabilities: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 balance_sheet liabilities data."""
    out_ferc1__yearly_balance_sheet_liabilities = (
        core_ferc1__yearly_balance_sheet_liabilities.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    )
    return out_ferc1__yearly_balance_sheet_liabilities


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_cash_flow(
    core_ferc1__yearly_cash_flow: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 cash flow data."""
    out_ferc1__yearly_cash_flow = core_ferc1__yearly_cash_flow.merge(
        core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_cash_flow


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_depreciation_amortization_summary(
    core_ferc1__yearly_depreciation_amortization_summary: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 depreciation amortization data."""
    out_ferc1__yearly_depreciation_amortization_summary = (
        core_ferc1__yearly_depreciation_amortization_summary.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_depreciation_amortization_summary


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electric_energy_dispositions(
    core_ferc1__yearly_electric_energy_dispositions: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 energy dispositions data."""
    out_ferc1__yearly_electric_energy_dispositions = (
        core_ferc1__yearly_electric_energy_dispositions.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_electric_energy_dispositions


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electric_energy_sources(
    core_ferc1__yearly_electric_energy_sources: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_electric_energy_sources = (
        core_ferc1__yearly_electric_energy_sources.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    )
    return out_ferc1__yearly_electric_energy_sources


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electric_operating_expenses(
    core_ferc1__yearly_electric_operating_expenses: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_electric_operating_expenses = (
        core_ferc1__yearly_electric_operating_expenses.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    )
    return out_ferc1__yearly_electric_operating_expenses


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electric_operating_revenues(
    core_ferc1__yearly_electric_operating_revenues: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_electric_operating_revenues = (
        core_ferc1__yearly_electric_operating_revenues.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    )
    return out_ferc1__yearly_electric_operating_revenues


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electric_plant_depreciation_changes(
    core_ferc1__yearly_electric_plant_depreciation_changes: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_electric_plant_depreciation_changes = (
        core_ferc1__yearly_electric_plant_depreciation_changes.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_electric_plant_depreciation_changes


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electric_plant_depreciation_functional(
    core_ferc1__yearly_electric_plant_depreciation_functional: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_electric_plant_depreciation_functional = (
        core_ferc1__yearly_electric_plant_depreciation_functional.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_electric_plant_depreciation_functional


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_electricity_sales_by_rate_schedule(
    core_ferc1__yearly_electricity_sales_by_rate_schedule: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_electricity_sales_by_rate_schedule = (
        core_ferc1__yearly_electricity_sales_by_rate_schedule.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_electricity_sales_by_rate_schedule


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_income_statement_ferc1(
    core_ferc1__yearly_income_statement: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_income_statement = core_ferc1__yearly_income_statement.merge(
        core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_income_statement


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_other_regulatory_liabilities(
    core_ferc1__yearly_other_regulatory_liabilities: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_other_regulatory_liabilities = (
        core_ferc1__yearly_other_regulatory_liabilities.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_other_regulatory_liabilities


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_retained_earnings(
    core_ferc1__yearly_retained_earnings: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_retained_earnings = core_ferc1__yearly_retained_earnings.merge(
        core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_retained_earnings


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_transmission_statistics(
    core_ferc1__yearly_transmission_statistics: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_transmission_statistics = (
        core_ferc1__yearly_transmission_statistics.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    return out_ferc1__yearly_transmission_statistics


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_utility_plant_summary(
    core_ferc1__yearly_utility_plant_summary: pd.DataFrame,
    core_pudl__assn_utilities_ferc1: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe of FERC Form 1 Purchased Power data."""
    out_ferc1__yearly_utility_plant_summary = (
        core_ferc1__yearly_utility_plant_summary.merge(
            core_pudl__assn_utilities_ferc1, on="utility_id_ferc1"
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
    )
    return out_ferc1__yearly_utility_plant_summary


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def out_ferc1__yearly_all_plants(
    _out_ferc1__yearly_steam_plants: pd.DataFrame,
    _out_ferc1__yearly_small_plants: pd.DataFrame,
    _out_ferc1__yearly_hydro_plants: pd.DataFrame,
    _out_ferc1__yearly_pumped_storage_plants: pd.DataFrame,
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
    steam_df = _out_ferc1__yearly_steam_plants.rename(
        columns={"opex_plants": "opex_plant"}
    )

    # Prep hydro tables (Add this to the meta data later)
    logger.debug("prepping hydro tables")
    hydro_df = _out_ferc1__yearly_hydro_plants.rename(
        columns={"project_num": "ferc_license_id"}
    )
    pump_df = _out_ferc1__yearly_pumped_storage_plants.rename(
        columns={"project_num": "ferc_license_id"}
    )

    # Combine all the tables together
    logger.debug("combining all tables")
    all_df = (
        pd.concat([steam_df, _out_ferc1__yearly_small_plants, hydro_df, pump_df])
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
def out_ferc1__yearly_fuel_by_plant(
    context,
    core_ferc1__yearly_fuel: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around
    :func:`pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1`
    which calculates some summary values on a per-plant basis (as indicated
    by ``utility_id_ferc1`` and ``plant_name_ferc1``) related to fuel
    consumption.

    Args:
        context: Dagster context object
        core_ferc1__yearly_fuel: Normalized FERC fuel table.
        _out_ferc1__yearly_plants_utilities: Denormalized table of FERC1 plant & utility IDs.

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
    core_ferc1__yearly_fuel["fuel_type_code_pudl"] = core_ferc1__yearly_fuel[
        "fuel_type_code_pudl"
    ].astype(str)

    fuel_categories = list(
        pudl.transform.ferc1.FuelFerc1TableTransformer()
        .params.categorize_strings["fuel_type_code_pudl"]
        .categories.keys()
    )

    fbp_df = (
        core_ferc1__yearly_fuel.pipe(drop_other_fuel_types)
        .pipe(
            pudl.analysis.classify_plants_ferc1.fuel_by_plant_ferc1,
            fuel_categories=fuel_categories,
            thresh=thresh,
        )
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_float_nulls)
        .pipe(pudl.analysis.classify_plants_ferc1.revert_filled_in_string_nulls)
        .merge(
            _out_ferc1__yearly_plants_utilities,
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
