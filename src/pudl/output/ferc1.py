"""A collection of denormalized FERC assets and helper functions."""
import importlib
import re
from copy import deepcopy
from functools import cached_property
from typing import Any, Literal, NamedTuple, Self

import networkx as nx
import numpy as np
import pandas as pd
from dagster import AssetIn, AssetsDefinition, Field, Mapping, asset
from matplotlib import pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from pandas._libs.missing import NAType as pandas_NAType
from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationInfo,
    field_validator,
    model_validator,
)

import pudl
from pudl.transform.ferc1 import (
    GroupMetricChecks,
    GroupMetricTolerances,
    MetricTolerances,
)

logger = pudl.logging_helpers.get_logger(__name__)


EXPLOSION_CALCULATION_TOLERANCES: dict[str, GroupMetricChecks] = {
    "income_statement_ferc1": GroupMetricChecks(
        groups_to_check=[
            "ungrouped",
            "report_year",
            "xbrl_factoid",
            "utility_id_ferc1",
        ],
        group_metric_tolerances=GroupMetricTolerances(
            ungrouped=MetricTolerances(
                error_frequency=0.02,
                relative_error_magnitude=0.04,
                null_calculated_value_frequency=1.0,
            ),
            report_year=MetricTolerances(
                error_frequency=0.036,
                relative_error_magnitude=0.048,
                null_calculated_value_frequency=1.0,
            ),
            xbrl_factoid=MetricTolerances(
                error_frequency=0.35,
                relative_error_magnitude=0.17,
                null_calculated_value_frequency=1.0,
            ),
            utility_id_ferc1=MetricTolerances(
                error_frequency=0.13,
                relative_error_magnitude=0.42,
                null_calculated_value_frequency=1.0,
            ),
        ),
    ),
    "balance_sheet_assets_ferc1": GroupMetricChecks(
        groups_to_check=[
            "ungrouped",
            "report_year",
            "xbrl_factoid",
            "utility_id_ferc1",
        ],
        group_metric_tolerances=GroupMetricTolerances(
            ungrouped=MetricTolerances(
                error_frequency=0.014,
                relative_error_magnitude=0.04,
                null_calculated_value_frequency=1.0,
            ),
            report_year=MetricTolerances(
                error_frequency=0.12,
                relative_error_magnitude=0.04,
                null_calculated_value_frequency=1.0,
            ),
            xbrl_factoid=MetricTolerances(
                error_frequency=0.37,
                relative_error_magnitude=0.22,
                null_calculated_value_frequency=1.0,
            ),
            utility_id_ferc1=MetricTolerances(
                error_frequency=0.21,
                relative_error_magnitude=0.26,
                null_calculated_value_frequency=1.0,
            ),
        ),
    ),
    "balance_sheet_liabilities_ferc1": GroupMetricChecks(
        groups_to_check=[
            "ungrouped",
            "report_year",
            "xbrl_factoid",
            "utility_id_ferc1",
        ],
        group_metric_tolerances=GroupMetricTolerances(
            ungrouped=MetricTolerances(
                error_frequency=0.028,
                relative_error_magnitude=0.019,
                null_calculated_value_frequency=1.0,
            ),
            report_year=MetricTolerances(
                error_frequency=0.028,
                relative_error_magnitude=0.04,
                null_calculated_value_frequency=1.0,
            ),
            xbrl_factoid=MetricTolerances(
                error_frequency=0.028,
                relative_error_magnitude=0.019,
                null_calculated_value_frequency=1.0,
            ),
            utility_id_ferc1=MetricTolerances(
                error_frequency=0.063,
                relative_error_magnitude=0.04,
                null_calculated_value_frequency=1.0,
            ),
        ),
    ),
}

MANUAL_DBF_METADATA_FIXES: dict[str, dict[str, str]] = {
    "less_noncurrent_portion_of_allowances": {
        "dbf2020_row_number": 53,
        "dbf2020_table_name": "f1_comp_balance_db",
        "dbf2020_row_literal": "(Less) Noncurrent Portion of Allowances",
    },
    "less_derivative_instrument_assets_long_term": {
        "dbf2020_row_number": 64,
        "dbf2020_table_name": "f1_comp_balance_db",
        "dbf2020_row_literal": "(Less) Long-Term Portion of Derivative Instrument Assets (175)",
    },
    "less_derivative_instrument_assets_hedges_long_term": {
        "dbf2020_row_number": 66,
        "dbf2020_table_name": "f1_comp_balance_db",
        "dbf2020_row_literal": "(Less) Long-Term Portion of Derivative Instrument Assets - Hedges (176)",
    },
    "less_long_term_portion_of_derivative_instrument_liabilities": {
        "dbf2020_row_number": 51,
        "dbf2020_table_name": "f1_bal_sheet_cr",
        "dbf2020_row_literal": "(Less) Long-Term Portion of Derivative Instrument Liabilities",
    },
    "less_long_term_portion_of_derivative_instrument_liabilities_hedges": {
        "dbf2020_row_number": 53,
        "dbf2020_table_name": "f1_bal_sheet_cr",
        "dbf2020_row_literal": "(Less) Long-Term Portion of Derivative Instrument Liabilities-Hedges",
    },
    "other_miscellaneous_operating_revenues": {
        "dbf2020_row_number": 25,
        "dbf2020_table_name": "f1_elctrc_oper_rev",
        "dbf2020_row_literal": "",
    },
    "amortization_limited_term_electric_plant": {
        "dbf2020_row_number": pd.NA,
        "dbf2020_table_name": "f1_dacs_epda",
        "dbf2020_row_literal": "Amortization of Limited Term Electric Plant (Account 404) (d)",
    },
    "amortization_other_electric_plant": {
        "dbf2020_row_number": pd.NA,
        "dbf2020_table_name": "f1_dacs_epda",
        "dbf2020_row_literal": "Amortization of Other Electric Plant (Acc 405) (e)",
    },
    "depreciation_amortization_total": {
        "dbf2020_row_number": pd.NA,
        "dbf2020_table_name": "f1_dacs_epda",
        "dbf2020_row_literal": "Total (f)",
    },
    "depreciation_expense": {
        "dbf2020_row_number": pd.NA,
        "dbf2020_table_name": "f1_dacs_epda",
        "dbf2020_row_literal": "Depreciation Expense (Account 403) (b)",
    },
    "depreciation_expense_asset_retirement": {
        "dbf2020_row_number": pd.NA,
        "dbf2020_table_name": "f1_dacs_epda",
        "dbf2020_row_literal": "Depreciation Expense for Asset Retirement Costs (Account 403.1) (c)",
    },
}
"""Manually compiled metadata from DBF-only or PUDL-generated xbrl_factios.

Note: the factoids beginning with "less" here could be removed after a transition
of expectations from assuming the calculation components in any given explosion
is a tree structure to being a dag. These xbrl_factoids were added in
`transform.ferc1` and could be removed upon this transition.
"""


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
def _out_ferc1__explosion_tags(table_dimensions_ferc1) -> pd.DataFrame:
    """Grab the stored tables of tags and add inferred dimension."""
    # Also, these tags may not be applicable to all exploded tables, but
    # we need to pass in a dataframe with the right structure to all of the exploders,
    # so we're just re-using this one for the moment.
    rate_base_tags = _rate_base_tags(table_dimensions_ferc1=table_dimensions_ferc1)
    plant_status_tags = _aggregatable_dimension_tags(
        table_dimensions_ferc1=table_dimensions_ferc1, dimension="plant_status"
    )
    plant_function_tags = _aggregatable_dimension_tags(
        table_dimensions_ferc1=table_dimensions_ferc1, dimension="plant_function"
    )
    # We shouldn't have more than one row per tag, so we use a 1:1 validation here.
    plant_tags = plant_status_tags.merge(
        plant_function_tags, how="outer", on=list(NodeId._fields), validate="1:1"
    )
    tags_df = pd.merge(
        rate_base_tags, plant_tags, on=list(NodeId._fields), how="outer"
    ).astype(pd.StringDtype())
    return tags_df


def _rate_base_tags(table_dimensions_ferc1: pd.DataFrame) -> pd.DataFrame:
    # NOTE: there are a bunch of duplicate records in xbrl_factoid_rate_base_tags.csv
    tags_csv = (
        importlib.resources.files("pudl.package_data.ferc1")
        / "xbrl_factoid_rate_base_tags.csv"
    )
    tags_df = (
        pd.read_csv(
            tags_csv,
            usecols=list(NodeId._fields) + ["in_rate_base"],
        )
        .drop_duplicates()
        .dropna(subset=["table_name", "xbrl_factoid"], how="any")
        .pipe(
            pudl.transform.ferc1.make_calculation_dimensions_explicit,
            table_dimensions_ferc1,
            dimensions=["utility_type", "plant_function", "plant_status"],
        )
    )
    return tags_df


def _aggregatable_dimension_tags(
    table_dimensions_ferc1: pd.DataFrame,
    dimension: Literal["plant_status", "plant_function"],
) -> pd.DataFrame:
    # make a new lil csv w the manually compiled plant status or dimension
    # add in the rest from the table_dims
    # merge it into _out_ferc1__explosion_tags
    aggregatable_col = f"aggregatable_{dimension}"
    tags_csv = (
        importlib.resources.files("pudl.package_data.ferc1")
        / f"xbrl_factoid_{dimension}_tags.csv"
    )
    dimensions = ["utility_type", "plant_function", "plant_status"]
    idx = list(NodeId._fields)
    tags_df = (
        pd.read_csv(tags_csv)
        .assign(**{dim: pd.NA for dim in dimensions})
        .pipe(
            pudl.transform.ferc1.make_calculation_dimensions_explicit,
            table_dimensions_ferc1,
            dimensions=dimensions,
        )
        .astype(pd.StringDtype())
        .set_index(idx)
    )
    table_dimensions_ferc1 = table_dimensions_ferc1.set_index(idx)
    tags_df = pd.concat(
        [
            tags_df,
            table_dimensions_ferc1.loc[
                table_dimensions_ferc1.index.difference(tags_df.index)
            ],
        ]
    ).reset_index()
    tags_df[aggregatable_col] = tags_df[aggregatable_col].fillna(tags_df[dimension])
    return tags_df[tags_df[aggregatable_col] != "total"]


def exploded_table_asset_factory(
    root_table: str,
    table_names_to_explode: list[str],
    seed_nodes: list[NodeId],
    group_metric_checks: GroupMetricChecks,
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """Create an exploded table based on a set of related input tables."""
    ins: Mapping[str, AssetIn] = {
        "metadata_xbrl_ferc1": AssetIn("metadata_xbrl_ferc1"),
        "calculation_components_xbrl_ferc1": AssetIn(
            "calculation_components_xbrl_ferc1"
        ),
        "_out_ferc1__explosion_tags": AssetIn("_out_ferc1__explosion_tags"),
    }
    ins |= {table_name: AssetIn(table_name) for table_name in table_names_to_explode}

    @asset(name=f"exploded_{root_table}", ins=ins, io_manager_key=io_manager_key)
    def exploded_tables_asset(
        **kwargs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        metadata_xbrl_ferc1 = kwargs["metadata_xbrl_ferc1"]
        calculation_components_xbrl_ferc1 = kwargs["calculation_components_xbrl_ferc1"]
        tags = kwargs["_out_ferc1__explosion_tags"]
        tables_to_explode = {
            name: df
            for (name, df) in kwargs.items()
            if name
            not in [
                "metadata_xbrl_ferc1",
                "calculation_components_xbrl_ferc1",
                "_out_ferc1__explosion_tags",
            ]
        }
        return Exploder(
            table_names=tables_to_explode.keys(),
            root_table=root_table,
            metadata_xbrl_ferc1=metadata_xbrl_ferc1,
            calculation_components_xbrl_ferc1=calculation_components_xbrl_ferc1,
            seed_nodes=seed_nodes,
            tags=tags,
            group_metric_checks=group_metric_checks,
        ).boom(tables_to_explode=tables_to_explode)

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
            "group_metric_checks": EXPLOSION_CALCULATION_TOLERANCES[
                "income_statement_ferc1"
            ],
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
                "utility_plant_summary_ferc1",
                "plant_in_service_ferc1",
                "electric_plant_depreciation_functional_ferc1",
            ],
            "group_metric_checks": EXPLOSION_CALCULATION_TOLERANCES[
                "balance_sheet_assets_ferc1"
            ],
            "seed_nodes": [
                NodeId(
                    table_name="balance_sheet_assets_ferc1",
                    xbrl_factoid="assets_and_other_debits",
                    utility_type="total",
                    plant_status=pd.NA,
                    plant_function=pd.NA,
                )
            ],
        },
        {
            "root_table": "balance_sheet_liabilities_ferc1",
            "table_names_to_explode": [
                "balance_sheet_liabilities_ferc1",
                "retained_earnings_ferc1",
            ],
            "group_metric_checks": EXPLOSION_CALCULATION_TOLERANCES[
                "balance_sheet_liabilities_ferc1"
            ],
            "seed_nodes": [
                NodeId(
                    table_name="balance_sheet_liabilities_ferc1",
                    xbrl_factoid="liabilities_and_other_credits",
                    utility_type="total",
                    plant_status=pd.NA,
                    plant_function=pd.NA,
                )
            ],
        },
    ]
    return [exploded_table_asset_factory(**kwargs) for kwargs in explosion_args]


exploded_ferc1_assets = create_exploded_table_assets()


class Exploder:
    """Get unique, granular datapoints from a set of related, nested FERC1 tables."""

    def __init__(
        self: Self,
        table_names: list[str],
        root_table: str,
        metadata_xbrl_ferc1: pd.DataFrame,
        calculation_components_xbrl_ferc1: pd.DataFrame,
        seed_nodes: list[NodeId],
        tags: pd.DataFrame = pd.DataFrame(),
        group_metric_checks: GroupMetricChecks = GroupMetricChecks(),
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
        self.group_metric_checks = group_metric_checks
        self.metadata_xbrl_ferc1 = metadata_xbrl_ferc1
        self.calculation_components_xbrl_ferc1 = calculation_components_xbrl_ferc1
        self.seed_nodes = seed_nodes
        self.tags = tags

    @cached_property
    def exploded_calcs(self: Self):
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
            .loc[
                :,
                parent_cols
                + calc_cols
                + ["weight", "is_within_table_calc", "is_total_to_subdimensions_calc"],
            ]
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

    @cached_property
    def exploded_meta(self: Self) -> pd.DataFrame:
        """Combine a set of interrelated table's metatada for use in :class:`Exploder`.

        Any calculations containing components that are part of tables outside the
        set of exploded tables will be converted to reported values with an empty
        calculation. Then we verify that all referenced calculation components actually
        appear as their own records within the concatenated metadata dataframe.
        """
        calc_cols = list(NodeId._fields)
        exploded_metadata = (
            self.metadata_xbrl_ferc1[
                self.metadata_xbrl_ferc1["table_name"].isin(self.table_names)
            ]
            .set_index(calc_cols)
            .sort_index()
            .reset_index()
        )
        # At this point all remaining calculation components should exist within the
        # exploded metadata.
        calc_comps = self.exploded_calcs
        missing_from_calcs_idx = calc_comps.set_index(calc_cols).index.difference(
            calc_comps.set_index(calc_cols).index
        )
        assert missing_from_calcs_idx.empty

        # Add additional metadata useful for debugging calculations and tagging:
        def snake_to_camel_case(factoid: str):
            return "".join([word.capitalize() for word in factoid.split("_")])

        exploded_metadata["xbrl_taxonomy_fact_name"] = exploded_metadata[
            "xbrl_factoid_original"
        ].apply(snake_to_camel_case)
        pudl_to_xbrl_map = {
            pudl_table: source_tables["xbrl"]
            for pudl_table, source_tables in pudl.extract.ferc1.TABLE_NAME_MAP_FERC1.items()
        }
        exploded_metadata["xbrl_schedule_name"] = exploded_metadata["table_name"].map(
            pudl_to_xbrl_map
        )

        def get_dbf_row_metadata(pudl_table: str, year: int = 2020):
            dbf_tables = pudl.transform.ferc1.FERC1_TFR_CLASSES[
                pudl_table
            ]().params.aligned_dbf_table_names
            dbf_metadata = (
                pudl.transform.ferc1.read_dbf_to_xbrl_map(dbf_table_names=dbf_tables)
                .pipe(pudl.transform.ferc1.fill_dbf_to_xbrl_map)
                .query("report_year==@year")
                .drop(columns="report_year")
                .astype(
                    {
                        "row_number": "Int64",
                        "row_literal": "string",
                    }
                )
                .rename(
                    columns={
                        "row_number": f"dbf{year}_row_number",
                        "row_literal": f"dbf{year}_row_literal",
                        "sched_table_name": f"dbf{year}_table_name",
                    }
                )
                .assign(table_name=pudl_table)
                .drop_duplicates(subset=["table_name", "xbrl_factoid"])
            )
            return dbf_metadata

        dbf_row_metadata = pd.concat(
            [get_dbf_row_metadata(table) for table in self.table_names]
        )

        exploded_metadata = exploded_metadata.merge(
            dbf_row_metadata,
            how="left",
            on=["table_name", "xbrl_factoid"],
            validate="many_to_one",
        )

        # Add manual fixes for created factoids
        fixes = pd.DataFrame(MANUAL_DBF_METADATA_FIXES).T
        exploded_metadata = exploded_metadata.set_index("xbrl_factoid")
        # restrict fixes to only those that are actually in the meta.
        fixes = fixes.loc[fixes.index.intersection(exploded_metadata.index)]
        exploded_metadata.loc[fixes.index, fixes.columns] = fixes
        exploded_metadata = exploded_metadata.reset_index()

        return exploded_metadata

    @cached_property
    def calculation_forest(self: Self) -> "XbrlCalculationForestFerc1":
        """Construct a calculation forest based on class attributes."""
        return XbrlCalculationForestFerc1(
            exploded_calcs=self.exploded_calcs,
            exploded_meta=self.exploded_meta,
            seeds=self.seed_nodes,
            tags=self.tags,
            group_metric_checks=self.group_metric_checks,
        )

    @cached_property
    def other_dimensions(self: Self) -> list[str]:
        """Get all of the column names for the other dimensions."""
        return pudl.transform.ferc1.other_dimensions(table_names=self.table_names)

    @cached_property
    def exploded_pks(self: Self) -> list[str]:
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

    @cached_property
    def value_col(self: Self) -> str:
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

    def boom(self: Self, tables_to_explode: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Explode a set of nested tables.

        There are five main stages of this process:

        #. Prep all of the individual tables for explosion.
        #. Concatenate all of the tabels together.
        #. Remove duplication in the concatenated exploded table.
        #. Annotate the fine-grained data with additional metadata.
        #. Validate that calculated top-level values are correct. (not implemented)

        Args:
            tables_to_explode: dictionary of table name (key) to transfomed table (value).
        """
        exploded = (
            self.initial_explosion_concatenation(tables_to_explode)
            .pipe(self.reconcile_intertable_calculations)
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
        if not self.exploded_meta.loc[:, missing_dims].isna().all(axis=None):
            raise AssertionError(
                f"Expected missing metadata dimensions {missing_dims} to be null."
            )
        exploded_meta = self.exploded_meta.drop(columns=missing_dims)
        meta_idx = list(set(meta_idx).difference(missing_dims))

        # drop any metadata columns that appear in the data tables, because we may have
        # edited them in the metadata table, and want the edited version to take
        # precedence
        cols_to_keep = list(
            set(exploded.columns).difference(exploded_meta.columns).union(meta_idx)
        )
        exploded = pd.merge(
            left=exploded.loc[:, cols_to_keep],
            right=exploded_meta,
            how="left",
            on=meta_idx,
            validate="m:1",
        )
        return exploded

    def reconcile_intertable_calculations(
        self: Self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Generate calculated values for inter-table calculated factoids.

        This function sums components of calculations for a given factoid when the
        components originate entirely or partially outside of the table. It also
        accounts for components that only sum to a factoid within a particular dimension
        (e.g., for an electric utility or for plants whose plant_function is
        "in_service"). This returns a dataframe with a "calculated_value" column.

        Args:
            exploded: concatenated tables for table explosion.
        """
        calculations_intertable = self.exploded_calcs[
            ~self.exploded_calcs.is_within_table_calc
        ]
        if calculations_intertable.empty:
            return exploded
        logger.info(
            f"{self.root_table}: Reconcile inter-table calculations: "
            f"{list(calculations_intertable.xbrl_factoid.unique())}."
        )
        calc_idx = [col for col in list(NodeId._fields) if col in self.exploded_pks]
        logger.info("Checking inter-table, non-total to subtotal calcs.")
        calculated_df = pudl.transform.ferc1.calculate_values_from_components(
            calculation_components=calculations_intertable[
                ~calculations_intertable.is_total_to_subdimensions_calc
            ],
            data=exploded,
            calc_idx=calc_idx,
            value_col=self.value_col,
        )
        calculated_df = pudl.transform.ferc1.check_calculation_metrics(
            calculated_df=calculated_df, group_metric_checks=self.group_metric_checks
        )
        calculated_df = pudl.transform.ferc1.add_corrections(
            calculated_df=calculated_df,
            value_col=self.value_col,
            is_close_tolerance=pudl.transform.ferc1.IsCloseTolerance(),
            table_name=self.root_table,
        )
        logger.info("Checking sub-total calcs.")
        subtotal_calcs = pudl.transform.ferc1.calculate_values_from_components(
            calculation_components=calculations_intertable[
                calculations_intertable.is_total_to_subdimensions_calc
            ],
            data=exploded,
            calc_idx=calc_idx,
            value_col=self.value_col,
        )
        subtotal_calcs = pudl.transform.ferc1.check_calculation_metrics(
            calculated_df=subtotal_calcs,
            group_metric_checks=self.group_metric_checks,
        )
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
    :meth:`Exploder.exploded_calcs`  A list of seed nodes can also be supplied,
    indicating which nodes must be present in the resulting forest. This can be used to
    prune irrelevant portions of the overall forest out of the exploded metadata. If no
    seeds are provided, then all of the nodes referenced in the exploded_calcs input
    dataframe will be used as seeds.

    This class makes heavy use of :mod:`networkx` to manage the graph that we build
    from calculation relationships.
    """

    # Not sure if dynamically basing this on NodeId is really a good idea here.
    calc_cols: list[str] = list(NodeId._fields)
    exploded_meta: pd.DataFrame = pd.DataFrame()
    exploded_calcs: pd.DataFrame = pd.DataFrame()
    seeds: list[NodeId] = []
    tags: pd.DataFrame = pd.DataFrame()
    group_metric_checks: GroupMetricChecks = GroupMetricChecks()
    model_config = ConfigDict(
        arbitrary_types_allowed=True, ignored_types=(cached_property,)
    )

    @property
    def parent_cols(self: Self) -> list[str]:
        """Construct parent_cols based on the provided calc_cols."""
        return [col + "_parent" for col in self.calc_cols]

    @model_validator(mode="after")
    def unique_associations(self: Self):
        """Ensure parent-child associations in exploded calculations are unique."""
        pks = self.calc_cols + self.parent_cols
        dupes = self.exploded_calcs.duplicated(subset=pks, keep=False)
        if dupes.any():
            logger.warning(
                "Consolidating non-unique associations found in exploded_calcs:\n"
                f"{self.exploded_calcs.loc[dupes]}"
            )
        assert not self.exploded_calcs.duplicated(subset=pks, keep=False).any()
        return self

    @model_validator(mode="after")
    def calcs_have_required_cols(self: Self):
        """Ensure exploded calculations include all required columns."""
        required_cols = self.parent_cols + self.calc_cols + ["weight"]
        missing_cols = [
            col for col in required_cols if col not in self.exploded_calcs.columns
        ]
        if missing_cols:
            raise ValueError(
                f"Exploded calculations missing expected columns: {missing_cols=}"
            )
        self.exploded_calcs = self.exploded_calcs.loc[:, required_cols]
        return self

    @model_validator(mode="after")
    def calc_parents_notna(self: Self):
        """Ensure that parent table_name and xbrl_factoid columns are non-null."""
        if (
            self.exploded_calcs[["table_name_parent", "xbrl_factoid_parent"]]
            .isna()
            .any(axis=None)
        ):
            raise AssertionError("Null parent table name or xbrl_factoid found.")
        return self

    @field_validator("tags")
    @classmethod
    def tags_have_required_cols(
        cls, v: pd.DataFrame, info: ValidationInfo
    ) -> pd.DataFrame:
        """Ensure tagging dataframe contains all required index columns."""
        missing_cols = [col for col in info.data["calc_cols"] if col not in v.columns]
        if missing_cols:
            raise ValueError(
                f"Tagging dataframe was missing expected columns: {missing_cols=}"
            )
        return v

    @field_validator("tags")
    @classmethod
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

    @field_validator("tags")
    @classmethod
    def single_valued_tags(cls, v: pd.DataFrame, info: ValidationInfo) -> pd.DataFrame:
        """Ensure all tags have unique values."""
        dupes = v.duplicated(subset=info.data["calc_cols"], keep=False)
        if dupes.any():
            logger.warning(
                f"Found {dupes.sum()} duplicate tag records:\n{v.loc[dupes]}"
            )
        return v

    @model_validator(mode="after")
    def seeds_within_bounds(self: Self):
        """Ensure that all seeds are present within exploded_calcs index.

        For some reason this validator is being run before exploded_calcs has been
        added to the values dictionary, which doesn't make sense, since "seeds" is
        defined after exploded_calcs in the model.
        """
        all_nodes = self.exploded_calcs.set_index(self.parent_cols).index
        bad_seeds = [seed for seed in self.seeds if seed not in all_nodes]
        if bad_seeds:
            raise ValueError(f"Seeds missing from exploded_calcs index: {bad_seeds=}")
        return self

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

    @cached_property
    def node_attrs(self: Self) -> dict[NodeId, dict[str, dict[str, str]]]:
        """Construct a dictionary of node attributes for application to the forest.

        Note attributes consist of the manually assigned tags.
        """
        # Reshape the tags to turn them into a dictionary of values per-node. This
        # will make it easier to add arbitrary sets of tags later on.
        tags_dict = (
            self.tags.convert_dtypes().set_index(self.calc_cols).to_dict(orient="index")
        )
        # Drop None tags created by combining multiple tagging CSVs
        clean_tags_dict = {
            k: {a: b for a, b in v.items() if b is not None}
            for k, v in tags_dict.items()
        }
        node_attrs = (
            pd.DataFrame(
                index=pd.MultiIndex.from_tuples(
                    clean_tags_dict.keys(), names=self.calc_cols
                ),
                data={"tags": list(clean_tags_dict.values())},
            )
            .reset_index()
            # Type conversion is necessary to get pd.NA in the index:
            .astype({col: pd.StringDtype() for col in self.calc_cols})
            # We need a dictionary for *all* nodes, not just those with tags.
            .merge(
                self.exploded_meta.loc[:, self.calc_cols],
                how="left",
                on=self.calc_cols,
                validate="one_to_many",
                indicator=True,
            )
            # For nodes with no tags, we assign an empty dictionary:
            .assign(tags=lambda x: np.where(x["tags"].isna(), {}, x["tags"]))
        )
        lefties = node_attrs[
            (node_attrs._merge == "left_only")
            & (node_attrs.table_name.isin(self.table_names))
        ]
        if not lefties.empty:
            logger.warning(
                f"Found {len(lefties)} tags that only exist in our manually compiled "
                "tags when expected none. Ensure the compiled tags match the metadata."
                f"Mismatched tags:\n{lefties}"
            )
        return (
            node_attrs.drop(columns=["_merge"])
            .set_index(self.calc_cols)
            .to_dict(orient="index")
        )

    @cached_property
    def edge_attrs(self: Self) -> dict[Any, Any]:
        """Construct a dictionary of edge attributes for application to the forest.

        The only edge attribute is the calculation component weight.
        """
        parents = [
            NodeId(*x)
            for x in self.exploded_calcs.set_index(self.parent_cols).index.to_list()
        ]
        children = [
            NodeId(*x)
            for x in self.exploded_calcs.set_index(self.calc_cols).index.to_list()
        ]
        weights = self.exploded_calcs["weight"].to_list()
        edge_attrs = {
            (parent, child): {"weight": weight}
            for parent, child, weight in zip(parents, children, weights)
        }
        return edge_attrs

    @cached_property
    def annotated_forest(self: Self) -> nx.DiGraph:
        """Annotate the calculation forest with node calculation weights and tags.

        The annotated forest should have exactly the same structure as the forest, but
        with additional data associated with each of the nodes. This method also does
        some error checking to try and ensure that the weights and tags that are being
        associated with the forest are internally self-consistent.

        We check whether there are multiple different weights assocated with the same
        node in the calculation components. There are a few instances where this is
        expected, but if there a lot of conflicting weights something is probably wrong.

        We check whether any of the nodes that were orphaned (never connected to the
        graph) or that were pruned in the course of enforcing a forest structure had
        manually assigned tags (e.g. indicating whether they contribute to rate base).
        If they do, then the final exploded data table may not capture all of the
        manually assigned metadata, and we either need to edit the metadata, or figure
        out why those nodes aren't being included in the final calculation forest.
        """
        annotated_forest = deepcopy(self.forest)
        nx.set_node_attributes(annotated_forest, self.node_attrs)
        nx.set_edge_attributes(annotated_forest, self.edge_attrs)

        logger.info("Checking whether any pruned nodes were also tagged.")
        self.check_lost_tags(lost_nodes=self.pruned)
        logger.info("Checking whether any orphaned nodes were also tagged.")
        self.check_lost_tags(lost_nodes=self.orphans)
        self.check_conflicting_tags(annotated_forest)
        return annotated_forest

    def check_lost_tags(self: Self, lost_nodes: list[NodeId]) -> None:
        """Check whether any of the input lost nodes were also tagged nodes."""
        if lost_nodes:
            lost = pd.DataFrame(lost_nodes).set_index(self.calc_cols)
            tagged = self.tags.set_index(self.calc_cols)
            lost_tagged = tagged.index.intersection(lost.index)
            if not lost_tagged.empty:
                logger.warning(
                    "The following tagged nodes were lost in building the forest:\n"
                    f"{tagged.loc[lost_tagged].sort_index()}"
                )

    @staticmethod
    def check_conflicting_tags(annotated_forest: nx.DiGraph) -> None:
        """Check for conflicts between ancestor and descendant tags.

        At this point, we have just applied the manually compiled tags to the nodes in
        the forest, and haven't yet propagated them down to the leaves. It's possible
        that ancestor nodes (closer to the roots) might have tags associated with them
        that are in conflict with descendant nodes (closer to the leaves). If that's
        the case then when we propagate the tags to the leaves, whichever tag is
        propagated last will end up taking precedence.

        These kinds of conflicts are probably due to errors in the tagging metadata, and
        should be investigated.
        """
        nodes = annotated_forest.nodes
        for ancestor in nodes:
            for descendant in nx.descendants(annotated_forest, ancestor):
                for tag in nodes[ancestor].get("tags", {}):
                    if tag in nodes[descendant].get("tags", {}):
                        ancestor_tag_value = nodes[ancestor]["tags"][tag]
                        descendant_tag_value = nodes[descendant]["tags"][tag]
                        if ancestor_tag_value != descendant_tag_value:
                            logger.error(
                                "\n================================================"
                                f"\nCalculation forest nodes have conflicting tags:"
                                f"\nAncestor: {ancestor}"
                                f"\n    tags[{tag}] == {ancestor_tag_value}"
                                f"\nDescendant: {descendant}"
                                f"\n    tags[{tag}] == {descendant_tag_value}"
                            )

    @cached_property
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
        """Prune those parts of the input graph that aren't reachable from the roots.

        Build a table of exploded calculations that includes only those nodes that
        are part of the input graph, and that are reachable from the roots of the
        calculation forest. Then use that set of exploded calculations to construct a
        new graph.

        This is complicated by the fact that some nodes may have already been pruned
        from the input graph, and so when selecting both parent and child nodes from
        the calculations, we need to make sure that they are present in the input graph,
        as well as the complete set of calculation components.
        """
        seeded_nodes = set(self.seeds)
        for seed in self.seeds:
            # the seeds and all of their descendants from the graph
            seeded_nodes = list(
                seeded_nodes.union({seed}).union(nx.descendants(graph, seed))
            )
        # Any seeded node that appears in the input graph and is also a parent.
        seeded_parents = [
            node
            for node, degree in dict(graph.out_degree(seeded_nodes)).items()
            if degree > 0
        ]
        # Any calculation where the parent is one of the seeded parents.
        seeded_calcs = (
            self.exploded_calcs.set_index(self.parent_cols)
            .loc[seeded_parents]
            .reset_index()
        )
        # All child nodes in the seeded calculations that are part of the input graph.
        seeded_child_nodes = list(
            set(
                seeded_calcs[self.calc_cols].itertuples(index=False, name="NodeId")
            ).intersection(graph.nodes)
        )
        # This seeded calcs includes only calculations where both the parent and child
        # nodes were part of the input graph.
        seeded_calcs = (
            seeded_calcs.set_index(self.calc_cols).loc[seeded_child_nodes].reset_index()
        )
        return self.exploded_calcs_to_digraph(exploded_calcs=seeded_calcs)

    @cached_property
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

    @cached_property
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
        forest = deepcopy(self.seeded_digraph)
        # Remove any node that ONLY has stepchildren.
        # A stepparent is a node that has a child with more than one parent.
        # A stepchild is a node with more than one parent.
        # See self.stepparents and self.stepchildren
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

        # Removing pure stepparents should NEVER disconnect nodes from the forest.
        # Defensive check to ensure that this is actually true
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

    @cached_property
    def full_digraph_roots(self: Self) -> list[NodeId]:
        """Find all roots in the full digraph described by the exploded metadata."""
        return self.roots(graph=self.full_digraph)

    @cached_property
    def seeded_digraph_roots(self: Self) -> list[NodeId]:
        """Find all roots in the seeded digraph."""
        return self.roots(graph=self.seeded_digraph)

    @cached_property
    def forest_roots(self: Self) -> list[NodeId]:
        """Find all roots in the pruned calculation forest."""
        return self.roots(graph=self.forest)

    @staticmethod
    def leaves(graph: nx.DiGraph) -> list[NodeId]:
        """Identify all leaf nodes in a digraph."""
        return [n for n, d in graph.out_degree() if d == 0]

    @cached_property
    def full_digraph_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the full digraph."""
        return self.leaves(graph=self.full_digraph)

    @cached_property
    def seeded_digraph_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the seeded digraph."""
        return self.leaves(graph=self.seeded_digraph)

    @cached_property
    def forest_leaves(self: Self) -> list[NodeId]:
        """All leaf nodes in the pruned forest."""
        return self.leaves(graph=self.forest)

    @cached_property
    def orphans(self: Self) -> list[NodeId]:
        """Identify all nodes that appear in metadata but not in the full digraph."""
        nodes = self.full_digraph.nodes
        return [
            NodeId(*n)
            for n in self.exploded_meta.set_index(self.calc_cols).index
            if n not in nodes
        ]

    @cached_property
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

    def _get_path_weight(self, path: list[NodeId], graph: nx.DiGraph) -> float:
        """Multiply all weights along a path together."""
        leaf_weight = 1.0
        for parent, child in zip(path, path[1:]):
            leaf_weight *= graph.get_edge_data(parent, child)["weight"]
        return leaf_weight

    @cached_property
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
        leaves = self.forest_leaves
        roots = self.forest_roots
        leaf_to_root_map = {
            leaf: root
            for leaf in leaves
            for root in roots
            if leaf in nx.descendants(self.annotated_forest, root)
        }
        leaves_df = pd.DataFrame(list(leaf_to_root_map.keys()))
        roots_df = pd.DataFrame(list(leaf_to_root_map.values())).rename(
            columns={col: col + "_root" for col in self.calc_cols}
        )
        leafy_meta = pd.concat([roots_df, leaves_df], axis="columns")

        # Propagate tags and weights to leaf nodes
        leaf_rows = []
        for leaf in leaves:
            leaf_tags = {}
            ancestors = list(nx.ancestors(self.annotated_forest, leaf)) + [leaf]
            for node in ancestors:
                leaf_tags |= self.annotated_forest.nodes[node].get("tags", {})
            all_leaf_weights = {
                self._get_path_weight(path, self.annotated_forest)
                for path in nx.all_simple_paths(
                    self.annotated_forest, leaf_to_root_map[leaf], leaf
                )
            }
            if len(all_leaf_weights) != 1:
                raise ValueError(
                    f"Paths from {leaf_to_root_map[leaf]} to {leaf} have "
                    f"different weights: {all_leaf_weights}"
                )
            leaf_weight = all_leaf_weights.pop()

            # Construct a dictionary describing the leaf node and convert it into a
            # single row DataFrame. This makes adding arbitrary tags easy.
            leaf_attrs = {
                "table_name": leaf.table_name,
                "xbrl_factoid": leaf.xbrl_factoid,
                "utility_type": leaf.utility_type,
                "plant_status": leaf.plant_status,
                "plant_function": leaf.plant_function,
                "weight": leaf_weight,
                "tags": leaf_tags,
            }
            leaf_rows.append(pd.json_normalize(leaf_attrs, sep="_"))

        # Combine the two dataframes we've constructed above:
        return (
            pd.merge(leafy_meta, pd.concat(leaf_rows), validate="one_to_one")
            .reset_index(drop=True)
            .convert_dtypes()
        )

    @cached_property
    def root_calculations(self: Self) -> pd.DataFrame:
        """Produce a calculation components dataframe containing only roots and leaves.

        This dataframe has a format similar to exploded_calcs and can be used with the
        exploded data to verify that the root values can still be correctly calculated
        from the leaf values.
        """
        return self.leafy_meta.rename(columns=lambda x: re.sub("_root$", "_parent", x))

    @cached_property
    def table_names(self: Self) -> list[str]:
        """Produce the list of tables involved in this explosion."""
        return list(self.exploded_calcs["table_name_parent"].unique())

    def plot_graph(self: Self, graph: nx.DiGraph) -> None:
        """Visualize a CalculationForest graph."""
        colors = ["red", "yellow", "green", "blue", "orange", "cyan", "purple"]
        color_map = dict(zip(self.table_names, colors[: len(self.table_names)]))

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

    @cached_property
    def forest_as_table(self: Self) -> pd.DataFrame:
        """Construct a tabular representation of the calculation forest.

        Each generation of nodes, starting with the root(s) of the calculation forest,
        make up a set of columns in the table. Each set of columns is merged onto
        """
        logger.info("Recursively building a tabular version of the calculation forest.")
        # Identify all root nodes in the forest:
        layer0_nodes = [n for n, d in self.annotated_forest.in_degree() if d == 0]
        # Convert them into the first layer of the dataframe:
        layer0_df = pd.DataFrame(layer0_nodes).rename(columns=lambda x: x + "_layer0")

        return (
            self._add_layers_to_forest_as_table(df=layer0_df)
            .dropna(axis="columns", how="all")
            .convert_dtypes()
        )

    def _add_layers_to_forest_as_table(self: Self, df: pd.DataFrame) -> pd.DataFrame:
        """Recursively add additional layers of nodes from the forest to the table.

        Given a dataframe with one or more set of columns with names corresponding to
        the components of a NodeId with suffixes of the form _layerN, identify the
        children of the nodes in the set of columns with the largest N, and merge them
        onto the table, recursively until there are no more children to add. Creating a
        tabular representation of the calculation forest that can be inspected in Excel.

        Include inter-layer calculation weights and tags associated with the nodes pre
        propagation.
        """
        # Identify the last layer of nodes present in the input dataframe.
        current_layer = df.rename(
            columns=lambda x: int(re.sub(r"^.*_layer(\d+)$", r"\1", x))
        ).columns.max()
        logger.info(f"{current_layer=}")
        suffix = f"_layer{current_layer}"
        parent_cols = [col + suffix for col in self.calc_cols]
        # Identify the list of nodes that are part of that last layer:
        parent_nodes = list(
            df[parent_cols]
            .drop_duplicates()
            .dropna(how="all")
            .rename(columns=lambda x: x.removesuffix(suffix))
            .itertuples(name="NodeId", index=False)
        )

        # Identify the successors (children), if any, of each node in the last layer:
        successor_dfs = []
        for node in parent_nodes:
            successor_nodes = list(self.forest.successors(node))
            # If this particular node has no successors, skip to the next one.
            if not successor_nodes:
                continue
            # Convert the list of successor nodes into a dataframe with layer = n+1
            successor_df = nodes_to_df(
                calc_forest=self.annotated_forest, nodes=successor_nodes
            ).rename(columns=lambda x: x + f"_layer{current_layer + 1}")
            # Add a set of parent columns that all have the same values so we can merge
            # this onto the previous layer
            successor_df[parent_cols] = node
            successor_dfs.append(successor_df)

        # If any child nodes were found, merge them onto the input dataframe creating
        # a new layer , and recurse:
        if successor_dfs:
            new_df = df.merge(pd.concat(successor_dfs), on=parent_cols, how="outer")
            df = self._add_layers_to_forest_as_table(df=new_df)

        # If no child nodes were found return the dataframe terminating the recursion.
        return df


def nodes_to_df(calc_forest: nx.DiGraph, nodes: list[NodeId]) -> pd.DataFrame:
    """Construct a dataframe from a list of nodes, including their annotations.

    NodeIds that are not present in the calculation forest will be ignored.

    Args:
        calc_forest: A calculation forest made of nodes with "weight" and "tags" data.
        nodes: List of :class:`NodeId` values to extract from the calculation forest.

    Returns:
        A tabular dataframe representation of the nodes, including their tags, extracted
        from the calculation forest.
    """
    node_dict = {
        k: v for k, v in dict(calc_forest.nodes(data=True)).items() if k in nodes
    }
    index = pd.DataFrame(node_dict.keys()).astype("string")
    data = pd.DataFrame(node_dict.values())
    try:
        tags = pd.json_normalize(data.tags).astype("string")
    except AttributeError:
        tags = pd.DataFrame()
    return pd.concat([index, tags], axis="columns")
