"""A collection of denormalized FERC assets and helper functions."""

import importlib
import re
from copy import deepcopy
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Literal, NamedTuple, Self

import networkx as nx
import numpy as np
import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetIn,
    AssetsDefinition,
    Field,
    Mapping,
    asset,
    asset_check,
)
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
    "core_ferc1__yearly_income_statements_sched114": GroupMetricChecks(
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
    "core_ferc1__yearly_balance_sheet_assets_sched110": GroupMetricChecks(
        groups_to_check=[
            "ungrouped",
            "report_year",
            "xbrl_factoid",
            "utility_id_ferc1",
        ],
        group_metric_tolerances=GroupMetricTolerances(
            ungrouped=MetricTolerances(
                error_frequency=0.011,
                relative_error_magnitude=0.004,
                null_calculated_value_frequency=1.0,
            ),
            report_year=MetricTolerances(
                error_frequency=0.12,
                relative_error_magnitude=0.12,
                null_calculated_value_frequency=1.0,
            ),
            xbrl_factoid=MetricTolerances(
                error_frequency=0.41,
                relative_error_magnitude=0.22,
                null_calculated_value_frequency=1.0,
            ),
            utility_id_ferc1=MetricTolerances(
                error_frequency=0.27,
                relative_error_magnitude=0.4,
                null_calculated_value_frequency=1.0,
            ),
        ),
    ),
    "core_ferc1__yearly_balance_sheet_liabilities_sched110": GroupMetricChecks(
        groups_to_check=[
            "ungrouped",
            "report_year",
            "xbrl_factoid",
            "utility_id_ferc1",
        ],
        group_metric_tolerances=GroupMetricTolerances(
            ungrouped=MetricTolerances(
                error_frequency=0.048,
                relative_error_magnitude=0.019,
                null_calculated_value_frequency=1.0,
            ),
            report_year=MetricTolerances(
                error_frequency=0.048,
                relative_error_magnitude=0.05,
                null_calculated_value_frequency=1.0,
            ),
            xbrl_factoid=MetricTolerances(
                error_frequency=0.65,  # worst fact: retained_earnings
                relative_error_magnitude=0.17,
                null_calculated_value_frequency=1.0,
            ),
            utility_id_ferc1=MetricTolerances(
                error_frequency=0.063,
                relative_error_magnitude=0.16,
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


def get_core_ferc1_asset_description(asset_name: str) -> str:
    """Get the asset description portion of a core FERC FORM 1 asset.

    This is useful when programmatically constructing output assets
    from core assets using asset factories.

    Args:
        asset_name: The name of the core asset.

    Returns:
        asset_description: The asset description portion of the asset name.
    """
    pattern = r"yearly_(.*?)_sched"
    match = re.search(pattern, asset_name)

    if match:
        asset_description = match.group(1)
    else:
        raise ValueError(
            f"The asset description can not be parsed from {asset_name}"
            "because it is not a valid core FERC Form 1 asset name."
        )
    return asset_description


def ferc1_output_asset_factory(table_name: str) -> AssetsDefinition:
    """Define an output asset for the FERC1 table by adding in utility IDs."""
    ins: Mapping[str, AssetIn] = {
        f"core_ferc1__{table_name}": AssetIn(f"core_ferc1__{table_name}"),
        "core_pudl__assn_ferc1_pudl_utilities": AssetIn(
            "core_pudl__assn_ferc1_pudl_utilities"
        ),
    }

    @asset(
        name=f"out_ferc1__{table_name}",
        io_manager_key="pudl_io_manager",
        compute_kind="Python",
        ins=ins,
    )
    def _create_output_asset(
        **kwargs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Generate an output dataframe from the corresponding FERC1 core table.

        Merge in utility IDs from ``core_pudl__assn_ferc1_pudl_utilities``.
        """
        return_df = kwargs[f"core_ferc1__{table_name}"].merge(
            kwargs["core_pudl__assn_ferc1_pudl_utilities"],
            on="utility_id_ferc1",
            how="left",
            validate="many_to_one",
        )
        return return_df

    return _create_output_asset


out_ferc1_assets = [
    ferc1_output_asset_factory(table)
    for table in [
        "yearly_purchased_power_and_exchanges_sched326",
        "yearly_plant_in_service_sched204",
        "yearly_balance_sheet_assets_sched110",
        "yearly_balance_sheet_liabilities_sched110",
        "yearly_cash_flows_sched120",
        "yearly_depreciation_summary_sched336",
        "yearly_energy_dispositions_sched401",
        "yearly_energy_sources_sched401",
        "yearly_operating_expenses_sched320",
        "yearly_operating_revenues_sched300",
        "yearly_depreciation_changes_sched219",
        "yearly_depreciation_by_function_sched219",
        "yearly_sales_by_rate_schedules_sched304",
        "yearly_income_statements_sched114",
        "yearly_other_regulatory_liabilities_sched278",
        "yearly_retained_earnings_sched118",
        "yearly_transmission_lines_sched422",
        "yearly_utility_plant_summary_sched200",
    ]
]


@asset(compute_kind="Python")
def _out_ferc1__yearly_plants_utilities(
    core_pudl__assn_ferc1_pudl_plants: pd.DataFrame,
    core_pudl__assn_ferc1_pudl_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized table containing FERC plant and utility names and IDs."""
    return pd.merge(
        core_pudl__assn_ferc1_pudl_plants,
        core_pudl__assn_ferc1_pudl_utilities,
        on="utility_id_ferc1",
    )


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_steam_plants_sched402(
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
    _out_ferc1__yearly_steam_plants_sched402_with_plant_ids: pd.DataFrame,
) -> pd.DataFrame:
    """Select and joins some useful fields from the FERC Form 1 steam table.

    Select the FERC Form 1 steam plant table entries, add in the reporting utility's
    name, and the PUDL ID for the plant and utility for readability and integration with
    other tables that have PUDL IDs.  Also calculates ``capacity_factor`` (based on
    ``net_generation_mwh`` & ``capacity_mw``)

    Args:
        _out_ferc1__yearly_plants_utilities: Denormalized dataframe of FERC Form 1
            plants and utilities data.
        _out_ferc1__yearly_steam_plants_sched402_with_plant_ids: The FERC Form 1 steam
            table with imputed plant IDs to group plants across report years.

    Returns:
        A DataFrame containing useful fields from the FERC Form 1 steam table.
    """
    steam_df = (
        _out_ferc1__yearly_steam_plants_sched402_with_plant_ids.merge(
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


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_small_plants_sched410(
    core_ferc1__yearly_small_plants_sched410: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 small plants."""
    plants_small_df = (
        core_ferc1__yearly_small_plants_sched410.merge(
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


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_hydroelectric_plants_sched406(
    core_ferc1__yearly_hydroelectric_plants_sched406: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe related to the FERC Form 1 hydro plants."""
    plants_hydro_df = (
        core_ferc1__yearly_hydroelectric_plants_sched406.merge(
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


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_pumped_storage_plants_sched408(
    core_ferc1__yearly_pumped_storage_plants_sched408: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a dataframe of FERC Form 1 Pumped Storage plant data."""
    pumped_storage_df = (
        core_ferc1__yearly_pumped_storage_plants_sched408.merge(
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


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_steam_plants_fuel_sched402(
    core_ferc1__yearly_steam_plants_fuel_sched402: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Pull a useful dataframe related to FERC Form 1 fuel information.

    This function pulls the FERC Form 1 fuel data, and joins in the name of the
    reporting utility, as well as the PUDL IDs for that utility and the plant, allowing
    integration with other PUDL tables. Useful derived values include:

    * ``fuel_consumed_mmbtu`` (total fuel heat content consumed)
    * ``fuel_consumed_total_cost`` (total cost of that fuel)
    """
    fuel_df = (
        core_ferc1__yearly_steam_plants_fuel_sched402.assign(
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


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_all_plants(
    out_ferc1__yearly_steam_plants_sched402: pd.DataFrame,
    out_ferc1__yearly_small_plants_sched410: pd.DataFrame,
    out_ferc1__yearly_hydroelectric_plants_sched406: pd.DataFrame,
    out_ferc1__yearly_pumped_storage_plants_sched408: pd.DataFrame,
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
    steam_df = out_ferc1__yearly_steam_plants_sched402.rename(
        columns={"opex_plants": "opex_plant"}
    )

    # Prep hydro tables (Add this to the meta data later)
    logger.debug("prepping hydro tables")
    hydro_df = out_ferc1__yearly_hydroelectric_plants_sched406.rename(
        columns={"project_num": "ferc_license_id"}
    )
    pump_df = out_ferc1__yearly_pumped_storage_plants_sched408.rename(
        columns={"project_num": "ferc_license_id"}
    )

    # Combine all the tables together
    logger.debug("combining all tables")
    all_df = (
        pd.concat(
            [steam_df, out_ferc1__yearly_small_plants_sched410, hydro_df, pump_df]
        )
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
    io_manager_key="pudl_io_manager",
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
def out_ferc1__yearly_steam_plants_fuel_by_plant_sched402(
    context,
    core_ferc1__yearly_steam_plants_fuel_sched402: pd.DataFrame,
    _out_ferc1__yearly_plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize FERC fuel data by plant for output.

    This is mostly a wrapper around
    :func:`pudl.analysis.record_linkage.classify_plants_ferc1.fuel_by_plant_ferc1`
    which calculates some summary values on a per-plant basis (as indicated
    by ``utility_id_ferc1`` and ``plant_name_ferc1``) related to fuel
    consumption.

    Args:
        context: Dagster context object
        core_ferc1__yearly_steam_plants_fuel_sched402: Normalized FERC fuel table.
        _out_ferc1__yearly_plants_utilities: Denormalized table of FERC1 plant & utility
            IDs.

    Returns:
        A DataFrame with fuel use summarized by plant.
    """

    def drop_other_fuel_types(df):
        """Internal function to drop other fuel type.

        Fuel type other indicates we didn't know how to categorize the reported fuel
        type, which leads to records with incomplete and unusable data.
        """
        return df[df.fuel_type_code_pudl != "other"].copy()

    thresh = context.op_config["thresh"]
    # The existing function expects `fuel_type_code_pudl` to be an object, rather than
    # a category. This is a legacy of pre-dagster code, and we convert here to prevent
    # further retooling in the code-base.
    core_ferc1__yearly_steam_plants_fuel_sched402["fuel_type_code_pudl"] = (
        core_ferc1__yearly_steam_plants_fuel_sched402["fuel_type_code_pudl"].astype(str)
    )

    fuel_categories = list(
        pudl.transform.ferc1.SteamPlantsFuelTableTransformer()
        .params.categorize_strings["fuel_type_code_pudl"]
        .categories.keys()
    )

    fbp_df = (
        core_ferc1__yearly_steam_plants_fuel_sched402.pipe(drop_other_fuel_types)
        .pipe(
            pudl.analysis.fuel_by_plant.fuel_by_plant_ferc1,
            fuel_categories=fuel_categories,
            thresh=thresh,
        )
        .pipe(pudl.analysis.fuel_by_plant.revert_filled_in_float_nulls)
        .pipe(pudl.analysis.fuel_by_plant.revert_filled_in_string_nulls)
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
    # bb tests for volume of negative annual capex
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
    # calculate the standard deviation of each generator's capex over time
    df = (
        steam_df.merge(
            gb_cap_an.std()
            .add_suffix("_gen_std")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="many_to_one",
        )
        .merge(
            gb_cap_an.mean()
            .add_suffix("_gen_mean")
            .reset_index()
            .pipe(pudl.helpers.convert_cols_dtypes, "ferc1"),
            how="left",
            on=idx_steam_no_date,
            validate="many_to_one",
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


class OffByFactoid(NamedTuple):
    """A calculated factoid which is off by one other factoid.

    A factoid where a sizeable majority of utilities are using a non-standard and
    non-reported calculation to generate it. These calculated factoids are either
    missing one factoid, or include an additional factoid not included in the FERC
    metadata. Thus, the calculations are 'off by' this factoid.
    """

    table_name: str
    xbrl_factoid: str
    utility_type: str | pandas_NAType
    plant_status: str | pandas_NAType
    plant_function: str | pandas_NAType
    table_name_off_by: str
    xbrl_factoid_off_by: str
    utility_type_off_by: str | pandas_NAType
    plant_status_off_by: str | pandas_NAType
    plant_function_off_by: str | pandas_NAType


@asset
def _out_ferc1__detailed_tags(_core_ferc1__table_dimensions) -> pd.DataFrame:
    """Grab the stored tables of tags and add inferred dimension."""
    rate_tags = _get_tags(
        "xbrl_factoid_rate_base_tags.csv", _core_ferc1__table_dimensions
    )
    rev_req_tags = _get_tags(
        "xbrl_factoid_revenue_requirement_tags.csv", _core_ferc1__table_dimensions
    )
    rate_cats = _get_tags(
        "xbrl_factoid_rate_base_category_tags.csv", _core_ferc1__table_dimensions
    )
    plant_status_tags = _aggregatable_dimension_tags(
        _core_ferc1__table_dimensions, "plant_status"
    )
    plant_function_tags = _aggregatable_dimension_tags(
        _core_ferc1__table_dimensions, "plant_function"
    )
    utility_type_tags = _aggregatable_dimension_tags(
        _core_ferc1__table_dimensions, "utility_type"
    )
    tag_dfs = [
        rate_tags,
        rev_req_tags,
        rate_cats,
        plant_status_tags,
        plant_function_tags,
        utility_type_tags,
    ]
    tag_idx = list(NodeId._fields)
    tags = (
        pd.concat(
            [df.set_index(tag_idx) for df in tag_dfs],
            join="outer",
            verify_integrity=True,
            ignore_index=False,
            axis="columns",
        )
        .reset_index()
        .drop(columns=["notes"])
    )
    # special case: condense the two hydro plant_functions from _core_ferc1__table_dimensions.
    # we didn't add yet the ability to change aggregatable_plant_function by
    # plant_function. we could but this seems simpler.
    tags.aggregatable_plant_function = tags.aggregatable_plant_function.replace(
        to_replace={
            "hydraulic_production_conventional": "hydraulic_production",
            "hydraulic_production_pumped_storage": "hydraulic_production",
        }
    )
    return tags


def _get_tags(
    file_name: str, _core_ferc1__table_dimensions: pd.DataFrame
) -> pd.DataFrame:
    """Grab tags from a stored CSV file and apply :func:`make_xbrl_factoid_dimensions_explicit`."""
    tags_csv = importlib.resources.files("pudl.package_data.ferc1") / file_name
    tags_df = (
        pd.read_csv(tags_csv)
        .drop_duplicates()
        .dropna(subset=["table_name", "xbrl_factoid"], how="any")
        .astype(pd.StringDtype())
        .pipe(
            pudl.transform.ferc1.make_xbrl_factoid_dimensions_explicit,
            _core_ferc1__table_dimensions,
            dimensions=["utility_type", "plant_function", "plant_status"],
        )
    )
    return tags_df


def _aggregatable_dimension_tags(
    _core_ferc1__table_dimensions: pd.DataFrame,
    dimension: Literal["plant_status", "plant_function"],
) -> pd.DataFrame:
    # make a new lil csv w the manually compiled plant status or dimension
    # add in the rest from the table_dims
    # merge it into _out_ferc1__detailed_tags
    aggregatable_col = f"aggregatable_{dimension}"
    tags_csv = (
        importlib.resources.files("pudl.package_data.ferc1")
        / f"xbrl_factoid_{dimension}_tags.csv"
    )
    dimensions = ["utility_type", "plant_function", "plant_status"]
    idx = list(NodeId._fields)
    tags_df = (
        pd.read_csv(tags_csv)
        .assign(**dict.fromkeys(dimensions, pd.NA))
        .astype(pd.StringDtype())
        .pipe(
            pudl.transform.ferc1.make_xbrl_factoid_dimensions_explicit,
            _core_ferc1__table_dimensions,
            dimensions=dimensions,
        )
        .set_index(idx)
    )
    # don't include the corrections because we will add those in later
    _core_ferc1__table_dimensions = _core_ferc1__table_dimensions[
        ~_core_ferc1__table_dimensions.xbrl_factoid.str.endswith("_correction")
    ].set_index(idx)
    tags_df = pd.concat(
        [
            tags_df,
            _core_ferc1__table_dimensions.loc[
                _core_ferc1__table_dimensions.index.difference(tags_df.index)
            ],
        ]
    ).reset_index()
    tags_df[aggregatable_col] = tags_df[aggregatable_col].fillna(tags_df[dimension])
    return tags_df[tags_df[aggregatable_col] != "total"]


def exploded_table_asset_factory(
    root_table: str,
    table_names: list[str],
    seed_nodes: list[NodeId],
    group_metric_checks: GroupMetricChecks,
    off_by_facts: list[OffByFactoid],
    io_manager_key: str | None = None,
) -> AssetsDefinition:
    """Create an exploded table based on a set of related input tables."""
    ins: Mapping[str, AssetIn] = {
        "_core_ferc1_xbrl__metadata": AssetIn("_core_ferc1_xbrl__metadata"),
        "_core_ferc1_xbrl__calculation_components": AssetIn(
            "_core_ferc1_xbrl__calculation_components"
        ),
        "_out_ferc1__detailed_tags": AssetIn("_out_ferc1__detailed_tags"),
        "core_pudl__assn_ferc1_pudl_utilities": AssetIn(
            "core_pudl__assn_ferc1_pudl_utilities"
        ),
    }
    ins |= {table_name: AssetIn(table_name) for table_name in table_names}

    @asset(
        name=f"out_ferc1__yearly_detailed_{get_core_ferc1_asset_description(root_table)}",
        ins=ins,
        io_manager_key=io_manager_key,
    )
    def exploded_tables_asset(
        **kwargs: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        _core_ferc1_xbrl__metadata = kwargs["_core_ferc1_xbrl__metadata"]
        _core_ferc1_xbrl__calculation_components = kwargs[
            "_core_ferc1_xbrl__calculation_components"
        ]
        tags = kwargs["_out_ferc1__detailed_tags"]
        tables_to_explode = {
            name: df
            for (name, df) in kwargs.items()
            if name
            not in [
                "_core_ferc1_xbrl__metadata",
                "_core_ferc1_xbrl__calculation_components",
                "_out_ferc1__detailed_tags",
                "off_by_facts",
                "core_pudl__assn_ferc1_pudl_utilities",
            ]
        }
        return (
            Exploder(
                table_names=tables_to_explode.keys(),
                root_table=root_table,
                metadata_xbrl_ferc1=_core_ferc1_xbrl__metadata,
                calculation_components_xbrl_ferc1=_core_ferc1_xbrl__calculation_components,
                seed_nodes=seed_nodes,
                tags=tags,
                group_metric_checks=group_metric_checks,
                off_by_facts=off_by_facts,
            )
            .boom(tables_to_explode=tables_to_explode)
            .merge(
                kwargs["core_pudl__assn_ferc1_pudl_utilities"],
                on="utility_id_ferc1",
                how="left",
                validate="many_to_one",
            )
        )

    return exploded_tables_asset


EXPLOSION_ARGS = [
    {
        "root_table": "core_ferc1__yearly_income_statements_sched114",
        "table_names": [
            "core_ferc1__yearly_income_statements_sched114",
            "core_ferc1__yearly_depreciation_summary_sched336",
            "core_ferc1__yearly_operating_expenses_sched320",
            "core_ferc1__yearly_operating_revenues_sched300",
        ],
        "group_metric_checks": EXPLOSION_CALCULATION_TOLERANCES[
            "core_ferc1__yearly_income_statements_sched114"
        ],
        "seed_nodes": [
            NodeId(
                table_name="core_ferc1__yearly_income_statements_sched114",
                xbrl_factoid="net_income_loss",
                utility_type="total",
                plant_status=pd.NA,
                plant_function=pd.NA,
            ),
        ],
        "off_by_facts": [],
        "io_manager_key": "pudl_io_manager",
    },
    {
        "root_table": "core_ferc1__yearly_balance_sheet_assets_sched110",
        "table_names": [
            "core_ferc1__yearly_balance_sheet_assets_sched110",
            "core_ferc1__yearly_utility_plant_summary_sched200",
            "core_ferc1__yearly_plant_in_service_sched204",
            "core_ferc1__yearly_depreciation_by_function_sched219",
        ],
        "group_metric_checks": EXPLOSION_CALCULATION_TOLERANCES[
            "core_ferc1__yearly_balance_sheet_assets_sched110"
        ],
        "seed_nodes": [
            NodeId(
                table_name="core_ferc1__yearly_balance_sheet_assets_sched110",
                xbrl_factoid="assets_and_other_debits",
                utility_type="total",
                plant_status=pd.NA,
                plant_function=pd.NA,
            )
        ],
        "off_by_facts": [
            OffByFactoid(
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "utility_plant_in_service_classified_and_property_under_capital_leases",
                "electric",
                pd.NA,
                pd.NA,
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "utility_plant_in_service_completed_construction_not_classified",
                "electric",
                pd.NA,
                pd.NA,
            ),
            OffByFactoid(
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "utility_plant_in_service_classified_and_property_under_capital_leases",
                "electric",
                pd.NA,
                pd.NA,
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "utility_plant_in_service_property_under_capital_leases",
                "electric",
                pd.NA,
                pd.NA,
            ),
            OffByFactoid(
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "depreciation_utility_plant_in_service",
                "electric",
                pd.NA,
                pd.NA,
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "amortization_of_other_utility_plant_utility_plant_in_service",
                "electric",
                pd.NA,
                pd.NA,
            ),
        ],
        "io_manager_key": "pudl_io_manager",
    },
    {
        "root_table": "core_ferc1__yearly_balance_sheet_liabilities_sched110",
        "table_names": [
            "core_ferc1__yearly_balance_sheet_liabilities_sched110",
            "core_ferc1__yearly_retained_earnings_sched118",
        ],
        "group_metric_checks": EXPLOSION_CALCULATION_TOLERANCES[
            "core_ferc1__yearly_balance_sheet_liabilities_sched110"
        ],
        "seed_nodes": [
            NodeId(
                table_name="core_ferc1__yearly_balance_sheet_liabilities_sched110",
                xbrl_factoid="liabilities_and_other_credits",
                utility_type="total",
                plant_status=pd.NA,
                plant_function=pd.NA,
            )
        ],
        "off_by_facts": [],
        "io_manager_key": "pudl_io_manager",
    },
]


def create_exploded_table_assets() -> list[AssetsDefinition]:
    """Create a list of exploded FERC Form 1 assets.

    Returns:
        A list of :class:`AssetsDefinitions` where each asset is an exploded FERC Form 1
        table.
    """
    return [exploded_table_asset_factory(**kwargs) for kwargs in EXPLOSION_ARGS]


exploded_ferc1_assets = create_exploded_table_assets()


class Exploder:
    """Get unique, granular datapoints from a set of related, nested FERC1 tables.

    The controlling method of this class which executes its primary function is
    :meth:`boom`.
    """

    def __init__(
        self: Self,
        table_names: list[str],
        root_table: str,
        metadata_xbrl_ferc1: pd.DataFrame,
        calculation_components_xbrl_ferc1: pd.DataFrame,
        seed_nodes: list[NodeId],
        tags: pd.DataFrame = pd.DataFrame(),
        group_metric_checks: GroupMetricChecks = GroupMetricChecks(),
        off_by_facts: list[OffByFactoid] = None,
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
        self.off_by_facts = off_by_facts

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
        calc_explode = self.add_sizable_minority_corrections_to_calcs(calc_explode)
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

    def add_sizable_minority_corrections_to_calcs(
        self: Self, exploded_calcs: pd.DataFrame
    ) -> pd.DataFrame:
        """Add correction calculation records for the sizable fuck up utilities."""
        if not self.off_by_facts:
            return exploded_calcs
        facts_to_fix = (
            pd.DataFrame(self.off_by_facts)
            .rename(columns={col: f"{col}_parent" for col in NodeId._fields})
            .assign(
                xbrl_factoid=(
                    lambda x: x.xbrl_factoid_parent
                    + "_off_by_"
                    + x.xbrl_factoid_off_by
                    + "_correction"
                ),
                weight=1,
                is_total_to_subdimensions_calc=False,
                # theses fixes rn are only from inter-table calcs.
                # if they weren't we'd need to check within the group of
                # the parent fact like in process_xbrl_metadata_calculations
                is_within_table_calc=False,
            )
            .drop(columns=["xbrl_factoid_off_by"])
        )
        facts_to_fix.columns = facts_to_fix.columns.str.removesuffix("_off_by")
        return pd.concat(
            [
                exploded_calcs,
                facts_to_fix[exploded_calcs.columns].astype(exploded_calcs.dtypes),
            ]
        )

    @cached_property
    def exploded_meta(self: Self) -> pd.DataFrame:
        """Combine a set of interrelated tables' metadata for use in :class:`Exploder`.

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
            seeds=self.seed_nodes,
            tags=self.tags,
            group_metric_checks=self.group_metric_checks,
        )

    @cached_property
    def dimensions(self: Self) -> list[str]:
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

    @property
    def calc_idx(self: Self) -> list[str]:
        """Primary key columns for calculations in this explosion."""
        return [col for col in list(NodeId._fields) if col in self.exploded_pks]

    def prep_table_to_explode(
        self: Self, table_name: str, table_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Assign table name and rename factoid column in preparation for explosion."""
        xbrl_factoid_name = pudl.transform.ferc1.FERC1_TFR_CLASSES[
            table_name
        ]().params.xbrl_factoid_name
        table_df = table_df.assign(table_name=table_name).rename(
            columns={xbrl_factoid_name: "xbrl_factoid"}
        )
        return table_df

    def boom(self: Self, tables_to_explode: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Explode a set of nested tables.

        There are seven main stages of this process:

        #. Prep all of the individual tables for explosion (via :meth:`prep_table_to_explode`).
        #. Concatenate all of the tables together (via :meth:`initial_explosion_concatenation`).
        #. Manage specific calculated values when a sizable minority of utilities report in a
           non-standard way (via :meth:`add_sizable_minority_corrections`).
        #. Reconcile the inter-table calculations (via :meth:`reconcile_intertable_calculations`)
        #. Annotate the data with additional metadata (via:meth:`XbrlCalculationForestFerc1.annotated_forest`).
        #. Identify the most granular ``xbrl_factoids`` (via
           :meth:`XbrlCalculationForestFerc1.leafy_meta`).
        #. Reconcile a calculation of least granular records using the most granular
           records (i.e. the seed to leaves calculation) (not yet implemented).

        Args:
            tables_to_explode: dictionary of table name (key) to transformed table (value).
        """
        exploded = (
            self.initial_explosion_concatenation(tables_to_explode)
            .pipe(self.calculate_intertable_non_total_calculations)
            .pipe(self.add_sizable_minority_corrections)
            .pipe(self.reconcile_intertable_calculations)
            .pipe(self.calculation_forest.leafy_data, value_col=self.value_col)
        )
        # remove the tag_ prefix. the tag verbiage is helpful in the context
        # of the forest construction but after that its distracting
        exploded.columns = exploded.columns.str.removeprefix("tags_")
        exploded = replace_dimension_columns_with_aggregatable(exploded)

        # TODO: Validate the root node calculations.
        # Verify that we get the same values for the root nodes using only the input
        # data from the leaf nodes:
        # root_calcs = self.calculation_forest.root_calculations

        # convert_cols_dtypes mostly to properly convert the booleans!
        return pudl.helpers.convert_cols_dtypes(exploded)

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
            tbl = self.prep_table_to_explode(table_name, table_df)
            explosion_tables.append(tbl)
        exploded = pd.concat(explosion_tables)

        # Identify which dimensions apply to the current explosion -- not all collections
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
            validate="many_to_one",
        )
        return exploded

    def calculate_intertable_non_total_calculations(
        self, exploded: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculate the inter-table non-total calculable xbrl_factoids."""
        # add the abs_diff column for the calculated fields
        calculated_df = pudl.transform.ferc1.calculate_values_from_components(
            calculation_components=self.exploded_calcs[
                ~self.exploded_calcs.is_within_table_calc
                & ~self.exploded_calcs.is_total_to_subdimensions_calc
            ],
            data=exploded,
            calc_idx=self.calc_idx,
            value_col=self.value_col,
            calc_to_data_merge_validation="many_to_many",
        )
        return calculated_df

    def add_sizable_minority_corrections(
        self: Self, calculated_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Identify and fix the utilities that report calcs off-by one other fact.

        We noticed that there are a sizable minority of utilities that report some
        calculated values with a different set of child subcomponents. Our tooling
        for these calculation expects all utilities to report in the same manner.
        So we have identified the handful of worst calculable ``xbrl_factiod``
        offenders :attr:`self.off_by_facts`. This method identifies data corrections
        for those :attr:`self.off_by_facts` and adds them into the exploded data table.

        The data corrections are identified by calculating the absolute difference
        between the reported value and calculable value from the standard set of
        subcomponents (via :func:`pudl.transform.ferc1.calculate_values_from_components`)
        and finding the child factiods that have the same value as the absolute difference.
        This indicates that the calculable parent factiod is off by that corresponding
        child fact.

        Relatedly, :meth:`add_sizable_minority_corrections_to_calcs` adds these
        :attr:`self.off_by_facts` to :attr:`self.exploded_calcs`.
        """
        if not self.off_by_facts:
            return calculated_df

        off_by = pd.DataFrame(self.off_by_facts)
        not_close = (
            calculated_df[~np.isclose(calculated_df.abs_diff, 0)]
            .set_index(list(NodeId._fields))
            # grab the parent side of the off_by facts
            .loc[off_by.set_index(list(NodeId._fields)).index.unique()]
            .reset_index()
        )
        off_by_fact = (
            calculated_df[calculated_df.row_type_xbrl != "correction"]
            .set_index(list(NodeId._fields))
            # grab the child side of the off_by facts
            .loc[
                off_by.set_index(
                    [f"{col}_off_by" for col in list(NodeId._fields)]
                ).index.unique()
            ]
            .reset_index()
        )
        # Identify the calculations with one missing other fact
        # when the diff is the same as the value of another fact, then that
        # calculated value could have been perfect with the addition of the
        # missing facoid.
        data_corrections = (
            pd.merge(
                left=not_close,
                right=off_by_fact,
                left_on=["report_year", "utility_id_ferc1", "abs_diff"],
                right_on=["report_year", "utility_id_ferc1", self.value_col],
                how="inner",
                suffixes=("", "_off_by"),
            )
            # use a dict/kwarg for assign so we can dynamically set the name of value_col
            # the correction value is the diff - not the abs_diff from not_close or value_col
            # from off_by_fact bc often the utility included a fact so this diff is negative
            .assign(
                **{
                    "xbrl_factoid": (
                        lambda x: x.xbrl_factoid
                        + "_off_by_"
                        + x.xbrl_factoid_off_by
                        + "_correction"
                    ),
                    self.value_col: lambda x: x["diff"],
                    "row_type_xbrl": "correction",
                }
            )[
                # drop all the _off_by and calc cols
                list(NodeId._fields)
                + ["report_year", "utility_id_ferc1", self.value_col, "row_type_xbrl"]
            ]
        )
        bad_utils = data_corrections.utility_id_ferc1.unique()
        logger.info(
            f"Adding {len(data_corrections)} from {len(bad_utils)} utilities that report differently."
        )
        dtype_calced = {
            col: dtype
            for (col, dtype) in calculated_df.dtypes.items()
            if col in data_corrections.columns
        }
        corrected = pd.concat(
            [calculated_df, data_corrections.astype(dtype_calced)]
        ).set_index(self.calc_idx)
        # now we are going to re-calculate just the fixed facts
        fixed_facts = corrected.loc[off_by.set_index(self.calc_idx).index.unique()]
        unfixed_facts = corrected.loc[
            corrected.index.difference(off_by.set_index(self.calc_idx).index.unique())
        ].reset_index()
        fixed_facts = self.calculate_intertable_non_total_calculations(
            exploded=fixed_facts.drop(
                columns=["calculated_value", "is_calc"]
            ).reset_index()
        )

        return pd.concat([unfixed_facts, fixed_facts.astype(unfixed_facts.dtypes)])

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
        logger.info("Checking inter-table, non-total to subdimension calcs.")
        # we've added calculated fields via calculate_intertable_non_total_calculations
        calculated_df = pudl.transform.ferc1.check_calculation_metrics(
            calculated_df=exploded, group_metric_checks=self.group_metric_checks
        )
        calculated_df = pudl.transform.ferc1.add_corrections(
            calculated_df=calculated_df,
            value_col=self.value_col,
            is_close_tolerance=pudl.transform.ferc1.IsCloseTolerance(),
            table_name=self.root_table,
            is_subdimension=False,
        )
        logger.info("Checking sub-total calcs.")
        subdimension_calcs = pudl.transform.ferc1.calculate_values_from_components(
            calculation_components=calculations_intertable[
                calculations_intertable.is_total_to_subdimensions_calc
            ],
            data=exploded.drop(columns=["calculated_value", "is_calc"]),
            calc_idx=self.calc_idx,
            value_col=self.value_col,
        )
        subdimension_calcs = pudl.transform.ferc1.check_calculation_metrics(
            calculated_df=subdimension_calcs,
            group_metric_checks=self.group_metric_checks,
        )
        return calculated_df


################################################################################
# XBRL Calculation Forests
################################################################################
class XbrlCalculationForestFerc1(BaseModel):
    """A class for manipulating groups of hierarchically nested XBRL calculations.

    We expect that the facts reported in less granular FERC tables like
    :ref:`core_ferc1__yearly_income_statements_sched114` and
    :ref:`core_ferc1__yearly_balance_sheet_assets_sched110` should be
    calculable from many individually reported granular values, based on the
    calculations encoded in the XBRL Metadata, and that these relationships should have
    a hierarchical tree structure. Several individual values from the less granular
    tables will appear as root nodes, and the leaves in the tree structure are the
    individually reported non-calculated values that make them up (i.e. the most
    granular values). Because the less granular tables have several distinct values in
    them, composed of disjunct sets of reported values, we have a forest (a group of
    several trees) rather than a single tree.

    The information required to build a calculation forest is most readily found in the
    :meth:`Exploder.exploded_calcs`. Seed nodes can be used to indicate which nodes
    should be the root(s) of the tree(s) we want to built.This can be used to prune
    irrelevant portions of the overall forest out of the exploded metadata. If no seeds
    are provided, then all of the nodes referenced in the exploded_calcs input dataframe
    will be used as seeds.

    This class makes heavy use of :mod:`networkx` to manage the graph that we build
    from calculation relationships and relies heavily on :mod:`networkx` terminology.
    """

    # Not sure if dynamically basing this on NodeId is really a good idea here.
    calc_cols: list[str] = list(NodeId._fields)
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
        source_nodes = [
            NodeId(*x)
            for x in exploded_calcs.set_index(self.parent_cols).index.to_list()
        ]
        target_nodes = [
            NodeId(*x) for x in exploded_calcs.set_index(self.calc_cols).index.to_list()
        ]
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
            self.tags.convert_dtypes()
            .set_index(self.calc_cols)
            .dropna(how="all")
            .to_dict(orient="index")
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
            .assign(tags=lambda x: np.where(x["tags"].isna(), {}, x["tags"]))
        )
        return node_attrs.set_index(self.calc_cols).to_dict(orient="index")

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
            for parent, child, weight in zip(parents, children, weights, strict=True)
        }
        return edge_attrs

    @cached_property
    def annotated_forest(self: Self) -> nx.DiGraph:
        """Annotate the calculation forest with node calculation weights and tags.

        The annotated forest should have exactly the same structure as the forest, but
        with additional data associated with each of the nodes. This method also does
        some error checking to try and ensure that the weights and tags that are being
        associated with the forest are internally self-consistent.

        We check whether there are multiple different weights associated with the same
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

        annotated_forest = self.propagate_node_attributes(annotated_forest)
        return annotated_forest

    def propagate_node_attributes(self: Self, annotated_forest: nx.DiGraph):
        """Propagate tags.

        Propagate tag values root-ward, leaf-wards &  to the _correction nodes. We
        propagate the tags root-ward first because we primarily manually compiled
        tags for the leaf nodes, so we want to send the values for the leafy tags
        root-ward first before trying to send tags leaf-ward.
        """
        tags_to_propagate = ["in_rate_base", "rate_base_category"]
        # Root-ward propagation
        for tag in tags_to_propagate:
            annotated_forest = _propagate_tag_rootward(annotated_forest, tag)
        ## Leaf-wards propagation
        annotated_forest = _propagate_tags_leafward(annotated_forest, tags_to_propagate)
        # Correction Records
        annotated_forest = _propagate_tags_to_corrections(annotated_forest)
        return annotated_forest

    def check_lost_tags(self: Self, lost_nodes: list[NodeId]) -> None:
        """Check whether any of the input lost nodes were also tagged nodes.

        It is not necessarily a problem if there are "lost" tags. This is mostly
        here as a debugging tool.
        """
        if lost_nodes:
            lost = pd.DataFrame(lost_nodes).set_index(self.calc_cols)
            tagged = self.tags.set_index(self.calc_cols)
            lost_tagged = tagged.index.intersection(lost.index)
            if not lost_tagged.empty:
                logger.debug(
                    "The following tagged nodes were lost in building the forest:\n"
                    f"{tagged.loc[lost_tagged].sort_index()}"
                )

    @staticmethod
    def check_conflicting_tags(annotated_forest: nx.DiGraph) -> None:
        """Check for conflicts between ancestor and descendant tags.

        This check should be applied before we have propagated tags via
        :meth:`propagate_node_attributes` so we can check conflicts within the tags
        that we've manually compiled.These kinds of conflicts are probably due to
        errors in the tagging metadata, and should be investigated.
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
        metadata to pass to :meth:`exploded_calcs_to_digraph`, so that all of the
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
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "depreciation_amortization_and_depletion_utility_plant_leased_to_others",
                "total",
                pd.NA,
                pd.NA,
            ),
            NodeId(
                "core_ferc1__yearly_utility_plant_summary_sched200",
                "depreciation_and_amortization_utility_plant_held_for_future_use",
                "total",
                pd.NA,
                pd.NA,
            ),
            NodeId(
                "core_ferc1__yearly_utility_plant_summary_sched200",
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
        """Identify all nodes that appear in the exploded_calcs but not in the full digraph.

        Because we removed the metadata and are now building the tree entirely based on
        the exploded_calcs, this should now never produce any orphans and is a bit redundant.
        """
        nodes = self.full_digraph.nodes
        orphans = []
        for idx_cols in [self.calc_cols, self.parent_cols]:
            orphans.extend(
                [
                    NodeId(*n)
                    for n in self.exploded_calcs.set_index(idx_cols).index
                    if NodeId(*n) not in nodes
                ]
            )
        return list(set(orphans))

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
        for parent, child in zip(path, path[1:], strict=False):
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
        color_map = dict(
            zip(self.table_names, colors[: len(self.table_names)], strict=True)
        )

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
        - Still need to validate the root node calculations.

        """
        # inner merge here because the nodes need to *both* be leaves in the forest
        # and need to show up in the data. If a node doesn't show up in the data itself
        # it won't have any non-node values like utility_id_ferc1 or report_year or any
        # $$ values.
        leafy_data = pd.merge(
            left=self.leafy_meta,
            right=exploded_data,
            how="inner",
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

        return self._add_layers_to_forest_as_table(df=layer0_df).convert_dtypes()

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
            .dropna(how="all", axis="rows")
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


def _propagate_tags_leafward(
    annotated_forest: nx.DiGraph, leafward_inherited_tags: list[str]
) -> nx.DiGraph:
    """Push a parent's tags down to its descendants.

    Only push the `leafward_inherited_tags` - others will be left alone.
    """
    existing_tags = nx.get_node_attributes(annotated_forest, "tags")
    for node, parent_tags in existing_tags.items():
        descendants = nx.descendants(annotated_forest, node)
        descendant_tags = {
            desc: {
                "tags": {
                    tag_name: parent_tags[tag_name]
                    for tag_name in leafward_inherited_tags
                    if tag_name in parent_tags
                }
                | existing_tags.get(desc, {})
            }
            for desc in descendants
        }
        nx.set_node_attributes(annotated_forest, descendant_tags)
    return annotated_forest


def _propagate_tag_rootward(
    annotated_forest: nx.DiGraph, tag_name: Literal["in_rate_base"]
) -> nx.DiGraph:
    """Set the tag for nodes when all of its children have same tag.

    This function returns the value of a tag, but also sets node attributes
    down the tree when all children of a node share the same tag.
    """

    def _get_tag(annotated_forest, node, tag_name):
        return annotated_forest.nodes.get(node, {}).get("tags", {}).get(tag_name)

    generations = list(nx.topological_generations(annotated_forest))
    for gen in reversed(generations):
        untagged_nodes = {
            node_id
            for node_id in gen
            if _get_tag(annotated_forest, node_id, tag_name) is None
        }
        for parent_node in untagged_nodes:
            child_tags = {
                _get_tag(annotated_forest, c, tag_name)
                for c in annotated_forest.successors(parent_node)
                if not c.xbrl_factoid.endswith("_correction")
            }
            non_null_tags = child_tags - {None}
            # sometimes, all children can share same tag but it's null.
            if len(child_tags) == 1 and non_null_tags:
                # actually assign the tag here but don't wipe out any other tags
                new_node_tag = non_null_tags.pop()
                existing_tags = nx.get_node_attributes(annotated_forest, "tags")
                node_tags = {
                    parent_node: {
                        "tags": {tag_name: new_node_tag}
                        | existing_tags.get(parent_node, {})
                    }
                }
                nx.set_node_attributes(annotated_forest, node_tags)
    return annotated_forest


def _propagate_tags_to_corrections(annotated_forest: nx.DiGraph) -> nx.DiGraph:
    existing_tags = nx.get_node_attributes(annotated_forest, "tags")
    correction_nodes = [
        node for node in annotated_forest if node.xbrl_factoid.endswith("_correction")
    ]
    correction_tags = {}
    for correction_node in correction_nodes:
        # for every correction node, we assume that that nodes parent tags can apply
        parents = list(annotated_forest.predecessors(correction_node))
        # all correction records should have a parent and only one
        if len(parents) != 1:
            raise AssertionError(
                f"Found more than one parent node for {correction_node=}\n{parents=}"
            )
        parent = parents[0]
        correction_tags[correction_node] = {
            "tags": existing_tags.get(parent, {})
            | existing_tags.get(correction_node, {})
        }
    nx.set_node_attributes(annotated_forest, correction_tags)
    return annotated_forest


def check_tag_propagation_compared_to_compiled_tags(
    df: pd.DataFrame,
    propagated_tag: Literal["in_rate_base"],
    _out_ferc1__explosion_tags: pd.DataFrame,
):
    """Check if tags got propagated.

    Args:
        df: table to check. This should be either the
            :func:`out_ferc1__yearly_rate_base`, ``exploded_balance_sheet_assets_ferc1``
            or ``exploded_balance_sheet_liabilities_ferc1``. The
            ``exploded_income_statement_ferc1`` table does not currently have propagated
            tags.
        propagated_tag: name of tag. Currently ``in_rate_base`` is the only propagated tag.
        _out_ferc1__explosion_tags: manually compiled tags. This table includes tags from
            many of the explosion tables so we will filter it before checking if the tag was
            propagated.

    Raises:
        AssertionError: If there are more manually compiled tags for the ``xbrl_factoids``
            in ``df`` than found in ``_out_ferc1__explosion_tags``.
        AssertionError: If there are more manually compiled tags for the correction
            ``xbrl_factoids`` in ``df`` than found in ``_out_ferc1__explosion_tags``.
    """
    # the tag df has all tags - not just those in a specific explosion
    # so we need to drop
    node_idx = list(NodeId._fields)
    df_filtered = df.filter(node_idx).drop_duplicates()
    df_tags = _out_ferc1__explosion_tags.merge(
        df_filtered, on=list(df_filtered.columns), how="right"
    )
    manually_tagged = df_tags[df_tags[propagated_tag].notnull()].xbrl_factoid.unique()
    detailed_tagged = df[df[propagated_tag].notnull()].xbrl_factoid.unique()
    if len(detailed_tagged) < len(manually_tagged):
        raise AssertionError(
            f"Found more {len(manually_tagged)} manually compiled tagged xbrl_factoids"
            " than tags in propagated detailed data."
        )
    manually_tagged_corrections = df_tags[
        df_tags[propagated_tag].notnull()
        & df_tags.xbrl_factoid.str.endswith("_correction")
    ].xbrl_factoid.unique()
    detailed_tagged_corrections = df[
        df[propagated_tag].notnull() & df.xbrl_factoid.str.endswith("_correction")
    ].xbrl_factoid.unique()
    if len(detailed_tagged_corrections) < len(manually_tagged_corrections):
        raise AssertionError(
            f"Found more {len(manually_tagged_corrections)} manually compiled "
            "tagged xbrl_factoids than tags in propagated detailed data."
        )


def check_for_correction_xbrl_factoids_with_tag(
    df: pd.DataFrame, propagated_tag: Literal["in_rate_base"]
):
    """Check if any correction records have tags.

    Args:
        df: table to check. This should be either the
            :func:`out_ferc1__yearly_rate_base`, ``exploded_balance_sheet_assets_ferc1``
            or ``exploded_balance_sheet_liabilities_ferc1``. The
            ``exploded_income_statement_ferc1`` table does not currently have propagated
            tags.
        propagated_tag: name of tag. Currently ``in_rate_base`` is the only propagated tag.

    Raises:
        AssertionError: If there are zero correction ``xbrl_factoids`` in ``df`` with tags.
    """
    detailed_tagged_corrections = df[
        df[propagated_tag].notnull() & df.xbrl_factoid.str.endswith("_correction")
    ].xbrl_factoid.unique()
    if len(detailed_tagged_corrections) == 0:
        raise AssertionError(
            "We expect there to be more than zero correction records with tags, but "
            f"found {len(detailed_tagged_corrections)}."
        )


check_specs_detailed_tables_tags = [
    {
        "asset": "out_ferc1__yearly_detailed_balance_sheet_assets",
        "tag_columns": ["in_rate_base", "utility_type"],
    },
    {
        "asset": "out_ferc1__yearly_detailed_balance_sheet_liabilities",
        "tag_columns": ["in_rate_base"],
    },
]


def make_check_tag_propagation(spec) -> AssetChecksDefinition:
    """Check the propagation of tags."""

    @asset_check(
        name="check_tag_propagation",
        asset=spec["asset"],
        additional_ins={"tags_df": AssetIn("_out_ferc1__detailed_tags")},
    )
    def _check(df: pd.DataFrame, tags_df: pd.DataFrame):
        for tag in spec["tag_columns"]:
            check_tag_propagation_compared_to_compiled_tags(df, tag, tags_df)
        return AssetCheckResult(passed=True)

    return _check


def make_check_correction_tags(spec) -> AssetChecksDefinition:
    """Check the propagation of tags."""

    @asset_check(
        name="check_correction_tags",
        asset=spec["asset"],
    )
    def _check(df):
        for tag in spec["tag_columns"]:
            check_for_correction_xbrl_factoids_with_tag(df, tag)
        return AssetCheckResult(passed=True)

    return _check


_tag_checks = [
    make_check_tag_propagation(spec) for spec in check_specs_detailed_tables_tags
] + [make_check_correction_tags(spec) for spec in check_specs_detailed_tables_tags]


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_ferc1__yearly_rate_base(
    out_ferc1__yearly_detailed_balance_sheet_assets: pd.DataFrame,
    out_ferc1__yearly_detailed_balance_sheet_liabilities: pd.DataFrame,
    core_ferc1__yearly_operating_expenses_sched320: pd.DataFrame,
    core_pudl__assn_ferc1_pudl_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Make a table of granular utility rate base data.

    This table contains granular data consisting of what utilities can
    include in their rate bases. This information comes from two core
    inputs: ``out_ferc1__yearly_detailed_balance_sheet_assets`` and
    ``out_ferc1__yearly_detailed_balance_sheet_liabilities``. These two detailed tables
    are generated from seven different core_ferc1_* accounting tables with
    nested calculations. We chose only the most granular data from these tables.
    See :class:`Exploder` for more details.

    This rate base table also contains one new "cash_working_capital" xbrl_factoid
    from :ref:`core_ferc1__yearly_operating_expenses_sched320` via
    :func:`prep_cash_working_capital`.

    We also disaggregate records that have nulls or totals in two of the key
    columns: ``utility_type`` and ``in_rate_base`` via
    :func:`disaggregate_null_or_total_tag`.
    """
    assets = out_ferc1__yearly_detailed_balance_sheet_assets
    liabilities = out_ferc1__yearly_detailed_balance_sheet_liabilities.assign(
        ending_balance=lambda x: -x.ending_balance
    )
    cash_working_capital = prep_cash_working_capital(
        core_ferc1__yearly_operating_expenses_sched320
    ).merge(
        core_pudl__assn_ferc1_pudl_utilities,
        on="utility_id_ferc1",
        how="left",
        validate="many_to_one",
    )

    # concat then select only the leafy exploded records that are in rate base
    rate_base_df = (
        pd.concat(
            [
                assets,
                liabilities,
                cash_working_capital,
            ]
        )
        .pipe(
            disaggregate_null_or_total_tag,
            tag_col="utility_type",
        )
        .pipe(
            disaggregate_null_or_total_tag,
            tag_col="in_rate_base",
        )
    )

    in_rate_base_df = rate_base_df[rate_base_df.in_rate_base == True]  # noqa: E712
    return in_rate_base_df.dropna(subset=["ending_balance"])


def replace_dimension_columns_with_aggregatable(df: pd.DataFrame) -> pd.DataFrame:
    """Replace the dimension columns with their aggregatable counterparts."""
    # some tables have a dimension column but we didn't augment them
    # with tags so they never got aggregatable_ columns so we skip those
    dimensions = [
        f
        for f in NodeId._fields
        if f not in ["table_name", "xbrl_factoid"] and f"aggregatable_{f}" in df
    ]
    dimensions_tags = [f"aggregatable_{d}" for d in dimensions]
    for dim in dimensions:
        df = df.assign(**{dim: lambda x: x[f"aggregatable_{dim}"]})  # noqa: B023
    return df.drop(columns=dimensions_tags)


@dataclass
class Ferc1DetailedCheckSpec:
    """Define some simple checks that can run on FERC 1 assets."""

    name: str
    asset: str
    idx: dict[int, int]


check_specs = [
    Ferc1DetailedCheckSpec(
        name="out_ferc1__yearly_rate_base_check_spec",
        asset="out_ferc1__yearly_rate_base",
        idx=[
            "report_year",
            "utility_id_ferc1",
            "xbrl_factoid",
            "utility_type",
            "plant_function",
            "plant_status",
            "table_name",
            "is_disaggregated_utility_type",
        ],
    ),
    Ferc1DetailedCheckSpec(
        name="out_ferc1__yearly_detailed_balance_sheet_assets_check_spec",
        asset="out_ferc1__yearly_detailed_balance_sheet_assets",
        idx=[
            "report_year",
            "utility_id_ferc1",
            "xbrl_factoid",
            "utility_type",
            "plant_function",
            "plant_status",
        ],
    ),
    Ferc1DetailedCheckSpec(
        name="out_ferc1__yearly_detailed_balance_sheet_liabilitiess_check_spec",
        asset="out_ferc1__yearly_detailed_balance_sheet_liabilities",
        idx=["report_year", "utility_id_ferc1", "xbrl_factoid", "utility_type"],
    ),
    Ferc1DetailedCheckSpec(
        name="out_ferc1__yearly_detailed_income_statements_check_spec",
        asset="out_ferc1__yearly_detailed_income_statements",
        idx=[
            "report_year",
            "utility_id_ferc1",
            "xbrl_factoid",
            "utility_type",
            "plant_function",
        ],
    ),
]


def make_idx_check(spec: Ferc1DetailedCheckSpec) -> AssetChecksDefinition:
    """Turn the Ferc1DetailedCheckSpec into an actual Dagster asset check."""

    @asset_check(asset=spec.asset, blocking=True)
    def _idx_check(df):
        """Check the primary keys of this table.

        We do this as an asset check instead of actually setting them as primary keys
        in the db schema because there are many expected nulls in these columns.
        """
        idx = spec.idx
        dupes = df[df.duplicated(idx, keep=False)]
        if "plant_function" in dupes:
            # this needs to be here bc we condensed two kinds of hydro
            # plant functions (conventional & pumped storage) into one
            # category for easier id-ing of all the hydro assets/liabiltiies
            dupes = dupes[dupes.plant_function != "hydraulic_production"]

        if not dupes.empty:
            raise AssertionError(
                "Found duplicate records given expected primary keys of the table:\n"
                f"{dupes.set_index(idx).sort_index()}"
            )
        return AssetCheckResult(passed=True)

    return _idx_check


_idx_checks = [make_idx_check(spec) for spec in check_specs]


def prep_cash_working_capital(
    core_ferc1__yearly_operating_expenses_sched320,
) -> pd.DataFrame:
    """Extract a new ``cash_working_capital`` ``xbrl_factoid`` for the rate base table.

    In standard ratemaking processes, utilities are allowed to include working
    capital - sometimes referred to as cash on hand or cash reverves - in their rate
    base. A standard ratemaking process considers the available rate-baseable working
    capital to be one eighth of the average operations and maintenance expense. This
    function grabs that expense and calculated this new ``xbrl_factoid`` in preparation
    to concatenate it with the rest of the assets and liabilities from the detailed rate
    base data.

    ``cash_working_capital`` is a new ``xbrl_factiod`` because it is not reported
    in the FERC1 data, but it is included in rate base so we had to calculate it.
    """
    # get the factoid name to grab the right part of the table
    xbrl_factoid_name = pudl.transform.ferc1.FERC1_TFR_CLASSES[
        "core_ferc1__yearly_operating_expenses_sched320"
    ]().params.xbrl_factoid_name
    # First grab the working capital out of the operating expense table.
    # then prep it for concatenating. Calculate working capital & add tags
    cash_working_capital_df = (
        core_ferc1__yearly_operating_expenses_sched320[
            core_ferc1__yearly_operating_expenses_sched320[xbrl_factoid_name]
            == "operations_and_maintenance_expenses_electric"
        ]
        .assign(
            dollar_value=lambda x: x.dollar_value.divide(8),
            xbrl_factoid="cash_working_capital",  # newly defined xbrl_factoid
            in_rate_base=True,
            rate_base_category="net_working_capital",
            aggregatable_utility_type="electric",
            table_name="core_ferc1__yearly_operating_expenses_sched320",
        )
        .drop(columns=[xbrl_factoid_name])
        # the assets/liabilites both use ending_balance for its main $$ column
        .rename(columns={"dollar_value": "ending_balance"})
    )
    return cash_working_capital_df


def disaggregate_null_or_total_tag(
    rate_base_df: pd.DataFrame,
    tag_col: str,
) -> pd.DataFrame:
    """Disaggregate records with an null or total value in the ``tag_col``.

    We have records in the rate base table with total and/or null values for
    key tag columns which we want to separate into component parts because the
    null or total values does not convey a level of detail we want for the
    rate base table. This is done in two steps:

    * :func:`get_tag_col_ratio` : for each ``report_year`` and ``utility_id_ferc1``,
      get a ratio of all of the ``ending_balance`` for all of the non-null and
      non-total tags.
    * use this ratio to disaggregate the ``ending_balance`` from records with total
      and null tag values across the other tag values.

    Args:
        rate_base_df: full table of rate base data.
        tag_col: column with the tags that contains null or total values to be
            disaggregated.

    """
    # this works for both the utility_type and in_rate_base tags columns because
    # for both tag columns the values that we want to disagregate are null and/or a "total"
    total_null_mask = (rate_base_df[tag_col] == "total") | rate_base_df[
        tag_col
    ].isnull()
    ratio_idx = ["report_year", "utility_id_ferc1"]
    ratio_df = get_column_value_ratio(
        # remove the total and/or null records because those are the values
        # we want to disaggreate so we can't have it in the columns to sum up
        rate_base_df=rate_base_df[~total_null_mask],
        ratio_idx=ratio_idx,
        column=tag_col,
    )
    disaggregated_df = (
        pd.merge(
            rate_base_df[total_null_mask],
            ratio_df,
            on=ratio_idx,
            how="left",
            validate="many_to_many",
            suffixes=("_total_or_null", ""),
        )
        # na values from this ratio_{tag_col} should be treated like a 100%.
        # because the ratio is of the non-total or non-null tags. But occasionally there
        # are no non-null or non-null tags. Which results in nulls in these ratio columns
        # during the left merge above. We want to preserve the ending balance's for these
        # records even if there is no way to disaggregate them.
        .assign(
            ending_balance=lambda x: x[f"ratio_{tag_col}"].fillna(1) * x.ending_balance,
        )
        .assign(**{f"is_disaggregated_{tag_col}": True})
        .drop(columns=[f"ratio_{tag_col}", f"{tag_col}_total_or_null"])
        # this automatically gets converted to a pandas Float64 which
        # results in nulls from any sum.
        .astype({"ending_balance": float})
    )
    rate_base_disaggregated_df = pd.concat(
        [
            rate_base_df[~total_null_mask].assign(
                **{f"is_disaggregated_{tag_col}": False}
            ),
            disaggregated_df,
        ]
    )
    if not np.isclose(
        new_balance := rate_base_disaggregated_df.ending_balance.sum(),
        old_balance := rate_base_df.ending_balance.sum(),
    ):
        logger.error(
            f"{tag_col}: New ending balance is not the same as the old ending balance: "
            f"{old_balance=}, {new_balance=}"
        )
    return rate_base_disaggregated_df.convert_dtypes().reset_index(drop=True)


def get_column_value_ratio(
    rate_base_df: pd.DataFrame, ratio_idx: list[str], column: str
) -> pd.DataFrame:
    """Calculate the percentage of the ``ending_balance`` within each value in the column.

    Make ratio column with a 0-1 value of the sum of ``ending_balance`` in each
    of the values in ``column`` within each ``ratio_idx``.

    In practice, this was built to be used within :func:`disaggregate_null_or_total_tag`.
    For each ``report_year``, ``utility_id_ferc1`` and value within the ``tag_col`` this
    function will calculate the ratio of ``ending_balance``. For example, if the tag
    column is ``utility_type`` and utility X has values of electric and gas, this function will calculate what ratio of that utility's annual
    ``ending_balance`` is electric and gas.
    """
    # get the sum of the balance in each of the values in tag_col
    grouped_df = (
        rate_base_df.groupby(ratio_idx + [column])[["ending_balance"]]
        .sum(min_count=1)
        .reset_index(level=[column])
        .pivot(columns=[column])
    )
    tag_values = grouped_df.columns.get_level_values(1)
    grouped_df.columns = tag_values

    grouped_df["abs_summed"] = abs(grouped_df).sum(axis=1)
    for tag_value in tag_values:
        grouped_df[f"ratio_{tag_value}"] = abs(grouped_df[tag_value]) / abs(
            grouped_df.abs_summed
        )
    ratio_df = (
        pd.DataFrame(
            grouped_df.filter(regex="^ratio_").stack(future_stack=False),
            columns=[f"ratio_{column}"],
        )
        .reset_index()
        .assign(**{column: lambda x: x[column].str.removeprefix("ratio_")})
    )
    assert all(
        ~ratio_df[f"ratio_{column}"].between(0, 1, inclusive="both")
        | ratio_df[f"ratio_{column}"].notnull()
    )
    return ratio_df
