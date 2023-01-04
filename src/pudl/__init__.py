"""The Public Utility Data Liberation (PUDL) Project."""

import pkg_resources

from . import (  # noqa: F401
    analysis,
    cli,
    convert,
    etl,
    extract,
    glue,
    helpers,
    load,
    logging_helpers,
    metadata,
    output,
    transform,
    validate,
    workspace,
)

logging_helpers.configure_root_logger()

__author__ = "Catalyst Cooperative"
__contact__ = "pudl@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__maintainer_email__ = "zane.selvans@catalyst.coop"
__version__ = pkg_resources.get_distribution("catalystcoop.pudl").version
__docformat__ = "restructuredtext en"
__description__ = "Tools for liberating public US electric utility data."
__long_description__ = """
This Public Utility Data Liberation (PUDL) project is a collection of tools
that allow programmatic access to and manipulation of many public data sets
related to electric utilities in the United States. These data sets are
often collected by state and federal agencies, but are publicized in ways
that are not well standardized, or intended for interoperability. PUDL
seeks to allow more transparent and useful access to this important public
data, with the goal of enabling climate advocates, academic researchers, and
data journalists to better understand the electricity system and its impacts
on climate.
"""
__projecturl__ = "https://catalyst.coop/pudl/"
__downloadurl__ = "https://github.com/catalyst-cooperative/pudl/"

TABLE_NAME_MAP_FERC1: dict[str, dict[str, str]] = {
    "fuel_ferc1": {
        "dbf": "f1_fuel",
        "xbrl": "steam_electric_generating_plant_statistics_large_plants_fuel_statistics_402",
    },
    "plants_steam_ferc1": {
        "dbf": "f1_steam",
        "xbrl": "steam_electric_generating_plant_statistics_large_plants_402",
    },
    "plants_small_ferc1": {
        "dbf": "f1_gnrt_plant",
        "xbrl": "generating_plant_statistics_410",
    },
    "plants_hydro_ferc1": {
        "dbf": "f1_hydro",
        "xbrl": "hydroelectric_generating_plant_statistics_large_plants_406",
    },
    "plants_pumped_storage_ferc1": {
        "dbf": "f1_pumped_storage",
        "xbrl": "pumped_storage_generating_plant_statistics_large_plants_408",
    },
    "plant_in_service_ferc1": {
        "dbf": "f1_plant_in_srvce",
        "xbrl": "electric_plant_in_service_204",
    },
    "purchased_power_ferc1": {
        "dbf": "f1_purchased_pwr",
        "xbrl": "purchased_power_326",
    },
    "electric_energy_sources_ferc1": {
        "dbf": "f1_elctrc_erg_acct",
        "xbrl": "electric_energy_account_401a",
    },
    "electric_energy_dispositions_ferc1": {
        "dbf": "f1_elctrc_erg_acct",
        "xbrl": "electric_energy_account_401a",
    },
    "utility_plant_summary_ferc1": {
        "dbf": "f1_utltyplnt_smmry",
        "xbrl": "summary_of_utility_plant_and_accumulated_provisions_for_depreciation_amortization_and_depletion_200",
    },
    "transmission_statistics_ferc1": {
        "dbf": "f1_xmssn_line",
        "xbrl": "transmission_line_statistics_422",
    },
    "electric_opex_ferc1": {
        "dbf": "f1_elc_op_mnt_expn",
        "xbrl": "electric_operations_and_maintenance_expenses_320",
    },
    "balance_sheet_liabilities_ferc1": {
        "dbf": "f1_bal_sheet_cr",
        "xbrl": "comparative_balance_sheet_liabilities_and_other_credits_110",
    },
    "balance_sheet_assets_ferc1": {
        "dbf": "f1_comp_balance_db",
        "xbrl": "comparative_balance_sheet_assets_and_other_debits_110",
    },
    # Special case for this table bc there are two dbf tables
    "income_statement_ferc1": {
        "dbf": ["f1_income_stmnt", "f1_incm_stmnt_2"],
        "xbrl": "statement_of_income_114",
    },
    "retained_earnings_ferc1": {
        "dbf": "f1_retained_erng",
        "xbrl": "retained_earnings_118",
    },
    "retained_earnings_appropriations_ferc1": {
        "dbf": "f1_retained_erng",
        "xbrl": "retained_earnings_appropriations_118",
    },
    "depreciation_amortization_summary_ferc1": {
        "dbf": "f1_dacs_epda",
        "xbrl": "summary_of_depreciation_and_amortization_charges_section_a_336",
    },
    "electric_plant_depreciation_changes_ferc1": {
        "dbf": "f1_accumdepr_prvsn",
        "xbrl": "accumulated_provision_for_depreciation_of_electric_utility_plant_changes_section_a_219",
    },
    "electric_plant_depreciation_functional_ferc1": {
        "dbf": "f1_accumdepr_prvsn",
        "xbrl": "accumulated_provision_for_depreciation_of_electric_utility_plant_functional_classification_section_b_219",
    },
    "cash_flow_ferc1": {
        "dbf": "f1_cash_flow",
        "xbrl": "statement_of_cash_flows_120",
    },
}
"""A mapping of PUDL DB table names to their XBRL and DBF source table names."""
