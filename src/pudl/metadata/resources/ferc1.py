"""Table definitions for the FERC Form 1 data group."""

from typing import Any

from pudl.metadata.codes import CODE_METADATA

PLANT_AGGREGATION_HAZARD = {
    "type": "custom",
    "description": (
        "FERC does not restrict respondents to report unique and non-duplicative plant records. "
        "There are sporadic instances of respondents reporting portions of plants and then the "
        "total plant (ex: unit 1, unit 2 and total). Use caution when aggregating."
    ),
}

PLANT_PRIMARY_KEY_TEXT = (
    "The best approximation for primary keys for this table would be: ``report_year``, ``utility_id_ferc1``, ``plant_name_ferc1``. "
    "FERC does not publish plant IDs. The main identifying column is ``plant_name_ferc1`` "
    "but that is a free-form string field and there are duplicate records."
)
DETAILED_ACCOUNTING_TABLES_WARNING = {
    "type": "custom",
    "description": (
        "The data from these xbrl_factoid tables contains nested totals and subtotals - making "
        "aggregations difficult. We used FERC 1's reported calculations to determine the "
        "most granular, non-duplicative records. The reported calculations are not always perfect - "
        "we've corrected many of them, but errors could still exist."
    ),
}

TABLE_DESCRIPTIONS = {
    "yearly_balance_sheet_assets_sched110": {
        "additional_summary_text": "utility assets and other debits.",
        "additional_source_text": "(Schedule 110)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_balance_sheet_liabilities_sched110": {
        "additional_summary_text": "utility liabilities and other credits.",
        "additional_source_text": "(Schedule 110)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_cash_flows_sched120": {
        "additional_summary_text": "utility cash flow.",
        "additional_source_text": "(Schedule 120)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_depreciation_summary_sched336": {
        "additional_summary_text": "depreciation and amortization of electric plant.",
        "additional_source_text": "(Schedule 336 - Section A)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": (
            "Electric Plant refers to FERC Accounts 403, 404, and 405. This table only "
            "contains information from Section A: Summary of depreciation and "
            "amortization changes."
        ),
    },
    "yearly_energy_sources_sched401": {
        "additional_summary_text": "sources of electric energy generated or purchased, exchanged and wheeled.",
        "additional_source_text": "(Schedule 401a)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": (
            "Electric Energy Account, sources only. Schedule 401a. Amount of "
            "electricity the utility obtained from each of several sources."
        ),
    },
    "yearly_energy_dispositions_sched401": {
        "additional_summary_text": "dispositions of electric energy sold, exchanged, or stored.",
        "additional_source_text": "(Schedule 401a)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": (
            "Electric Energy Account, dispositions only. Schedule 401a. Electricity "
            "utilities delivered to end users, internal losses, etc."
        ),
    },
    "yearly_operating_expenses_sched320": {
        "additional_summary_text": "operating and maintenance costs associated with producing electricity.",
        "additional_source_text": "(Schedule 320)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_depreciation_changes_sched219": {
        "additional_summary_text": "changes in accumulated provision for depreciation of electric utility plant.",
        "additional_source_text": "(Schedule 219 - Section A)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": "Electric utility plant refers to FERC Account 108.",
    },
    "yearly_depreciation_by_function_sched219": {
        "additional_summary_text": "ending balances in accumulated provision for depreciation of electric utility plant.",
        "additional_source_text": "(Schedule 219 - Section B)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": (
            "Electric utility plant refers to FERC Account 108. Section B includes the "
            "Balances at End of Year According to Functional Classification (plant_function)."
        ),
    },
    "yearly_sales_by_rate_schedules_sched304": {
        "additional_summary_text": "utilities' electric sales from all rate schedules in effect throughout the year.",
        "additional_source_text": "(Schedule 304)",
        "usage_warnings": [
            "aggregation_hazard",
            {
                "type": "custom",
                "description": "Values in rate_schedule_description are free-form strings.",
            },
            {
                "type": "custom",
                "description": (
                    "Data prior to 2021 does not include information in columns: rate_schedule_type and billing_status."
                ),
            },
            {
                "type": "custom",
                "description": (
                    "Units of revenue_per_kwh are suspected to include a mix of dollars and possibly cents."
                ),
            },
        ],
        "additional_details_text": (
            """The pre-2021 data in this table (extracted from FoxProDB vs. XBRL) is
extremely unstructured. Where the post-2020 data (from XBRL) sorts the data
into rate schedule types: residential, industrial, commercial,
public_lighting, public_authorities, railroads, interdepartmental,
provision_for_rate_refund, commercial_and_industrial, total, and billing
status: billed, unbilled, total, the pre-2021 data stuffs all of that
information (if you're lucky) into the rate_schedule_description column.
There's no point trying to parse through the pre 2021
rate_schedule_description column en masse because it's just too messy. The
contents of rate_schedule_description often contain numbers and acronyms
that have little to no meaning out of context. The table is structured
somewhat like the FERC1 small generators table with headings about rate
structure type also embedded into the rate_schedule_description column. To
all who dare, beware.

This table is a combination of one pre-2021 (DBF) table and nine post-2020
(XBRL) tables--one for each rate schedule type plus totals--hence increase
in data clarity post-2020. The rate_schedule_type and billing_status
columns are only relevant for post-2020 data as they can be reliably parsed
from each of the tables and incorporated into columns. The
rate_schedule_description is supposed to contain sub-rate_schedule_type
names for charges (Ex: Residential 1, Residential 2, etc.). However, the
pre-2021 data contains a little bit of everything (or nothing) and the
post-2020 has some totals or wonky data thrown in. That's to say, even when
working with post-2020 data, be wary of aggregating the data. That's what
the "total" rows are for.

The values that come from from the totals table are marked with the string
"total" in the rate_schedule_description column. The totals table is a
product of the transition to XBRL, so these distinguishable totals are only
available for data post-2020 (otherwise you could try keyword searching for
"total" in rate_schedule_description). The total table contains two types of
totals, the utility totals across all rate schedules in a given year
(marked with rate_schedule_description = "total" and rate_schedule_type =
"total") and each of the utility's individual rate schedule totals in a
given year (marked with rate_schedule_description = "total" and
rate_schedule_type = "residential" or any other rate schedule type).

The rate schedule based XBRL tables only report billed values whereas the
total tables report billed, unbilled, and total values. (See the column
description for more info on the difference between billed and unbilled).
This is important to consider if you're endeavoring to compare the subtotal
values with the total values. We have not attempted to fix or verify any
subtotals or totals that don't add up.

Another important note is the possibility of unit discrepancies in certain
columns. The revenue_per_kwh column does not specify reporting units, and
closer inspection of the data reveals two clear peaks approximate two orders
of magnitude apart. This indicates that values may be reported in both
dollars and cents. However, because the price of energy per kwh varies
so much regionally, we cannot guarantee which is which and have not put
any cleaning mechanisms in place to account for this."""
        ),
    },
    "yearly_steam_plants_fuel_sched402": {
        "additional_summary_text": "fuel cost and quantity for steam plants with a capacity of 25+ MW, internal combustion and gas-turbine plants of 10+ MW, and all nuclear plants. ",
        "additional_source_text": "(Schedule 402)",
        "usage_warnings": [
            {
                "type": "custom",
                "description": "The ``fuel_type_code_pudl`` is inferred from a free-form string field.",
            }
        ],
        "additional_details_text": (
            "This table is a subset of the steam plant table reported on page 402 of FERC Form 1."
        ),
    },
    "yearly_income_statements_sched114": {
        "additional_summary_text": "utility income statements.",
        "additional_source_text": "(Schedule 114)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_other_regulatory_liabilities_sched278": {
        "additional_summary_text": "utilities' other regulatory liabilities, including rate order docket number.",
        "additional_source_text": "(Schedule 278)",
        "additional_primary_key_text": "Respondents are able to enter any number of liabilities across many rows. There are no IDs or set fields enforced in the original table.",
        "usage_warnings": [
            {
                "type": "custom",
                "description": "The ``description`` column is a free-form string.",
            }
        ],
    },
    "yearly_plant_in_service_sched204": {
        "additional_summary_text": "utilities' balances and changes to FERC Electric Plant in Service accounts.",
        "additional_source_text": "(Schedule 204)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": (
            "Account numbers correspond to the FERC Uniform "
            "System of Accounts for Electric Plant, which is defined in Code of "
            "Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. "
            "(See e.g. https://www.law.cornell.edu/cfr/text/18/part-101). Each FERC "
            "respondent reports starting and ending balances for each account "
            "annually. Balances are organization wide, and are not broken down on a "
            "per-plant basis. End of year balance should equal beginning year balance "
            "plus the sum of additions, retirements, adjustments, and transfers."
        ),
    },
    "yearly_hydroelectric_plants_sched406": {
        "additional_summary_text": "plant statistics for large hydroelectric generating plants.",
        "additional_source_text": "(Schedule 406)",
        "usage_warnings": [
            {
                "type": "custom",
                "description": "The ``plant_type`` and ``construction_type`` are standardized into categorical values from free-form strings.",
            },
            PLANT_AGGREGATION_HAZARD,
        ],
        "additional_primary_key_text": PLANT_PRIMARY_KEY_TEXT,
        "additional_details_text": (
            "Large plants have an installed nameplate capacity of more than 10 MW."
        ),
    },
    "yearly_pumped_storage_plants_sched408": {
        "additional_summary_text": "plant statistics for hydroelectric pumped storage plants with an installed nameplate capacity of 10+ MW.",
        "additional_source_text": "(Schedule 408)",
        "usage_warnings": [PLANT_AGGREGATION_HAZARD],
        "additional_primary_key_text": PLANT_PRIMARY_KEY_TEXT,
        "additional_details_text": "As reported in Schedule 408 of FERC Form 1.",
    },
    "yearly_small_plants_sched410": {
        "additional_summary_text": (
            "plant statistics for internal combustion plants, gas turbine-plants, "
            "conventional hydro plants, and pumped storage plants with less than 10 MW "
            "installed nameplate capacity and steam plants with less than 25 MW "
            "installed nameplate capacity."
        ),
        "additional_source_text": "(Schedule 410)",
        "usage_warnings": [PLANT_AGGREGATION_HAZARD],
        "additional_primary_key_text": PLANT_PRIMARY_KEY_TEXT,
        "additional_details_text": (
            """As reported on FERC Form 1 Schedule 410 (pages 410-411)
and extracted from the FERC Visual FoxPro and XBRL. See our
``pudl.extract.ferc1.TABLE_NAME_MAP_FERC1`` for links to the raw tables.

The raw version of this table is more like a digitized PDF than an actual data table.
The rows contain lots of information in addition to what the columns might suggest.
For instance, a single column may contain header rows, note rows, and total rows. This
extraneous information is useful, but it prevents proper analysis when mixed in with the
rest of the values data in the column. We employ a couple of data transformations to
extract these rows from the data and preserve some of the information they contain
(fuel type, plant type, FERC license, or general notes about the plant) in separate
columns."""
        ),
    },
    "yearly_steam_plants_sched402": {
        "additional_summary_text": (
            "plant statistics for steam plants with a capacity of 25+ MW, "
            "internal combustion and gas-turbine plants of 10+ MW, and all nuclear "
            "plants."
        ),
        "additional_source_text": "(Schedule 402)",
        "usage_warnings": [PLANT_AGGREGATION_HAZARD],
        "additional_primary_key_text": PLANT_PRIMARY_KEY_TEXT,
    },
    "yearly_purchased_power_and_exchanges_sched326": {
        "additional_summary_text": (
            "purchased power (Account 555) including power exchanges (transactions "
            "involving a balancing of debits and credits for energy, capacity, etc.) "
            "and any settlements for imbalanced exchanges."
        ),
        "additional_source_text": "(Schedule 326)",
        "usage_warnings": ["free_text"],
        "additional_details_text": (
            "This table has data about inter-utility power purchases. This "
            "includes how much electricity was purchased, how much it cost, and who it was "
            "purchased from. Unfortunately the field describing which other utility the power was "
            "being bought from (``seller_name``) is poorly standardized, making it difficult to "
            "correlate with other data.\n\n"
            "Purchased Power is considered FERC Account 555 according to FERC's "
            "Uniform System of Accounts. Reported on pages 326-327 of FERC Form 1."
        ),
    },
    "yearly_transmission_lines_sched422": {
        "additional_summary_text": "statistics about transmission lines.",
        "additional_source_text": "(Schedule 422)",
        "additional_primary_key_text": (
            "Each record of this table is supposed to represent one stretch of "
            "a transmission line, but there are no IDs and many nulls in the fields "
            "which would nominally distinguish unique transmission lines."
        ),
        "usage_warnings": [
            "free_text",
            "aggregation_hazard",
        ],
        "additional_details_text": (
            "Information "
            "describing transmission lines, the cost of lines, annual operating and "
            "capital expenses, etc. "
            "This table includes transmission lines having nominal voltage of 132 kilovolts or greater. "
            "Transmission lines below these voltages are required to be reported in group "
            "totals only for each voltage."
        ),
    },
    "yearly_utility_plant_summary_sched200": {
        "additional_summary_text": (
            "utility plant and accumulated provisions for depreciation, "
            "amortization and depletion of utility plant assets."
        ),
        "additional_source_text": "(Schedule 200)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_retained_earnings_sched118": {
        "additional_summary_text": "utilities' statements of retained earnings.",
        "additional_source_text": "(Schedule 118)",
        "usage_warnings": ["aggregation_hazard"],
    },
    "yearly_operating_revenues_sched300": {
        "additional_summary_text": "utilities' electric operating revenues.",
        "additional_source_text": "(Schedule 300)",
        "usage_warnings": ["aggregation_hazard"],
        "additional_details_text": (
            "This table includes only the structured part of schedule 300. "
            "There are a number of revenue_type's that do not have sales_mwh,"
            "or avg_customers_per_month provided, in which case these columns"
            "will be NULL."
        ),
    },
}

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_ferc1__yearly_balance_sheet_assets_sched110": {
        "description": TABLE_DESCRIPTIONS["yearly_balance_sheet_assets_sched110"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "record_id",
                "asset_type",
                "ending_balance",
                "starting_balance",
                "ferc_account",
                "balance",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "asset_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_balance_sheet_liabilities_sched110": {
        "description": TABLE_DESCRIPTIONS["yearly_balance_sheet_liabilities_sched110"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "starting_balance",
                "ending_balance",
                "liability_type",
                "balance",
                "ferc_account",
                "row_type_xbrl",
                "utility_type",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "liability_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_cash_flows_sched120": {
        "description": TABLE_DESCRIPTIONS["yearly_cash_flows_sched120"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "amount_type",
                "amount",
                "balance",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "amount_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_depreciation_summary_sched336": {
        "description": TABLE_DESCRIPTIONS["yearly_depreciation_summary_sched336"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "plant_function",
                "ferc_account_label",
                "ferc_account",
                "dollar_value",
                "utility_type",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "plant_function",
                "ferc_account_label",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_energy_sources_sched401": {
        "description": TABLE_DESCRIPTIONS["yearly_energy_sources_sched401"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "energy_source_type",
                "row_type_xbrl",
                "energy_mwh",
                "record_id",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "energy_source_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_energy_dispositions_sched401": {
        "description": TABLE_DESCRIPTIONS["yearly_energy_dispositions_sched401"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "energy_disposition_type",
                "row_type_xbrl",
                "energy_mwh",
                "record_id",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "energy_disposition_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_operating_expenses_sched320": {
        "description": TABLE_DESCRIPTIONS["yearly_operating_expenses_sched320"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "dollar_value",
                "expense_type",
                "record_id",
                "utility_type",
                "ferc_account",
                "row_type_xbrl",
            ],
            "primary_key": ["utility_id_ferc1", "report_year", "expense_type"],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_depreciation_changes_sched219": {
        "description": TABLE_DESCRIPTIONS["yearly_depreciation_changes_sched219"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "depreciation_type",
                "plant_status",
                "utility_type",
                "dollar_value",
                "record_id",
                "balance",
                "ferc_account",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "depreciation_type",
                "plant_status",
                "utility_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_depreciation_by_function_sched219": {
        "description": TABLE_DESCRIPTIONS["yearly_depreciation_by_function_sched219"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "depreciation_type",
                "plant_function",
                "plant_status",
                "utility_type",
                "ending_balance",
                "record_id",
                "balance",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "depreciation_type",
                "plant_function",
                "plant_status",
                "utility_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_sales_by_rate_schedules_sched304": {
        "description": TABLE_DESCRIPTIONS["yearly_sales_by_rate_schedules_sched304"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "rate_schedule_type",
                "billing_status",
                "rate_schedule_description",
                "sales_mwh",
                "dollar_value",
                "avg_customers_per_month",
                "kwh_per_customer",
                "revenue_per_kwh",
                "record_id",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc__codes_accounts": {
        "description": {
            "additional_summary_text": "account numbers from the FERC Uniform System of Accounts for Electric Plant.",
            "additional_details_text": "These codes are defined in Code of Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g. https://www.law.cornell.edu/cfr/text/18/part-101).",
        },
        "schema": {
            "fields": ["ferc_account_id", "ferc_account_description"],
            "primary_key": ["ferc_account_id"],
        },
        "sources": ["ferc1"],
        "etl_group": "static_ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_steam_plants_fuel_sched402": {
        "description": TABLE_DESCRIPTIONS["yearly_steam_plants_fuel_sched402"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "plant_name_ferc1",
                "fuel_type_code_pudl",
                "fuel_units",
                "fuel_consumed_units",
                "fuel_mmbtu_per_unit",
                "fuel_cost_per_unit_burned",
                "fuel_cost_per_unit_delivered",
                "fuel_cost_per_mmbtu",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_income_statements_sched114": {
        "description": TABLE_DESCRIPTIONS["yearly_income_statements_sched114"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "income_type",
                "dollar_value",
                "balance",
                "ferc_account",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "income_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_other_regulatory_liabilities_sched278": {
        "description": TABLE_DESCRIPTIONS[
            "yearly_other_regulatory_liabilities_sched278"
        ],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "description",
                "ending_balance",
                "starting_balance",
                "increase_in_other_regulatory_liabilities",
                "account_detail",
                "decrease_in_other_regulatory_liabilities",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_plant_in_service_sched204": {
        "description": TABLE_DESCRIPTIONS["yearly_plant_in_service_sched204"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "ferc_account_label",
                "ferc_account",
                "row_type_xbrl",
                "starting_balance",
                "additions",
                "retirements",
                "adjustments",
                "transfers",
                "ending_balance",
                "record_id",
                "utility_type",
                "plant_status",
            ],
            "primary_key": ["utility_id_ferc1", "report_year", "ferc_account_label"],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_pudl__assn_ferc1_pudl_plants": {
        "description": {
            "additional_summary_text": "FERC 1 plants and their manually assigned PUDL plant IDs.",
            "additional_details_text": "FERC does not assign IDs to plants, so each FERC 1 plant is identified by a ``utility_id_ferc1`` and a ``plant_name_ferc1``.",
        },
        "schema": {
            "fields": ["utility_id_ferc1", "plant_name_ferc1", "plant_id_pudl"],
            "primary_key": ["utility_id_ferc1", "plant_name_ferc1"],
            "foreign_key_rules": {
                "fields": [
                    ["utility_id_ferc1", "plant_name_ferc1"],
                ],
            },
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_hydroelectric_plants_sched406": {
        "description": TABLE_DESCRIPTIONS["yearly_hydroelectric_plants_sched406"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "plant_name_ferc1",
                "project_num",
                "plant_type",
                "construction_type",
                "construction_year",
                "installation_year",
                "capacity_mw",
                "peak_demand_mw",
                "plant_hours_connected_while_generating",
                "net_capacity_favorable_conditions_mw",
                "net_capacity_adverse_conditions_mw",
                "avg_num_employees",
                "net_generation_mwh",
                "capex_land",
                "capex_structures",
                "capex_facilities",
                "capex_equipment",
                "capex_roads",
                "asset_retirement_cost",
                "capex_total",
                "capex_per_mw",
                "opex_operations",
                "opex_water_for_power",
                "opex_hydraulic",
                "opex_electric",
                "opex_generation_misc",
                "opex_rents",
                "opex_engineering",
                "opex_structures",
                "opex_dams",
                "opex_plant",
                "opex_misc_plant",
                "opex_total",
                "opex_per_mwh",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_pumped_storage_plants_sched408": {
        "description": TABLE_DESCRIPTIONS["yearly_pumped_storage_plants_sched408"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "plant_name_ferc1",
                "project_num",
                "construction_type",
                "construction_year",
                "installation_year",
                "capacity_mw",
                "peak_demand_mw",
                "plant_hours_connected_while_generating",
                "plant_capability_mw",
                "avg_num_employees",
                "net_generation_mwh",
                "energy_used_for_pumping_mwh",
                "net_load_mwh",
                "capex_land",
                "capex_structures",
                "capex_facilities",
                "capex_wheels_turbines_generators",
                "capex_equipment_electric",
                "capex_equipment_misc",
                "capex_roads",
                "asset_retirement_cost",
                "capex_total",
                "capex_per_mw",
                "opex_operations",
                "opex_water_for_power",
                "opex_pumped_storage",
                "opex_electric",
                "opex_generation_misc",
                "opex_rents",
                "opex_engineering",
                "opex_structures",
                "opex_dams",
                "opex_plant",
                "opex_misc_plant",
                "opex_production_before_pumping",
                "opex_pumping",
                "opex_total",
                "opex_per_mwh",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_small_plants_sched410": {
        "description": TABLE_DESCRIPTIONS["yearly_small_plants_sched410"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "plant_name_ferc1",
                "plant_type",
                "license_id_ferc1",
                "construction_year",
                "capacity_mw",
                "peak_demand_mw",
                "net_generation_mwh",
                "capex_total",
                "capex_per_mw",
                "opex_operations",
                "opex_fuel",
                "opex_maintenance",
                "fuel_type",
                "fuel_cost_per_mmbtu",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_steam_plants_sched402": {
        "description": TABLE_DESCRIPTIONS["yearly_steam_plants_sched402"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "plant_name_ferc1",
                "plant_type",
                "construction_type",
                "construction_year",
                "installation_year",
                "capacity_mw",
                "peak_demand_mw",
                "plant_hours_connected_while_generating",
                "plant_capability_mw",
                "water_limited_capacity_mw",
                "not_water_limited_capacity_mw",
                "avg_num_employees",
                "net_generation_mwh",
                "capex_land",
                "capex_structures",
                "capex_equipment",
                "capex_total",
                "capex_per_mw",
                "opex_operations",
                "opex_fuel",
                "opex_coolants",
                "opex_steam",
                "opex_steam_other",
                "opex_transfer",
                "opex_electric",
                "opex_misc_power",
                "opex_rents",
                "opex_allowances",
                "opex_engineering",
                "opex_structures",
                "opex_boiler",
                "opex_plants",
                "opex_misc_steam",
                "opex_production_total",
                "opex_per_mwh",
                "asset_retirement_cost",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__codes_power_purchase_types": {
        "description": {
            "additional_summary_text": "electric power purchase types.",
        },
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["purchase_type_code"]]},
        },
        "encoder": CODE_METADATA["core_ferc1__codes_power_purchase_types"],
        "sources": ["ferc1"],
        "etl_group": "static_ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_purchased_power_and_exchanges_sched326": {
        "description": TABLE_DESCRIPTIONS[
            "yearly_purchased_power_and_exchanges_sched326"
        ],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "seller_name",
                "purchase_type_code",
                "tariff",
                "billing_demand_mw",
                "non_coincident_peak_demand_mw",
                "coincident_peak_demand_mw",
                "purchased_mwh",
                "purchased_storage_mwh",
                "purchased_other_than_storage_mwh",
                "received_mwh",
                "delivered_mwh",
                "demand_charges",
                "energy_charges",
                "other_charges",
                "total_settlement",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_transmission_lines_sched422": {
        "description": TABLE_DESCRIPTIONS["yearly_transmission_lines_sched422"],
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "start_point",
                "end_point",
                "operating_voltage_kv",
                "designed_voltage_kv",
                "supporting_structure_type",
                "transmission_line_length_miles",
                "transmission_line_and_structures_length_miles",
                "num_transmission_circuits",
                "conductor_size_and_material",
                "capex_land",
                "capex_other",
                "capex_total",
                "opex_operations",
                "opex_maintenance",
                "opex_rents",
                "opex_total",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_pudl__assn_ferc1_pudl_utilities": {
        "description": {
            "additional_summary_text": "PUDL utility IDs and PUDL-assigned FERC1 utility IDs.",
            "additional_details_text": "This table maps two manually assigned utility IDs: a PUDL ID and a FERC1 ID. The PUDL IDs link EIA and FERC1 utilities. The PUDL FERC1 IDs link records from older DBF respondent IDs and new XBRL entity IDs via :ref:`core_pudl__assn_ferc1_dbf_pudl_utilities` and :ref:`core_pudl__assn_ferc1_xbrl_pudl_utilities` respectively. This table is generated from a table stored in the PUDL repository: src/package_data/glue/utility_id_pudl.csv",
        },
        "schema": {
            "fields": ["utility_id_ferc1", "utility_name_ferc1", "utility_id_pudl"],
            "primary_key": ["utility_id_ferc1"],
            "foreign_key_rules": {"fields": [["utility_id_ferc1"]]},
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "core_pudl__assn_ferc1_dbf_pudl_utilities": {
        "description": {
            "additional_summary_text": "PUDL-assigned FERC1 utility IDs and the native FERC1 DBF utility IDs originally reported as ``respondent_id``.",
        },
        "schema": {
            "fields": ["utility_id_ferc1", "utility_id_ferc1_dbf"],
            "primary_key": ["utility_id_ferc1_dbf"],
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "core_pudl__assn_ferc1_xbrl_pudl_utilities": {
        "description": {
            "additional_summary_text": "PUDL-assigned FERC1 utility IDs and the native FERC1 XBRL utility IDs originally reported as ``entity_id``.",
        },
        "schema": {
            "fields": ["utility_id_ferc1", "utility_id_ferc1_xbrl"],
            "primary_key": ["utility_id_ferc1_xbrl"],
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_utility_plant_summary_sched200": {
        "description": TABLE_DESCRIPTIONS["yearly_utility_plant_summary_sched200"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "utility_type_other",
                "utility_plant_asset_type",
                "row_type_xbrl",
                "ending_balance",
                "record_id",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "utility_plant_asset_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_retained_earnings_sched118": {
        "description": TABLE_DESCRIPTIONS["yearly_retained_earnings_sched118"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "record_id",
                "earnings_type",
                "starting_balance",
                "ending_balance",
                "balance",
                "ferc_account",
                "row_type_xbrl",
                "utility_type",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "earnings_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "core_ferc1__yearly_operating_revenues_sched300": {
        "description": TABLE_DESCRIPTIONS["yearly_operating_revenues_sched300"],
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "report_year",
                "record_id",
                "revenue_type",
                "dollar_value",
                "sales_mwh",
                "avg_customers_per_month",
                "ferc_account",
                "utility_type",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "revenue_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_balance_sheet_assets_sched110": {
        "description": TABLE_DESCRIPTIONS["yearly_balance_sheet_assets_sched110"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "asset_type",
                "balance",
                "ending_balance",
                "ferc_account",
                "row_type_xbrl",
                "starting_balance",
                "utility_type",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "asset_type",
            ],
        },
        "field_namespace": "ferc1",
        "etl_group": "outputs",
        "sources": ["ferc1"],
    },
    "out_ferc1__yearly_balance_sheet_liabilities_sched110": {
        "description": TABLE_DESCRIPTIONS["yearly_balance_sheet_liabilities_sched110"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "starting_balance",
                "ending_balance",
                "liability_type",
                "balance",
                "ferc_account",
                "row_type_xbrl",
                "utility_type",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "liability_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_cash_flows_sched120": {
        "description": TABLE_DESCRIPTIONS["yearly_cash_flows_sched120"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "amount_type",
                "amount",
                "balance",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "amount_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_depreciation_summary_sched336": {
        "description": TABLE_DESCRIPTIONS["yearly_depreciation_summary_sched336"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_function",
                "ferc_account_label",
                "ferc_account",
                "utility_type",
                "dollar_value",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "plant_function",
                "ferc_account_label",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_energy_dispositions_sched401": {
        "description": TABLE_DESCRIPTIONS["yearly_energy_dispositions_sched401"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "energy_disposition_type",
                "row_type_xbrl",
                "energy_mwh",
                "record_id",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "energy_disposition_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_energy_sources_sched401": {
        "description": TABLE_DESCRIPTIONS["yearly_energy_sources_sched401"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "energy_source_type",
                "row_type_xbrl",
                "energy_mwh",
                "record_id",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "energy_source_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_operating_expenses_sched320": {
        "description": TABLE_DESCRIPTIONS["yearly_operating_expenses_sched320"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "dollar_value",
                "expense_type",
                "utility_type",
                "record_id",
                "ferc_account",
                "row_type_xbrl",
            ],
            "primary_key": ["utility_id_ferc1", "report_year", "expense_type"],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_operating_revenues_sched300": {
        "description": TABLE_DESCRIPTIONS["yearly_operating_revenues_sched300"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "revenue_type",
                "dollar_value",
                "sales_mwh",
                "avg_customers_per_month",
                "ferc_account",
                "utility_type",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "revenue_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_depreciation_changes_sched219": {
        "description": TABLE_DESCRIPTIONS["yearly_depreciation_changes_sched219"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "depreciation_type",
                "plant_status",
                "utility_type",
                "dollar_value",
                "record_id",
                "balance",
                "ferc_account",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "depreciation_type",
                "plant_status",
                "utility_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_depreciation_by_function_sched219": {
        "description": TABLE_DESCRIPTIONS["yearly_depreciation_by_function_sched219"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "depreciation_type",
                "plant_function",
                "plant_status",
                "utility_type",
                "ending_balance",
                "record_id",
                "balance",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "depreciation_type",
                "plant_function",
                "plant_status",
                "utility_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_sales_by_rate_schedules_sched304": {
        "description": TABLE_DESCRIPTIONS["yearly_sales_by_rate_schedules_sched304"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "rate_schedule_type",
                "billing_status",
                "rate_schedule_description",
                "sales_mwh",
                "dollar_value",
                "avg_customers_per_month",
                "kwh_per_customer",
                "revenue_per_kwh",
                "record_id",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_income_statements_sched114": {
        "description": TABLE_DESCRIPTIONS["yearly_income_statements_sched114"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "utility_type",
                "income_type",
                "dollar_value",
                "balance",
                "ferc_account",
                "row_type_xbrl",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "income_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_other_regulatory_liabilities_sched278": {
        "description": TABLE_DESCRIPTIONS[
            "yearly_other_regulatory_liabilities_sched278"
        ],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "description",
                "ending_balance",
                "starting_balance",
                "increase_in_other_regulatory_liabilities",
                "account_detail",
                "decrease_in_other_regulatory_liabilities",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_retained_earnings_sched118": {
        "description": TABLE_DESCRIPTIONS["yearly_retained_earnings_sched118"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "record_id",
                "earnings_type",
                "starting_balance",
                "ending_balance",
                "balance",
                "ferc_account",
                "row_type_xbrl",
                "utility_type",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "earnings_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_transmission_lines_sched422": {
        "description": TABLE_DESCRIPTIONS["yearly_transmission_lines_sched422"],
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "start_point",
                "end_point",
                "operating_voltage_kv",
                "designed_voltage_kv",
                "supporting_structure_type",
                "transmission_line_length_miles",
                "transmission_line_and_structures_length_miles",
                "num_transmission_circuits",
                "conductor_size_and_material",
                "capex_land",
                "capex_other",
                "capex_total",
                "opex_operations",
                "opex_maintenance",
                "opex_rents",
                "opex_total",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_utility_plant_summary_sched200": {
        "description": TABLE_DESCRIPTIONS["yearly_utility_plant_summary_sched200"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "utility_type",
                "utility_type_other",
                "utility_plant_asset_type",
                "row_type_xbrl",
                "ending_balance",
                "record_id",
            ],
            "primary_key": [
                "utility_id_ferc1",
                "report_year",
                "utility_type",
                "utility_plant_asset_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_steam_plants_sched402": {
        "description": TABLE_DESCRIPTIONS["yearly_steam_plants_sched402"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_id_ferc1",
                "plant_name_ferc1",
                "asset_retirement_cost",
                "avg_num_employees",
                "capacity_factor",
                "capacity_mw",
                "capex_annual_addition",
                "capex_annual_addition_rolling",
                "capex_annual_per_kw",
                "capex_annual_per_mw",
                "capex_annual_per_mw_rolling",
                "capex_annual_per_mwh",
                "capex_annual_per_mwh_rolling",
                "capex_equipment",
                "capex_land",
                "capex_per_mw",
                "capex_structures",
                "capex_total",
                "capex_wo_retirement_total",
                "construction_type",
                "construction_year",
                "installation_year",
                "net_generation_mwh",
                "not_water_limited_capacity_mw",
                "opex_allowances",
                "opex_boiler",
                "opex_coolants",
                "opex_electric",
                "opex_engineering",
                "opex_fuel",
                "opex_fuel_per_mwh",
                "opex_misc_power",
                "opex_misc_steam",
                "opex_nonfuel_per_mwh",
                "opex_operations",
                "opex_per_mwh",
                "opex_plants",
                "opex_production_total",
                "opex_rents",
                "opex_steam",
                "opex_steam_other",
                "opex_structures",
                "opex_total_nonfuel",
                "opex_transfer",
                "peak_demand_mw",
                "plant_capability_mw",
                "plant_hours_connected_while_generating",
                "plant_type",
                "record_id",
                "water_limited_capacity_mw",
            ],
            "primary_key": [
                "record_id",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_small_plants_sched410": {
        "description": TABLE_DESCRIPTIONS["yearly_small_plants_sched410"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
                "record_id",
                "capacity_mw",
                "capex_per_mw",
                "capex_total",
                "construction_year",
                "fuel_cost_per_mmbtu",
                "fuel_type",
                "license_id_ferc1",
                "net_generation_mwh",
                "opex_fuel",
                "opex_maintenance",
                "opex_operations",
                "opex_total",
                "opex_total_nonfuel",
                "peak_demand_mw",
                "plant_type",
            ],
            "primary_key": [
                "record_id",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_hydroelectric_plants_sched406": {
        "description": TABLE_DESCRIPTIONS["yearly_hydroelectric_plants_sched406"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_name_ferc1",
                "record_id",
                "asset_retirement_cost",
                "avg_num_employees",
                "capacity_factor",
                "capacity_mw",
                "capex_equipment",
                "capex_facilities",
                "capex_land",
                "capex_per_mw",
                "capex_roads",
                "capex_structures",
                "capex_total",
                "construction_type",
                "construction_year",
                "installation_year",
                "net_capacity_adverse_conditions_mw",
                "net_capacity_favorable_conditions_mw",
                "net_generation_mwh",
                "opex_dams",
                "opex_electric",
                "opex_engineering",
                "opex_generation_misc",
                "opex_hydraulic",
                "opex_misc_plant",
                "opex_operations",
                "opex_per_mwh",
                "opex_plant",
                "opex_rents",
                "opex_structures",
                "opex_total",
                "opex_total_nonfuel",
                "opex_water_for_power",
                "peak_demand_mw",
                "plant_hours_connected_while_generating",
                "plant_id_pudl",
                "plant_type",
                "project_num",
            ],
            "primary_key": [
                "record_id",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_pumped_storage_plants_sched408": {
        "description": TABLE_DESCRIPTIONS["yearly_pumped_storage_plants_sched408"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_name_ferc1",
                "record_id",
                "asset_retirement_cost",
                "avg_num_employees",
                "capacity_factor",
                "capacity_mw",
                "capex_equipment_electric",
                "capex_equipment_misc",
                "capex_facilities",
                "capex_land",
                "capex_per_mw",
                "capex_roads",
                "capex_structures",
                "capex_total",
                "capex_wheels_turbines_generators",
                "construction_type",
                "construction_year",
                "energy_used_for_pumping_mwh",
                "installation_year",
                "net_generation_mwh",
                "net_load_mwh",
                "opex_dams",
                "opex_electric",
                "opex_engineering",
                "opex_generation_misc",
                "opex_misc_plant",
                "opex_operations",
                "opex_per_mwh",
                "opex_plant",
                "opex_production_before_pumping",
                "opex_pumped_storage",
                "opex_pumping",
                "opex_rents",
                "opex_structures",
                "opex_total",
                "opex_total_nonfuel",
                "opex_water_for_power",
                "peak_demand_mw",
                "plant_capability_mw",
                "plant_hours_connected_while_generating",
                "plant_id_pudl",
                "project_num",
            ],
            "primary_key": [
                "record_id",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_steam_plants_fuel_sched402": {
        "description": TABLE_DESCRIPTIONS["yearly_steam_plants_fuel_sched402"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
                "fuel_consumed_mmbtu",
                "fuel_consumed_total_cost",
                "fuel_consumed_units",
                "fuel_cost_per_mmbtu",
                "fuel_cost_per_unit_burned",
                "fuel_cost_per_unit_delivered",
                "fuel_mmbtu_per_unit",
                "fuel_type_code_pudl",
                "fuel_units",
                "record_id",
            ],
            "primary_key": [
                "record_id",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_purchased_power_and_exchanges_sched326": {
        "description": TABLE_DESCRIPTIONS[
            "yearly_purchased_power_and_exchanges_sched326"
        ],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "seller_name",
                "record_id",
                "billing_demand_mw",
                "coincident_peak_demand_mw",
                "delivered_mwh",
                "demand_charges",
                "energy_charges",
                "non_coincident_peak_demand_mw",
                "other_charges",
                "purchase_type_code",
                "purchased_mwh",
                "purchased_storage_mwh",
                "purchased_other_than_storage_mwh",
                "received_mwh",
                "tariff",
                "total_settlement",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_plant_in_service_sched204": {
        "description": TABLE_DESCRIPTIONS["yearly_plant_in_service_sched204"],
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "utility_type",
                "plant_status",
                "record_id",
                "additions",
                "adjustments",
                "ending_balance",
                "ferc_account",
                "ferc_account_label",
                "retirements",
                "row_type_xbrl",
                "starting_balance",
                "transfers",
            ],
            "primary_key": ["utility_id_ferc1", "report_year", "ferc_account_label"],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_all_plants": {
        "description": {
            "additional_summary_text": "plants reported in the steam, small generators, hydro, and pumped storage tables.",
            "additional_source_text": "(Schedules 402, 404, 406 and 408)",
            "usage_warnings": [
                {
                    "type": "custom",
                    "description": "Not all columns are originally reported in all of the input plant tables. Expect nulls.",
                },
                PLANT_AGGREGATION_HAZARD,
            ],
            "additional_details_text": (
                "This table is a concatenation of the following plant tables:\n\n"
                "* `core_ferc1__yearly_steam_plants_sched402`\n"
                "* `core_ferc1__yearly_hydroelectric_plants_sched406`\n"
                "* `core_ferc1__yearly_small_plants_sched410`\n"
                "* `core_ferc1__yearly_pumped_storage_plants_sched408`"
            ),
        },
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_id_ferc1",
                "plant_name_ferc1",
                "asset_retirement_cost",
                "avg_num_employees",
                "capacity_factor",
                "capacity_mw",
                "capex_annual_addition",
                "capex_annual_addition_rolling",
                "capex_annual_per_kw",
                "capex_annual_per_mw",
                "capex_annual_per_mw_rolling",
                "capex_annual_per_mwh",
                "capex_annual_per_mwh_rolling",
                "capex_equipment",
                "capex_land",
                "capex_per_mw",
                "capex_structures",
                "capex_total",
                "capex_wo_retirement_total",
                "construction_type",
                "construction_year",
                "installation_year",
                "net_generation_mwh",
                "not_water_limited_capacity_mw",
                "opex_allowances",
                "opex_boiler",
                "opex_coolants",
                "opex_electric",
                "opex_engineering",
                "opex_fuel",
                "fuel_cost_per_mwh",
                "opex_misc_power",
                "opex_misc_steam",
                "opex_nonfuel_per_mwh",
                "opex_operations",
                "opex_per_mwh",
                "opex_plant",
                "opex_production_total",
                "opex_rents",
                "opex_steam",
                "opex_steam_other",
                "opex_structures",
                "opex_total_nonfuel",
                "opex_transfer",
                "peak_demand_mw",
                "plant_capability_mw",
                "plant_hours_connected_while_generating",
                "plant_type",
                "record_id",
                "water_limited_capacity_mw",
                "fuel_cost_per_mmbtu",
                "fuel_type",
                "license_id_ferc1",
                "opex_maintenance",
                "opex_total",
                "capex_facilities",
                "capex_roads",
                "net_capacity_adverse_conditions_mw",
                "net_capacity_favorable_conditions_mw",
                "opex_dams",
                "opex_generation_misc",
                "opex_hydraulic",
                "opex_misc_plant",
                "opex_water_for_power",
                "ferc_license_id",
                "capex_equipment_electric",
                "capex_equipment_misc",
                "capex_wheels_turbines_generators",
                "energy_used_for_pumping_mwh",
                "net_load_mwh",
                "opex_production_before_pumping",
                "opex_pumped_storage",
                "opex_pumping",
            ],
            "primary_key": ["record_id"],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_steam_plants_fuel_by_plant_sched402": {
        "description": {
            "additional_summary_text": "FERC fuel data by plant.",
            "additional_source_text": "(Schedule 402)",
            "usage_warnings": [
                "aggregation_hazard",
            ],
        },
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "plant_id_pudl",
                "plant_name_ferc1",
                "coal_fraction_cost",
                "coal_fraction_mmbtu",
                "fuel_cost",
                "fuel_mmbtu",
                "gas_fraction_cost",
                "gas_fraction_mmbtu",
                "nuclear_fraction_cost",
                "nuclear_fraction_mmbtu",
                "oil_fraction_cost",
                "oil_fraction_mmbtu",
                "primary_fuel_by_cost",
                "primary_fuel_by_mmbtu",
                "waste_fraction_cost",
                "waste_fraction_mmbtu",
            ],
            "primary_key": ["report_year", "utility_id_ferc1", "plant_name_ferc1"],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_detailed_income_statements": {
        "description": {
            "additional_summary_text": "granular, de-duplicated accounting data of utilities' income statements.",
            "additional_source_text": "(Schedules 114, 300, 320 and 336)",
            "usage_warnings": [DETAILED_ACCOUNTING_TABLES_WARNING],
            "additional_details_text": (
                "This table is derived from four FERC Form 1 accounting tables with nested calculations:\n\n"
                "* :ref:`core_ferc1__yearly_income_statements_sched114`\n"
                "* :ref:`core_ferc1__yearly_depreciation_summary_sched336`\n"
                "* :ref:`core_ferc1__yearly_operating_expenses_sched320`\n"
                "* :ref:`core_ferc1__yearly_operating_revenues_sched300`\n\n"
                "We reconciled the nested calculations within these tables and then identified the "
                "most granular data across the tables.\n"
                "We applied slight modifications to two columns (utility_type & plant_function) "
                "as compared to the originally reported values in our core tables. "
                "The modifications were applied to either provide more specificity (i.e. we converted "
                "some `total` utility_type's into `electric`) or to condense similar categories "
                "for easier analysis (i.e. creating a `hydraulic_production` plant_function by "
                "combining `hydraulic_production_conventional` and `hydraulic_production_pumped_storage`).\n"
                "See ``pudl.output.ferc1.Exploder`` for more details. This table was made entirely with "
                "support and direction from RMI."
            ),
        },
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "xbrl_factoid",
                "utility_type",
                "plant_function",
                "revenue_requirement_technology",
                "dollar_value",
                "in_revenue_requirement",
                "revenue_requirement_category",
                "table_name",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_detailed_balance_sheet_assets": {
        "description": {
            "additional_summary_text": "granular, de-duplicated accounting data of utilities' balance sheet assets.",
            "additional_source_text": "(Schedules 110, 200, 204 and 219)",
            "usage_warnings": [DETAILED_ACCOUNTING_TABLES_WARNING],
            "additional_details_text": (
                "This table is derived from four FERC Form 1 accounting tables with nested calculations:\n\n"
                "* `core_ferc1__yearly_balance_sheet_assets_sched110`\n"
                "* `core_ferc1__yearly_utility_plant_summary_sched200`\n"
                "* `core_ferc1__yearly_plant_in_service_sched204`\n"
                "* `core_ferc1__yearly_depreciation_by_function_sched219`\n\n"
                "We reconciled the nested calculations within these tables and then identified the "
                "most granular data across the tables.\n"
                "We applied slight modifications to three columns (utility_type, plant_function & plant_status) "
                "as compared to the originally reported values in our core tables. "
                "The modifications were applied to either provide more specificity (i.e. we converted "
                "some `total` utility_type's into `electric`) or to condense similar categories "
                "for easier analysis (i.e. creating a `hydraulic_production` plant_function by "
                "combining `hydraulic_production_conventional` and `hydraulic_production_pumped_storage`).\n"
                "See ``pudl.output.ferc1.Exploder`` for more details. This table was made entirely with "
                "support and direction from RMI."
            ),
        },
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "xbrl_factoid",
                "utility_type",
                "plant_function",
                "plant_status",
                "ending_balance",
                "utility_type_other",
                "in_rate_base",
                "rate_base_category",
                "table_name",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_detailed_balance_sheet_liabilities": {
        "description": {
            "additional_summary_text": "granular, de-duplicated accounting data of utilities' balance sheet liabilities.",
            "additional_source_text": "(Schedule 110 and 118)",
            "usage_warnings": [DETAILED_ACCOUNTING_TABLES_WARNING],
            "additional_details_text": (
                "This table is derived from two FERC Form 1 accounting tables with nested calculations:\n\n"
                "* `core_ferc1__yearly_balance_sheet_liabilities_sched110`\n"
                "* `core_ferc1__yearly_retained_earnings_sched118`\n\n"
                "We reconciled the nested calculations within these tables and then identified the "
                "most granular data across the tables.\n"
                "We applied slight modifications to three columns (utility_type, plant_function & plant_status) "
                "as compared to the originally reported values in our core tables. "
                "The modifications were applied to either provide more specificity (i.e. we converted "
                "some `total` utility_type's into `electric`) or to condense similar categories "
                "for easier analysis (i.e. creating a `hydraulic_production` plant_function by "
                "combining `hydraulic_production_conventional` and `hydraulic_production_pumped_storage`).\n"
                "See ``pudl.output.ferc1.Exploder`` for more details. This table was made entirely with "
                "support and direction from RMI."
            ),
        },
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "xbrl_factoid",
                "utility_type",
                "ending_balance",
                "in_rate_base",
                "rate_base_category",
                "table_name",
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "out_ferc1__yearly_rate_base": {
        "description": {
            "additional_summary_text": "granular accounting data consisting of what utilities can typically include in their rate bases.",
            "additional_source_text": "(Schedules 110, 118, 200, 204 219 and 320)",
            "usage_warnings": [DETAILED_ACCOUNTING_TABLES_WARNING],
            "additional_details_text": (
                "This table is derived from seven FERC Form 1 "
                "accounting tables with nested calculations. "
                "We reconciled these nested calculations and then identified the most "
                "granular data across the tables.\n"
                "Here are the three direct upstream inputs - the two "
                "``detailed`` tables have several ``core_ferc1`` inputs each:\n\n"
                "* :ref:`out_ferc1__yearly_detailed_balance_sheet_assets`\n"
                "* :ref:`out_ferc1__yearly_detailed_balance_sheet_liabilities`\n"
                "* :ref:`core_ferc1__yearly_operating_expenses_sched320`\n\n"
                "We applied slight modifications to three columns (utility_type, plant_function & "
                "plant_status) as compared to the originally reported values in our core tables. "
                "The modifications were applied to either provide more specificity (i.e. we converted "
                "some `total` utility_type's into `electric`) or to condense similar categories "
                "for easier analysis (i.e. creating a `hydraulic_production` plant_function by "
                "combining `hydraulic_production_conventional` and `hydraulic_production_pumped_storage`).\n"
                "See ``pudl.output.ferc1.Exploder`` for more details. This table was made entirely with "
                "support and direction from RMI."
            ),
        },
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
                "utility_id_ferc1_dbf",
                "utility_id_ferc1_xbrl",
                "utility_id_pudl",
                "utility_name_ferc1",
                "utility_type",
                "plant_function",
                "plant_status",
                "xbrl_factoid",
                "ending_balance",
                "utility_type_other",
                "rate_base_category",
                "ferc_account",
                "row_type_xbrl",
                "record_id",
                "is_disaggregated_utility_type",
                "is_disaggregated_in_rate_base",
                "table_name",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
}
"""FERC Form 1 resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
