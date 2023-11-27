"""Table definitions for the FERC Form 1 data group."""
from typing import Any

from pudl.metadata.codes import CODE_METADATA

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "balance_sheet_assets_ferc1": {
        "description": "Comparative Balance Sheet (Assets and Other Debits). Schedule 110.",
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
    "balance_sheet_liabilities_ferc1": {
        "description": "Comparative balance sheet (liabilities and other credits)",
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
    "cash_flow_ferc1": {
        "description": "The structured portion of the FERC1 cash flow table - Schedule 120.",
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
    "depreciation_amortization_summary_ferc1": {
        "description": (
            "Depreciation and Amortization of Electric Plant (Account 403, 404, 405) "
            "Section A: Summary of depreciation and amortization changes. "
            "Schedule 336a of FERC Form 1."
        ),
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
    "electric_energy_sources_ferc1": {
        "description": (
            "Electric Energy Account, sources only. Schedule 401a. Amount of "
            "electricity the utility obtained from each of several sources, by year."
        ),
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
    "electric_energy_dispositions_ferc1": {
        "description": (
            "Electric Energy Account, dispositions only. Schedule 401a. Electricity "
            "utilities delived to end users, internal losses, etc."
        ),
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
    "electric_operating_expenses_ferc1": {
        "description": (
            "Operating and maintenance costs associated with producing electricty, "
            "reported in Schedule 320 of FERC Form 1."
        ),
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
    "electric_plant_depreciation_changes_ferc1": {
        "description": (
            "Accumulated provision for depreciation of electric utility plant "
            "(Account 108). Schedule 219 Section A: balances and changes during year."
        ),
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
    "electric_plant_depreciation_functional_ferc1": {
        "description": (
            "Accumulated provision for depreciation of electric utility plant "
            "(Account 108). Schedule 219 Section B: Functional plant classifications."
        ),
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
                "plant_function",
                "plant_status",
                "utility_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1",
        "field_namespace": "ferc1",
    },
    "electricity_sales_by_rate_schedule_ferc1": {
        "description": (
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
totals, the utility totals accross all rate schedules in a given year
(marked with rate_schedule_description = "total" and rate_schedule_type =
"total") and each of the utility's individual rate schedule totals in a
given year (marked with rate_schedule_description = "total" and
rate_schdedule_type = "residential" or any other rate schdedule type).

The rate schedule based XBRL tables only report billed values whereas the
total tables report billed, unbilled, and total values. (See the column
description for more info on the difference between billed and unbilled).
This is important to consider if you're endeavoring to compare the subtotal
values with the total values. We have not attempted to fix or verify any
subtotals or totals that don't add up.

Another important note is the possability of unit discrepancies in certain
columns. The revenue_per_kwh column does not specify reporting units, and
closer inspection of the data reveals two clear peaks approximate two orders
of magnitude appart. This indicates that values may be reported in both
dollars and cents. However, because the price of energy per kwh varies
so much regionally, we cannot guarantee which is which and have not put
any cleaning mechanisms in place to account for this."""
        ),
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
    "ferc_accounts": {
        "description": "Account numbers from the FERC Uniform System of Accounts for Electric Plant, which is defined in Code of Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. (See e.g. https://www.law.cornell.edu/cfr/text/18/part-101).",
        "schema": {
            "fields": ["ferc_account_id", "ferc_account_description"],
            "primary_key": ["ferc_account_id"],
        },
        "sources": ["ferc1"],
        "etl_group": "static_ferc1",
        "field_namespace": "ferc1",
    },
    "fuel_ferc1": {
        "description": "Annual fuel cost and quantity for steam plants with a capacity of 25+ MW, internal combustion and gas-turbine plants of 10+ MW, and all nuclear plants. As reported on page 402 of FERC Form 1 and extracted from the f1_fuel table in FERC's FoxPro Database.",
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
    "income_statement_ferc1": {
        "description": "Statement of Income. Schedule 114.",
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
    "other_regulatory_liabilities_ferc1": {
        "description": "Other regulatory liabilities, including rate order docket number.",
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
    "plant_in_service_ferc1": {
        "description": (
            "Balances and changes to FERC Electric Plant in Service accounts, as "
            "reported on FERC Form 1, Schedule 204. Data originally from the "
            "f1_plant_in_srvce table "
            "in FERC's FoxPro database. Account numbers correspond to the FERC Uniform "
            "System of Accounts for Electric Plant, which is defined in Code of "
            "Federal Regulations (CFR) Title 18, Chapter I, Subchapter C, Part 101. "
            "(See e.g. https://www.law.cornell.edu/cfr/text/18/part-101). Each FERC "
            "respondent reports starting and ending balances for each account "
            "annually. Balances are organization wide, and are not broken down on a "
            "per-plant basis. End of year balance should equal beginning year balance "
            "plus the sum of additions, retirements, adjustments, and transfers."
        ),
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
    "plants_ferc1": {
        "description": "FERC 1 Plants and their associated manually assigned PUDL Plant IDs",
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
    "plants_hydro_ferc1": {
        "description": (
            "Hydroelectric generating plant statistics for large plants. Large plants "
            "have an installed nameplate capacity of more than 10 MW. As reported on "
            "FERC Form 1, Schedule 406 (pages 406-407), and extracted from the "
            "f1_hydro table in FERC's FoxPro database."
        ),
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
    "plants_pumped_storage_ferc1": {
        "description": (
            "Generating plant statistics for hydroelectric pumped storage plants with "
            "an installed nameplate capacity of 10+ MW. As reported in Scheudle 408 of "
            "FERC Form 1 and extracted from the f1_pumped_storage table in FERC's "
            "Visual FoxPro Database."
        ),
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
    "plants_small_ferc1": {
        "description": """The generating plant statistics for internal combustion
plants, gas turbine-plants, conventional hydro plants, and pumped storage plants with
less than 10 MW installed nameplate capacity and steam plants with less than 25 MW
installed nameplate capacity. As reported on FERC Form 1 Schedule 410 (pages 410-411)
and extracted from the FERC Visual FoxPro and XBRL. See our
``pudl.extract.ferc1.TABLE_NAME_MAP_FERC1`` for links to the raw tables.

The raw version of this table is more like a digitized PDF than an actual data table.
The rows contain lots of information in addition to what the columns might suggest.
For instance, a single column may contain header rows, note rows, and total rows. This
extraneous information is useful, but it prevents proper analysis when mixed in with the
rest of the values data in the column. We employ a couple of data transformations to
extract these rows from the data and preserve some of the information they contain
(fuel type, plant type, FERC license, or general notes about the plant) in separate
columns.""",
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
    "plants_steam_ferc1": {
        "description": (
            "Generating plant statistics for steam plants with a capacity of 25+ MW, "
            "internal combustion and gas-turbine plants of 10+ MW, and all nuclear "
            "plants. As reported in Schedule 402 of FERC Form 1 and extracted from the "
            "f1_gnrt_plant table in FERC's Visual FoxPro Database."
        ),
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "report_year",
                "plant_id_ferc1",
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
    "power_purchase_types_ferc1": {
        "description": "Coding table defining different types of electricity power purchases.",
        "schema": {
            "fields": ["code", "label", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["purchase_type_code"]]},
        },
        "encoder": CODE_METADATA["power_purchase_types_ferc1"],
        "sources": ["ferc1"],
        "etl_group": "static_ferc1",
        "field_namespace": "ferc1",
    },
    "purchased_power_ferc1": {
        "description": (
            "Purchased Power (Account 555) including power exchanges (transactions "
            "involving a balancing of debits and credits for energy, capacity, etc.) "
            "and any settlements for imbalanced exchanges. Reported on pages 326-327 "
            "of FERC Form 1. Extracted from the f1_purchased_pwr table in FERC's "
            "Visual FoxPro database."
        ),
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
    "transmission_statistics_ferc1": {
        "description": (
            "Transmission Line Statistics. Schedule 422 of FERC Form 1. Information "
            "describing transmission lines, the cost of lines, annual operating and "
            "capital expenses, etc."
        ),
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
    "utilities_ferc1": {
        "description": "This table maps two manually assigned utility IDs: a PUDL ID and a FERC1 ID. The PUDL ID maps EIA and FERC1 utilities. The FERC1 ID maps the older DBF respondent IDs to new XBRL entity IDs. This table is generated from a table stored in the PUDL repository: src/package_data/glue/utility_id_pudl.csv",
        "schema": {
            "fields": ["utility_id_ferc1", "utility_name_ferc1", "utility_id_pudl"],
            "primary_key": ["utility_id_ferc1"],
            "foreign_key_rules": {"fields": [["utility_id_ferc1"]]},
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "utilities_ferc1_dbf": {
        "description": "This table maps the assign utility ID FERC1 to the native utility ID from the FERC1 DBF inputs - originally reported as respondent_id.",
        "schema": {
            "fields": ["utility_id_ferc1", "utility_id_ferc1_dbf"],
            "primary_key": ["utility_id_ferc1_dbf"],
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "utilities_ferc1_xbrl": {
        "description": "This table maps the assign utility ID FERC1 to the native utility ID from the FERC1 XBRL inputs - originally reported as entity_id.",
        "schema": {
            "fields": ["utility_id_ferc1", "utility_id_ferc1_xbrl"],
            "primary_key": ["utility_id_ferc1_xbrl"],
        },
        "sources": ["ferc1"],
        "etl_group": "glue",
        "field_namespace": "ferc1",
    },
    "utility_plant_summary_ferc1": {
        "description": (
            "Summary of utility plant and accumulated provisions for depreciation, "
            "amortization and depletion of utilty plant assets reported annually at "
            "the end of the report year. Schedule 200 of FERC Form 1."
        ),
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
    "retained_earnings_ferc1": {
        "description": "Retained Earnings - The structed part of schedule 118.",
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
    "retained_earnings_appropriations_ferc1": {
        "description": "Retained Earnings - some of the unstructed part of schedule 118.",
        "schema": {
            "fields": ["utility_id_ferc1", "report_year", "utility_type", "record_id"],
        },
        "sources": ["ferc1"],
        "etl_group": "ferc1_disabled",
        "field_namespace": "ferc1",
        "create_database_schema": False,
    },
    "electric_operating_revenues_ferc1": {
        "description": (
            "Electric operating revenues - The structed part of schedule 300."
            "There are a number of revenue_type's that do not have sales_mwh,"
            "or avg_customers_per_month provided, in which case these columns"
            "will be NULL."
        ),
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
    "denorm_plants_utilities_ferc1": {
        "description": "Denormalized table that contains FERC plant and utility information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "plant_name_ferc1",
                "plant_id_pudl",
                "utility_name_ferc1",
                "utility_id_pudl",
            ],
            "primary_key": ["utility_id_ferc1", "plant_name_ferc1"],
        },
        "field_namespace": "ferc1",
        "etl_group": "outputs",
        "sources": ["ferc1"],
    },
    "denorm_balance_sheet_assets_ferc1": {
        "description": "Denormalized table that contains FERC balance sheet asset information.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_balance_sheet_liabilities_ferc1": {
        "description": "Denormalized table that contains FERC balance sheet liability information.",
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
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
    "denorm_cash_flow_ferc1": {
        "description": "Denormalized table that contains FERC cash flow information.",
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
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
    "denorm_depreciation_amortization_summary_ferc1": {
        "description": "Denormalized table that contains FERC depreciation amortization information.",
        "schema": {
            "fields": [
                "record_id",
                "report_year",
                "utility_id_ferc1",
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
    "denorm_electric_energy_dispositions_ferc1": {
        "description": "Denormalized table that contains FERC electric energy dispositions information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_electric_energy_sources_ferc1": {
        "description": "Denormalized table that contains FERC electric energy sources information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_electric_operating_expenses_ferc1": {
        "description": "Denormalized table that contains FERC electric operating expense information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
                "report_year",
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
    "denorm_electric_operating_revenues_ferc1": {
        "description": "Denormalized table that contains FERC electric operating revenue information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_electric_plant_depreciation_changes_ferc1": {
        "description": "Denormalized table that contains FERC electric plant depreciation changes information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_electric_plant_depreciation_functional_ferc1": {
        "description": (
            "Denormalized accumulated provision for depreciation of electric utility "
            "plant (Account 108). Schedule 219 Section B: Functional plant classifications."
        ),
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
                "plant_function",
                "plant_status",
                "utility_type",
            ],
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_electricity_sales_by_rate_schedule_ferc1": {
        "description": "Denormalized table that contains FERC electricity sales by rate schedule information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
            ]
        },
        "sources": ["ferc1"],
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_income_statement_ferc1": {
        "description": "Denormalized table that contains FERC income statement information.",
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_other_regulatory_liabilities_ferc1": {
        "description": "Denormalized table that contains FERC other regulatory liabilities information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_retained_earnings_ferc1": {
        "description": "Denormalized table that contains FERC retained earnings information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_transmission_statistics_ferc1": {
        "description": "Denormalized table that contains FERC transmission statistics information.",
        "schema": {
            "fields": [
                "record_id",
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_utility_plant_summary_ferc1": {
        "description": "Denormalized table that contains FERC utility plant summary information.",
        "schema": {
            "fields": [
                "utility_id_ferc1",
                "utility_id_pudl",
                "utility_name_ferc1",
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
        "etl_group": "outputs",
        "field_namespace": "ferc1",
    },
    "denorm_plants_steam_ferc1": {
        "description": "Denormalized table that contains steam plant information from FERC Form 1.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_plants_small_ferc1": {
        "description": "Denormalized table that contains small plant information from FERC Form 1.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_plants_hydro_ferc1": {
        "description": "Denormalized table that contains small plant information from FERC Form 1.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_plants_pumped_storage_ferc1": {
        "description": "Denormalized table that contains pumped storage plant information from FERC Form 1.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_fuel_ferc1": {
        "description": "Denormalized table that contains fuel information from FERC Form 1.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_purchased_power_ferc1": {
        "description": "Denormalized table of FERC Form 1 Purchased Power data.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_plant_in_service_ferc1": {
        "description": "Denormalized table of FERC Form 1 Electric Plant in Service data.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_plants_all_ferc1": {
        "description": "Denormalized table combining the steam, small generators, hydro, and pumped storage tables from FERC Form 1.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
    "denorm_fuel_by_plant_ferc1": {
        "description": "A table summarizing FERC fuel data by plant, using FERC Form 1 data.",
        "schema": {
            "fields": [
                "report_year",
                "utility_id_ferc1",
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
}
"""FERC Form 1 resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
