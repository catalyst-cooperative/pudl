"""Table definitions for the RUS12 tables."""

RESOURCE_METADATA = {
    "core_rus12__yearly_meeting_and_board": {
        "description": {
            "additional_summary_text": (
                "annual meeting and board information for RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H - Section I)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "last_annual_meeting_date",
                "members_num",
                "members_present_at_meeting_num",
                "was_quorum_present",
                "members_voting_by_proxy_or_mail_num",
                "board_members_num",
                "fees_and_expenses_for_board_members",
                "does_manager_have_written_contract",
            ],
            "primary_key": ["report_date", "borrower_id_rus"],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_balance_sheet_assets": {
        "description": {
            "additional_summary_text": (
                "assets and other debts from the balance sheet."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A - Section B)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "asset_type",
                "balance",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "asset_type",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_balance_sheet_liabilities": {
        "description": {
            "additional_summary_text": (
                "liabilities and other credits from the balance sheet."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A - Section B)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "liability_type",
                "balance",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "liability_type",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_long_term_debt": {
        "description": {
            "additional_summary_text": (
                "long-term debt and debt service requirements for RUS borrowers."
            ),
            "additional_primary_key_text": (
                "This table has no primary key because some borrowers report multiple debt values from "
                "the same entity in a given year."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H - Section H)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "debt_description",
                "debt_ending_balance",
                "debt_interest",
                "debt_principal",
                "debt_total",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__scd_borrowers": {
        "description": {
            "additional_summary_text": ("active RUS borrowers."),
            "usage_warnings": ["experimental_wip"],
            "additional_details_text": (
                # note from readme about this table
                "This table contains all of the Active Distribution Borrowers as of each report year "
                "who were eligible to report to RUS Form 12.  If these Borrowers have reported to RUS "
                "they will have records in the enclosed data tables, however a small number of these "
                "Borrowers did not report for various reasons and these Borrowers will not be represented "
                "in any of the other tables."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "state",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
            ],
            # TODO: we could check to see if we could add a FK relationship here
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_renewable_plants": {
        "description": {
            "additional_summary_text": (
                "renewable energy plant generation information for RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C RE)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "prime_mover_id",
                "prime_mover_type",
                "primary_renewable_fuel_type_id",  # could maybe get rid of this?
                "primary_renewable_fuel_type",
                "renewable_fuel_pct",
                "capacity_mw",
                "net_generation_mwh",
                "capacity_factor",
                "employees_num",
                "opex_per_mwh",
                "power_cost_per_mwh",
                "invested",
                "ownership_pct",
                "rus_funding",
                "comments",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "plant_name_rus",
                "prime_mover_id",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_lines_stations_labor_materials_cost": {
        "description": {
            "additional_summary_text": (
                "labor and material cost for lines and stations operated by RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part I - Section C)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "labor_or_material",
                "operation_or_maintenance",
                "lines_or_stations",
                "cost",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "labor_or_material",
                "operation_or_maintenance",
                "lines_or_stations",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_sources_and_distribution_by_plant_type": {
        "description": {
            "additional_summary_text": (
                "capacity, plant num, MWh, and cost of energy by plant type for RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_type",
                "capacity_mw",
                "plant_num",
                "cost",
                "net_energy_received_mwh",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "plant_type",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_sources_and_distribution": {
        "description": {
            "additional_summary_text": (
                "MWh and cost of energy sources and distribution by RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C)",
            "additional_details_text": (
                "See the ``sources_and_distribution_by_plant_type`` table for "
                "a breakdown of plant-type-specific cost, capacity, plant_num, "
                "and net_energy_received values. "
                "Also note that there are several ``source_of_energy`` values "
                "that don't have a corresponding cost value."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "source_of_energy",
                "net_energy_received_mwh",
                "cost",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "source_of_energy",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_loans": {
        "description": {
            "additional_summary_text": ("loans guaranteed by RUS borrowers."),
            "additional_primary_key_text": (
                "This table has no primary key because some borrowers report multiple loan values from "
                "the same entity in a given year."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H - Section F - Subsection II)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "loan_organization",
                "loan_maturity_date",
                "loan_original_amount",
                "loan_balance",
                "for_rural_development",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_plant_labor": {
        "description": {
            "additional_summary_text": (
                "labor and payroll information for plants owned by RUS borrowers."
            ),
            "additional_primary_key_text": (
                "The primary key should be report_date, borrower_id_rus, plant_name_rus, "
                "and plant_type, but this table did not report plant_type before 2009 and "
                "there are respondents who report multiple rows per plant pre-2009. "
                "The data cannot be backfilled because there is no way to distinguish between "
                "duplicate rows pre-2009."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Parts D, E, F, G - Section B)",
            "additional_details_text": (
                "Note the lack of plant_type pre-2009 leading to a lack of "
                "reliable primary keys.\n\n"
                "For plant Walter Scott, there were duplicate rows reported by borrowers IA0083 and IA0084. "
                "We removed the rows from borrower IA0083 to prevent double counting."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "plant_type",
                "employees_full_time_num",
                "employees_part_time_num",
                "employee_hours_worked_total",
                "payroll_maintenance",
                "payroll_operations",
                "payroll_other_accounts",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_statement_of_operations": {
        "description": {
            "additional_summary_text": (
                "opex and cost of electric service for RUS borrowers by time period."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A - Section A)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "opex_group",
                "opex_type",
                "opex_report_month",
                "opex_ytd",
                "opex_ytd_budget",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "opex_group",
                "opex_type",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
}

PLANT_OPERATIONS_DETAIL = (
    "The data in this table comes from five different portions of RUS 12 "
    "corresponding to different plant types (steam, hydroelectric, "
    "combined_cycle, internal_combustion and nuclear). The original form "
    "combines plant operations data for each plant type with records corresponding "
    "to the portion of plants that borrowers own as well as the whole plant. "
    "Records that are wholly owned by one borrower show up in both "
    ":ref:`core_rus12__yearly_plant_operations_by_borrower` and "
    ":ref:`core_rus12__yearly_plant_operations_by_plant`.\n\n"
    "There are two boolean columns used to delineate which records are associated "
    "with the borrowers' share vs the whole plant - which is documented in "
    "``_OR_PowerSupply Plant File Documentation.rtf`` in the newer years in the "
    "RUS 12 archive. One of these two fields - ``is_partly_owned_by_borrower`` - "
    "was not reported before 2009. For the pre-2009 years, we assume that all records "
    "that report TRUE for is_full_ownership_portion should end up in the by-plant table "
    "while all records should end up in the by-borrower portion of the table."
    "Like the post-2009 records, this involves records from the original tables ending "
    "up in both of these PUDL tables."
)

DRAFT_RESOURCE_METADATA = {
    "core_rus12__yearly_plant_costs": {
        "description": {
            "additional_summary_text": ("costs of net energy generated by plant."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part F - Section D)",
            "additional_primary_key_text": (
                "This table has no primary key because there is one plant (named Walter "
                "Scott) that has duplicate records every year. The primary key of this table "
                "otherwise would be: ['report_date', 'borrower_id_rus', 'plant_name_rus', 'cost_group', 'cost_type']."
            ),
            "additional_details_text": (
                "The cost column in this table is expected to be largely non-null, the "
                "cost_per_mwh and cost_per_mmbtu columns only apply to some cost_type's "
                "and even plant_type's and thus are expected to contain many nulls "
                "(64% and 90% respectively)"
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "plant_type",
                "cost_group",  # capex|opex|total
                "cost_type",  # big enum (below)
                "cost",  # $
                "cost_per_mwh",
                "cost_per_mmbtu",
            ]
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_plant_operations_by_borrower": {
        "description": {
            "additional_summary_text": (
                "borrower portion of plant operational data including fuel consumption and operational hours."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part D, E, F & G - Section A)",
            "additional_primary_key_text": (
                "This table has no primary key because there are a handful of plants that "
                "have duplicate records. The primary key of this table "
                "otherwise would be: [`report_date`, `borrower_id_rus`, `plant_name_rus`, `plant_name_rus`, `unit_id_rus`, `plant_type`, `is_full_ownership_portion`, `is_partly_owned_by_borrower`]."
            ),
            "additional_details_text": PLANT_OPERATIONS_DETAIL,
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "unit_id_rus",
                "plant_type",
                "capacity_kw",
                "gross_generation_mwh",
                "borrower_ownership_pct",
                "is_full_ownership_portion",
                "is_partly_owned_by_borrower",  # was not reported till 2009
                "fuel_consumption_coal_lbs",
                "fuel_consumption_gas_cubic_feet",
                "fuel_consumption_oil_gals",
                "fuel_consumption_other",
                "operating_hours_in_service",
                "operating_hours_on_standby",
                "operating_hours_out_of_service_scheduled",
                "operating_hours_out_of_service_unscheduled",
                "times_started",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_plant_operations_by_plant": {
        "description": {
            "additional_summary_text": (
                "whole plant operational data including fuel consumption and operational hours."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part D, E, F (CC), F (IC) & G - Section A)",
            "additional_primary_key_text": (
                "This table has no primary key because there are a handful of plants that "
                "have duplicate records. The primary key of this table "
                "otherwise would be: [`report_date`, `borrower_id_rus`, `plant_name_rus`, `plant_name_rus`, `unit_id_rus`, `plant_type`, `is_full_ownership_portion`, `is_partly_owned_by_borrower`]."
            ),
            "additional_details_text": (
                f"{PLANT_OPERATIONS_DETAIL}.\n\nRUS instructions copied verbatim below include "
                "information about how to link records from this table with records from "
                ":ref:`core_rus12__yearly_plant_labor` and forthcoming "
                "``core_rus12__yearly_plant_factors_and_maximum_demand``. From RUS documentation:"
                "\n\n"
                "This data is for the total plant and does not contain any data for the "
                "Borrower’s Share; this data can be  matched up with the data in the "
                "following two files (where “YYYY” is the data year) that contain data for "
                "Sections B and C for all plants; however you should use caution when using "
                "total plant data since there are cases where more than one Borrower shares "
                "units at the same plant which means that you will be getting duplicate plant "
                "total records (and there is no guarantee that the total plant records entered "
                "by two borrowers for the same plant will be identical):\n\n"
                "OpRpt_PS_YYYY__US_dg_B_OpRpt_PSOperatingReportPlantLabor.csv\n"
                "OpRpt_PS_YYYY__US_dg_C_OpRpt_PSOperatingReportPlantFactorsAndMaxDemand.csv"
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "unit_id_rus",
                "plant_type",
                "capacity_kw",
                "gross_generation_mwh",
                "borrower_ownership_pct",
                "is_partly_owned_by_borrower",  # was not reported till 2009
                "fuel_consumption_coal_lbs",
                "fuel_consumption_gas_cubic_feet",
                "fuel_consumption_oil_gals",
                "fuel_consumption_other",
                "operating_hours_in_service",
                "operating_hours_on_standby",
                "operating_hours_out_of_service_scheduled",
                "operating_hours_out_of_service_unscheduled",
                "times_started",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_": {
        "description": {
            "additional_summary_text": (""),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part )",
        },
        "schema": {
            "fields": ["report_date", "borrower_id_rus", "borrower_name_rus"],
            "primary_key": ["report_date", "borrower_id_rus"],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
}

plant_cost_type_enum = {
    "allowances",
    "coal_fuel",
    "coolants_and_water",
    "depreciation",
    "electric",
    "energy_for_compressed_air",
    "energy_for_pumped_storage",
    "fuel",
    "gas_fuel",
    "generation",
    "hydraulic",
    "interest",
    "less_fuel_acquisition_adjustment",
    "maintenance_boiler_plant",
    "maintenance_electric_plant",
    "maintenance_generating_and_electric_plant",
    "maintenance_miscellaneous_plant",
    "maintenance_other_plant",
    "maintenance_reactor_plant_equipment",
    "maintenance_reservoirs_dams_waterways",
    "maintenance_structures",
    "maintenance_supervision_and_engineering",
    "maintenance_total",
    "miscellaneous_power_generation",
    "net_fuel",
    "non_fuels_subtotal",
    "oil_fuel",
    "operations_total",
    "other_fuel",
    "other_generation",
    "other_nuclear_power",
    "plant_acquisition_adjustment",
    "power",
    "power_cost",
    "reactor_credits",
    "rents",
    "steam",
    "steam_other_sources",
    "steam_power",
    "supervision_and_engineering",
    "total",
    "total_fixed",
    "total_fuel",
    "water_for_power",
}
