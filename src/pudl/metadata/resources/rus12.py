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
    "core_rus12__yearly_investments": {
        "description": {
            "additional_summary_text": ("investments, loan guarantees and loans."),
            "additional_source_text": "(Part H - Section F, Sub-section I)",
            "additional_primary_key_text": (
                "This is a list of all investments or loans in each year and borrowers can have "
                "multiple records with the same ``investment_description``."
            ),
            "additional_details_text": (
                "Reporting of investments is required by 7 CFR 1717, Subpart N. Investment "
                "categories reported on this Part correspond to Balance Sheet items in Part C."
            ),
            "usage_warnings": ["experimental_wip"],
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "investment_description",
                "investment_type_code",
                "included_investments",
                "excluded_investments",
                "income_or_loss",
                "for_rural_development",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
}
