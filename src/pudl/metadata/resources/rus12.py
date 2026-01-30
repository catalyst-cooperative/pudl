"""Table definitions for the RUS12 tables."""

from typing import Any

RESOURCE_METADATA = {
    "core_rus12__yearly_meeting_and_board": {
        "description": {
            "additional_summary_text": (
                "annual meeting and board information for RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H - Section I)",
            "additional_details_text": "",
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
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "debt_description",
                "debt_balance_end_of_report_year",
                "debt_interest_billed",
                "debt_principal_billed",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
}

DRAFT_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
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
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "prime_mover_id",
                "prime_mover_type",
                "primary_renewable_fuel_type_id",
                "primary_renewable_fuel_type",
                "renewable_fuel_pct",
                "capacity_kw",  # TODO: convert to mw
                "net_generation_mwh",
                "capacity_factor",
                "employees_num",
                "total_opex_dollars_per_mwh",
                "power_cost_dollars_per_mwh",
                "total_investment_thousand_dollars",  # TODO: convert to dollars
                "ownership_pct",
                "rus_funding_thousand_dollars",  # TODO: convert to dollars
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
    "core_rus12__yearly_statement_of_operations": {  # Need to decide how to split this up
        "description": {
            "additional_summary_text": (
                "opex and cost of electric service for RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A - Section A)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "operations_type"  # enum (list below)
                "ytd_amount",
                "ytd_budget",
                "report_month_amount",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "operations_type",
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_plant_labor": {
        "description": {
            "additional_summary_text": ("plant-based labor report for RUS borrowers."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Parts D, E, F, G - Section B)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "plant_type",
                "employees_fte_num",
                "employees_part_time_num",
                "operating_plant_payroll",
                "other_accounts_plant_payroll",
                "total_plant_payroll",  # remove?
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "plant_name_rus",
                "plant_type",  # this should be the primary key but there are duplicates for borrower_oid IA0084 and plant Walter Scott
            ],
        },
        "sources": ["rus12"],
        "etl_group": "rus12",
        "field_namespace": "rus",
    },
    "core_rus12__yearly_lines_and_stations_labor_materials": {
        "description": {
            "additional_summary_text": (
                "labor and material cost for lines and stations operated by RUS borrowers."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part I - Section C)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "employees_num",  # might want to separate this out.
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
    "core_rus12__yearly_loans": {
        "description": {
            "additional_summary_text": ("loans guaranteed by RUS borrowers."),
            "additional_primary_key_text": (
                "This table has no primary key because some borrowers report multiple loan values from "
                "the same entity in a given year."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H - Section F - Subsection II)",
            "additional_details_text": "",
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
    "core_rus12__yearly_sources_and_distribution": {
        "description": {
            "additional_summary_text": (
                "MWh and cost of energy sources and distribution by RUS borrowers."
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
                "source_of_energy",
                "cost",
                "mwh",
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
                "capacity_mw",  # still need to update this in the function
                "plant_num",
                "cost",
                "mwh",
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
}


operations_type_enum = [
    "electric_energy_revenues",
    "electric_energy_revenues_report_month",
    "income_from_leased_property",
    "other_operating_revenue_and_income",
    "total_operation_revenues_and_patronage_capital",
    "operating_expense_production_excluding_fuel",
    "operating_expense_production_fuel",
    "operating_expense_other_power_supply",
    "operating_expense_transmission",
    "operating_expense_rto_iso",
    "operating_expense_distribution",
    "operating_expense_customer_accounts",
    "operating_expense_customer_service_and_information",
    "operating_expense_sales",
    "operating_expense_administrative_and_general",
    "total_operation_expense",
    "maintenance_expense_production",
    "maintenance_expense_transmission",
    "maintenance_expense_rto_iso",
    "maintenance_expense_distribution",
    "maintenance_expense_general_plant",
    "total_maintenance_expense",
    "depreciation_and_amortization_expense",
    "taxes",
    "interest_on_long_term_debt",
    "interest_charged_to_construction_credit",
    "other_interest_expense",
    "asset_retirement_obligations",
    "other_deductions",
    "total_cost_of_electric_service",
    "operating_margins",
    "interest_income",
    "allowance_for_funds_used_during_construction",
    "allowance_for_funds_used_during_construction_report_month",
    "income_or_loss_from_equity_investments",  # need to fix this in the extraction PR -- was lossfrom
    "other_non_operating_income",
    "generation_and_transmission_capital_credits",
    "generation_and_transmission_capital_credits_",
    "other_capital_credits_and_patronage_dividends",
    "extraordinary_items",
]
