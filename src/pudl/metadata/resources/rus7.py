"""Table definitions for the RUS7 tables."""

from typing import Any

from pudl.metadata.resource_helpers import (
    HARVESTED_CORE_TABLES_RUS7,
    HARVESTED_CORE_TABLES_RUS12,
    HARVESTING_DETAIL_TEXT_RUS,
    core_to_out_harvested_resources,
)

RESOURCE_METADATA_BASE: dict[str, dict[str, Any]] = {
    "core_rus7__yearly_meeting_and_board": {
        "description": {
            "additional_summary_text": (
                "governance information about RUS borrowers' annual "
                "member meetings as well as information about their board."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part M)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "last_annual_meeting_date",
                "members_num",
                "members_present_at_meeting_num",
                "was_quorum_present",
                "members_voting_by_proxy_or_mail_num",
                "board_members_num",
                "fees_and_expenses_for_board_members",
                "does_manager_have_written_contract",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_balance_sheet_assets": {
        "description": {
            "additional_summary_text": (
                "assets and other debts from the balance sheet."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "asset_type",
                "ending_balance",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "asset_type",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_balance_sheet_liabilities": {
        "description": {
            "additional_summary_text": (
                "liabilities and other credits from the balance sheet."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "liability_type",
                "ending_balance",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "liability_type",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_employee_statistics": {
        "description": {
            "additional_summary_text": ("statistics about employment and payroll."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "employees_fte_num",
                "employee_hours_worked_regular_time",
                "employee_hours_worked_over_time",
                "payroll_expensed",
                "payroll_capitalized",
                "payroll_other",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_energy_efficiency": {
        "description": {
            "additional_summary_text": (""),
            "usage_warnings": [
                "experimental_wip",
                {
                    "type": "custom",
                    "description": "The savings_mmbtu likely contains values with incorrect units.",
                },
            ],
            "additional_source_text": "(Part P)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "customer_class",
                "observation_period",
                "customers_num",
                "invested",
                "savings_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "customer_class",
                "observation_period",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_power_requirements_electric_customers": {
        "description": {
            "additional_summary_text": (
                "power requirements - number of customers served by customer type."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part O)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "customer_class",
                "observation_period",
                "customers_num",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "customer_class",
                "observation_period",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_power_requirements_electric_sales": {
        "description": {
            "additional_summary_text": (
                "power requirements - revenue and energy sold by customer type."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part O)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "customer_class",
                "sales_mwh",
                "revenue",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "customer_class",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_power_requirements": {
        "description": {
            "additional_summary_text": (
                "power requirements - revenue and generation summary."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part O)",
            "additional_details_text": (
                "This table includes totals of electric sales and revenue which also appear in "
                ":ref:`core_rus7__yearly_power_requirements_electric_sales` with a "
                "``customer_class`` of ``total``. This table includes all other power requirements - "
                "not broken out by customer class, so we include these electric requirements in this "
                "table as well."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                # we could pull out the revenue/costs into one table and then the kwh into another.
                "electric_sales_revenue",
                "transmission_revenue",
                "other_electric_revenue",
                "purchases_and_generation_cost",
                "electric_sales_mwh",
                "own_use_mwh",
                "purchased_mwh",
                "generated_mwh",
                "interchange_mwh",
                "peak_mw",
                "is_peak_coincident",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_investments": {
        "description": {
            "additional_summary_text": ("investments, loan guarantees and loans."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part P - Section I)",
            "additional_details_text": (
                "Reporting of investments is required by 7 CFR 1717, Subpart N. Investment "
                "categories reported on this Part correspond to Balance Sheet items in Part C."
            ),
            "additional_primary_key_text": (
                "This is a list of all investments or loans in each year and borrowers can have "
                "multiple records with the same ``investment_description``."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "investment_description",
                "investment_type_code",
                "included_investments",
                "excluded_investments",
                "income_or_loss",
                "for_rural_development",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_long_term_debt": {
        "description": {
            "additional_summary_text": (
                "long term debt and debt service requirements."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part N)",
            "additional_primary_key_text": (
                "This table has no native primary key. It is a list of all debts "
                "in each year and borrowers can have multiple records with the same ``investment_description``."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "debt_description",
                "debt_ending_balance",
                "debt_interest",
                "debt_principal",
                "debt_total",
            ]
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_long_term_leases": {
        "description": {
            "additional_summary_text": ("long term leases by property type."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part L)",
            "additional_primary_key_text": (
                "Borrowers may receive multiple leases from ``lending_organizations`` in a given year."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "lending_organization",
                "property_type",
                "rental_cost_ytd",
            ]
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_patronage_capital": {
        "description": {
            "additional_summary_text": ("patronage capital distributed and received."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part I)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "patronage_type",
                "patronage_report_year",
                "patronage_cumulative",
                "is_total",
            ],
            "primary_key": ["report_date", "borrower_id_rus", "patronage_type"],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_statement_of_operations": {
        "description": {
            "additional_summary_text": (
                "opex and cost of electric service for RUS borrowers by time period."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
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
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__entity_borrowers": {
        "description": {
            "additional_summary_text": ("active RUS borrowers"),
            "usage_warnings": ["experimental_wip", "harvested"],
            "additional_details_text": (
                "This table contains canonical values for borrowers are set. It contains "
                "values which are expected to remain fixed over time."
                f"{HARVESTING_DETAIL_TEXT_RUS}.\n\n"
                # note from readme about this table
                "This table contains all of the Active Distribution Borrowers as of each report year "
                "who were eligible to report to RUS Form 7.  If these Borrowers have reported to RUS "
                "they will have records in the enclosed data tables, however a small number of these "
                "Borrowers did not report for various reasons and these Borrowers will not be represented "
                "in any of the other tables."
            ),
        },
        "schema": {
            "fields": [
                "borrower_id_rus",
                "borrower_name_rus",
                "state",
            ],
            "primary_key": [
                "borrower_id_rus",
            ],
            "foreign_key_rules": {
                "fields": [["borrower_id_rus"]],
                # We must remove all of the rus12 tables - otherwise
                # these would get a FK relationship from this rus7 table
                "exclude": ["core_rus12__entity_borrowers"]
                + HARVESTED_CORE_TABLES_RUS12
                + [
                    f"out_{tbl.removeprefix('core_')}"
                    for tbl in HARVESTED_CORE_TABLES_RUS12
                ],
            },
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_loans": {
        "description": {
            "additional_summary_text": ("loans provided by RUS borrowers."),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part Q - Sections II & IV)",
            "additional_primary_key_text": (
                "Borrowers may receive multiple loans from ``lending_organizations`` in a given year."
            ),
            "additional_details_text": (
                "This table also includes loan guarantees where the RUS borrower backs a loan "
                "from another entity and is therefore liable to pay any remaining "
                "balance should the original borrower default. \n\n"
                "In 2006, the loan maturity date for borrower ND0051's loan from ERC - Paulson, David "
                "was reported as 2/8/2820. There is no clear way to determine the correct maturity date "
                "given that 2006 is the first year of data we have and the same loan does not appear in "
                "future years. For this reason we've nulled the date."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "loan_recipient",
                "loan_balance",
                "loan_maturity_date",
                "loan_original_amount",
                "for_rural_development",
                "is_loan_guarantee",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_external_financial_risk_ratio": {
        "description": {
            "additional_summary_text": (
                "ratio of investments and loan guarantee balances to total utility plant assets."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part Q - Section III)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "external_financial_risk_ratio",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_energy_purchased": {
        "description": {
            "additional_summary_text": "energy purchased by RUS borrowers.",
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part K)",
            "additional_primary_key_text": (
                "The primary key would probably be report_date, borrower_id_rus, fuel_type_code, "
                "supplier_code_rus, renewable_energy_program if not for certain EIA utilities "
                "represented as *Miscellaneous (supplier code 700000)."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "purchased_mwh",
                "purchased_energy_cost_total",
                "average_energy_cost_cents_per_mwh",
                "wheeling_and_other_charges",
                "fuel_cost_adjustment",
                # "fuel_type" --> add in out table??
                "fuel_type_code_rus",
                "is_supplier_eia_respondent",
                "supplier_code_rus",
                "utility_name_eia",
                "comments",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__yearly_materials_and_supplies": {
        "description": {
            "additional_summary_text": (
                "cost of electric vs. other materials that were purchased, salvaged, "
                "used, or sold."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part F)",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "electric_or_other_materials",
                "transaction_type",
                "amount",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "electric_or_other_materials",
                "transaction_type",
            ],
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
}


RESOURCE_METADATA = RESOURCE_METADATA_BASE | core_to_out_harvested_resources(
    HARVESTED_CORE_TABLES_RUS7,
    RESOURCE_METADATA_BASE,
    ["borrower_name_rus", "state"],
)
