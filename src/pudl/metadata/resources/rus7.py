"""Table definitions for the RUS7 tables."""

from typing import Any

from pudl.metadata.codes import CODE_METADATA

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
                "borrower_name_rus",
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
    "core_rus7__codes_investment_types": {
        "description": {
            "additional_summary_text": "investment types.",
        },
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["investment_type_code"]]},
        },
        "encoder": CODE_METADATA["core_rus7__codes_investment_types"],
        "field_namespace": "rus",
        "sources": ["rus7"],
        # I added this as RUS instead of RUS7 so we can compile any RUS code table
        # in one static_assets function
        "etl_group": "static_rus",
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
                "borrower_name_rus",
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
                "Borrowers may receive multiple leases ``lending agencies`` in a given year."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
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
                "borrower_name_rus",
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
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    "core_rus7__scd_borrowers": {  # this is kinda a SCD table? with just two things?
        "description": {
            "additional_summary_text": ("active RUS borrowers"),
            "usage_warnings": ["experimental_wip"],
            "additional_details_text": (
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
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
}
