"""Table definitions for the RUS7 tables."""

from typing import Any

from pudl.metadata.codes import CODE_METADATA
from pudl.metadata.resource_helpers import HARVESTING_DETAIL_TEXT_RUS

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
                "This table has no native primary key. It is a list of all investments or loan "
                "in each year and borrowers can have multiple records with the same ``investment_description``."
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
                "exclude": [
                    # We must remove all of the rus12 tables - otherwise
                    # these would get a FK relationship from this rus7 table
                    "core_rus12__yearly_meeting_and_board",
                    "core_rus12__yearly_balance_sheet_assets",
                    "core_rus12__yearly_balance_sheet_liabilities",
                    "core_rus12__yearly_long_term_debt",
                    "core_rus12__entity_borrowers",
                    "core_rus12__yearly_renewable_plants",
                    "core_rus12__yearly_lines_stations_labor_materials_cost",
                    "core_rus12__yearly_sources_and_distribution_by_plant_type",
                    "core_rus12__yearly_sources_and_distribution",
                    "core_rus12__yearly_loans",
                    "core_rus12__yearly_plant_labor",
                    "core_rus12__yearly_statement_of_operations",
                    "core_rus12__yearly_plant_costs",
                    "core_rus12__yearly_plant_operations_by_borrower",
                    "core_rus12__yearly_plant_operations_by_plant",
                ],
            },
        },
        "sources": ["rus7"],
        "etl_group": "rus7",
        "field_namespace": "rus",
    },
    # "core_rus7__yearly_owed_by_customers": {
    #     "description": {
    #         "additional_summary_text": (
    #             "debt owed by customers."
    #         ),
    #         "usage_warnings": ["experimental_wip"],
    #         "additional_source_text": "(Part J)",
    #     },
    #     "schema": {
    #         "fields": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "amount_due_over_60_days",
    #             "amount_written_off_ytd"
    #         ],
    #         "primary_key": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "loan_status",
    #         ],
    #     },
    #     "sources": ["rus7"],
    #     "etl_group": "rus7",
    #     "field_namespace": "rus",
    # },
    # "core_rus7__yearly_customer_energy_efficiency_and_conservation_loans": {
    #     "description": {
    #         "additional_summary_text": (
    #             "the repayment status of loans made by a borrower to customers for investments in energy efficiency and conservation initiatives."
    #         ),
    #         "usage_warnings": ["experimental_wip"],
    #         "additional_source_text": "(Part J)",
    #     },
    #     "schema": {
    #         "fields": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "loan_status", # deliquency or default
    #             "actual_pct",
    #             "anticipated_pct",
    #             "ytd_dollars"
    #         ],
    #         "primary_key": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "loan_status",
    #         ],
    #     },
    #     "sources": ["rus7"],
    #     "etl_group": "rus7",
    #     "field_namespace": "rus",
    # },
    # "core_rus7__yearly_service_interruptions": {
    #     "description": {
    #         "additional_summary_text": (
    #             "service interruptions by cause."
    #         ),
    #         "usage_warnings": ["experimental_wip"],
    #         "additional_source_text": "(Part G)",
    #     },
    #     "schema": {
    #         "fields": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "service_interruption_cause",
    #             "observation_period",
    #             "saidi_minutes",
    #             "is_total",
    #         ],
    #         "primary_key": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "service_interruption_cause",
    #             "observation_period",
    #         ],
    #     },
    #     "sources": ["rus7"],
    #     "etl_group": "rus7",
    #     "field_namespace": "rus",
    # },
    # "core_rus7__yearly_distribution_services": {
    #     "description": {
    #         "additional_summary_text": (
    #             "distribution services."
    #         ),
    #         "usage_warnings": ["experimental_wip"],
    #         "additional_source_text": "(Part B)",
    #     },
    #     "schema": {
    #         "fields": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "service_status",
    #             "services",
    #             "is_total"
    #         ],
    #         "primary_key": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "service_status"
    #         ],
    #     },
    #     "sources": ["rus7"],
    #     "etl_group": "rus7",
    #     "field_namespace": "rus",
    # },
    # "core_rus7__yearly_transmission_and_distribution_mileage": {
    #     "description": {
    #         "additional_summary_text": (
    #             "miles of transmission and distribution infrastructure."
    #         ),
    #         "usage_warnings": ["experimental_wip"],
    #         "additional_source_text": "(Part B)",
    #     },
    #     "schema": {
    #         "fields": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "line_type",
    #             "miles",
    #             "is_total"
    #         ],
    #         "primary_key": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "line_type",
    #         ],
    #     },
    #     "sources": ["rus7"],
    #     "etl_group": "rus7",
    #     "field_namespace": "rus",
    # },
}
