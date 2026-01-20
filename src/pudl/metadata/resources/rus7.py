"""Table definitions for the RUS7 tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_rus7__yearly_meeting_and_board": {
        "description": {
            "additional_summary_text": (
                "borrower's governance - member meeting and board."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part M)",
            "additional_details_text": (
                "This table contains governance information about RUS borrowers annual "
                "member meetings as well as information about their board."
            ),
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
            "additional_summary_text": (""),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "asset_type",  # enum (list below)
                "balance",  # $s
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
}

DRAFT_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_rus7__yearly_balance_sheet_liabilities": {
        "description": {
            "additional_summary_text": (""),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part C)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "liability_type",  # enum (list below)
                "balance",  # $s
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
    "core_rus7__yearly_employee_statistics": {
        "description": {
            "additional_summary_text": (""),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part H)",
            "additional_details_text": "",
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
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part P)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "customer_classification",  # enum w/ things like residential_excluding_seasonal, residential_seasonal	irrigation etc.
                "date_range",  # ?idk about that name enum w/ new_in_report_year, cumulative
                "customers_num",
                "invested",
                "estimated_savings_mmbtu",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "customer_classification",
                "date_range",
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
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "customer_classification",  # enum w/ things like residential_excluding_seasonal, residential_seasonal	irrigation etc.
                "date_range",  # ?idk about that name enum w/ new_in_report_year, cumulative
                "customers_num",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "customer_classification",
                "date_range",
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
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "customer_classification",
                "sales_kwh",
                "revenue",
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
    "core_rus7__yearly_power_requirements": {
        "description": {
            "additional_summary_text": (
                "power requirements - revenue and generation summary."
            ),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part O)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                # we could pull out the revenue/costs into one table and then the kwh into another.
                "electric_sales_revenue",
                "electric_sales_kwh",
                "transmission_revenue",
                "other_electric_revenue",
                "own_use_kwh",
                "purchased_kwh",
                "generated_kwh",
                "purchases_and_generation_cost",
                "interchange_kwh",
                "peak_kwh",
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
    # "core_rus7__yearly_": {
    #     "description": {
    #         "additional_summary_text": (""),
    #         "usage_warnings": ["experimental_wip"],
    #         "additional_source_text": "(Part )",
    #         "additional_details_text": "",
    #     },
    #     "schema": {
    #         "fields": [
    #             "report_date",
    #             "borrower_id_rus",
    #             "borrower_name_rus",
    #         ],
    #         "primary_key": [
    #             "report_date",
    #             "borrower_id_rus",
    #         ],
    #     },
    #     "sources": ["rus7"],
    #     "etl_group": "rus7",
    #     "field_namespace": "rus",
    # },
}

asset_type_enum = [
    "utility_plant_in_service",
    "construction_work_in_progress",
    "total_utility_plant",
    "depreciation_and_amortization",
    "total_utility_plant",
    "non_utility_property",
    "investments_subsidiary_companies",
    "investments_associated_orgs_patronage_capital",
    "investments_associated_orgs_other_general_funds",
    "investments_associated_orgs_other_non_general_funds",
    "investments_economic_development",
    "investments_other",
    "special_funds",
    "total_other_property_and_investments",
    "cash_general_funds",
    "cash_construction_funds_trustee",
    "special_deposits",
    "investments_temporary",
    "notes_receivable",
    "accounts_receivable_sales_of_energy",
    "accounts_receivable_other",
    "renewable_energy_credits",
    "materials_and_supplies",
    "prepayments",
    "other_current_and_accrued",
    "total_current_and_accrued",
    "regulatory",
    "other_deferred_debits",
    "total",
]
liability_type_enum = [
    "memberships",
    "patronage_capital",
    "operating_margins_prior_years",
    "operating_margins_current_year",
    "non_operating_margins",
    "other_margins_and_equities",
    "total_margins_and_equities",
    "long_term_debt_rus",
    "long_term_debt_ffb_rus_guaranteed",
    "long_term_debt_other_rus_guaranteed",
    "long_term_debt_other",
    "long_term_debt_rus_economic_development",
    "payments_unapplied",
    "total_long_term_debt",
    "noncurrent_obligations_under_capital_leases",
    "noncurrent_obligations_asset_retirement",
    "total_noncurrent_obligations",
    "notes_payable",
    "accounts_payable",
    "consumer_deposits",
    "current_maturities_long_term_debt",
    "economic_development",
    "current_maturities_capital_leases",
    "other_current_and_accrued_liabilities",
    "total_current_and_accrued_liabilities",
    "regulatory",
    "other_deferred_credits",
    "total_liabilities_and_other_credits",
]
