"""Table definitions for the RUS12 tables."""

from typing import Any

RESOURCE_METADATA = {}

DRAFT_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_rus12__yearly_meeting_and_board": {
        "description": {
            "additional_summary_text": (""),
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
    },
    "core_rus12__yearly_balance_sheet_assets": {
        "description": {
            "additional_summary_text": (""),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A - Section B)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "asset_type",  # enum (list below)
                "balance",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "asset_type",
            ],
        },
    },
    "core_rus12__yearly_balance_sheet_liabilities": {
        "description": {
            "additional_summary_text": (""),
            "usage_warnings": ["experimental_wip"],
            "additional_source_text": "(Part A - Section B)",
            "additional_details_text": "",
        },
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "liability_type",  # enum (list below)
                "balance",
                "is_total",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "liability_type",
            ],
        },
    },
    "core_rus12__scd_borrowers": {
        "description": {
            "additional_summary_text": ("active RUS borrowers"),
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
    },
    "core_rus12__yearly_renewable_plants": {
        "description": {
            "additional_summary_text": (""),
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
                "capacity_kw",  # do we convert to mw here?
                "net_generation_mwh",
                "capacity_factor",
                "employees_num",
                "total_opex_dollars_per_mwh",
                "power_cost_dollars_per_mwh",
                "total_investment_thousand_dollars",  # will want to convert this to NOT thousand dollars
                "ownership_pct",
                "rus_funding_thousand_dollars",  # will want to convert this to NOT thousand dollars
                "comments",
            ],
            "primary_key": [
                "report_date",
                "borrower_id_rus",
                "prime_mover_id",
            ],
        },
    },
    "core_rus12__yearly_statement_of_operations": {  # Need to decide how to split this up
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
            ]
        }
    },
    "core_rus12__yearly_plant_labor": {
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
                "plant_name_rus",
                "plant_type",
                "employees_fte_num",
                "employees_part_time_num",
                "payroll_operating_plant",  # seems like we might want to put payroll as a suffix here
                "payroll_other_accounts_plant",  # seems like we might want to put payroll as a suffix here
                "total_plant_payroll",  # remove?
            ]
        }
    },
    "core_rus12__yearly_lines_and_stations_labor_materials": {
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
            ]
        }
    },
}

asset_type_enum = [
    "construction_work_in_progress",
    "total_utility_plant",
    "depreciation_and_amortization",
    "non_utility_property_asset",
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
    "fuel_stock",
    "renewable_energy_credits",
    "materials_and_supplies",
    "prepayments",
    "other_current_and_accrued",
    "total_current_and_accrued",
    "unamortized_debt_discount_property_losses",
    "regulatory",
    "other_deferred_debits",
    "accumulated_deferred_income_taxes_debits",
    "total",
]

liability_type_enum = [
    "memberships",
    "assigned_and_assignable_patronage_capital",
    "retired_this_year_patronage_capital",
    "retired_prior_years_patronage_capital",
    "patronage_capital",
    "operating_margins_prior_years",
    "operating_margins_current_year",
    "non_operating_margins",
    "other_margins_and_equities",
    "total_margins_and_equities",
    "long_term_debt_rus",
    "payments_unapplied",
    "long_term_debt_rus_economic_development",
    "long_term_debt_ffb_rus_guaranteed",
    "long_term_debt_other_rus_guaranteed",
    "long_term_debt_other",
    "total_long_term_debt",
    "noncurrent_obligations_under_capital_leases",
    "accumulated_operating_provisions",
    "total_other_noncurrent_liabilities",
    "notes_payable",
    "accounts_payable",
    "current_maturities_long_term_debt",
    "rural_development",
    "current_maturities_capital_leases",
    "taxes_accrued",
    "interest_accrued",
    "other_current_and_accrued",
    "total_current_and_accrued",
    "deferred_credits",
    "accumulated_deferred_income_taxes_credits",
    "total_liabilities_and_other_credits",
]
