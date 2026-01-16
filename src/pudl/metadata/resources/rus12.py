"""Table definitions for the RUS12 tables."""

from typing import Any

RESOURCE_METADATA = {}

DRAFT_RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_rus12__yearly_meeting_and_board": {},
    "core_rus12__yearly_balance_sheet_assets": {},
    "core_rus12__yearly_balance_sheet_liabilities": {},
    "core_rus12__scd_borrowers": {
        "schema": {
            "fields": [
                "report_date",
                "borrower_id_rus",
                "borrower_name_rus",
            ],
        },
    },
    "core_rus12__yearly_renewable_plants": {
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
            ]
        }
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
