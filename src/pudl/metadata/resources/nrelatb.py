"""Table definitions for the NREL ATB data ."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {  # }
    "core_nrelatb__yearly_projected_financial_cases": {
        "description": (
            "Financial assumptions for each model case (model_case_nrelatb), "
            "and sub-type of technology (technology_description)."
        ),
        "schema": {
            "fields": [
                "report_year",
                "model_case_nrelatb",
                "projection_year",
                "technology_description",
                "inflation_rate",
                "interest_rate_during_construction_nominal",
                "interest_rate_calculated_real",
                "interest_rate_nominal",
                "rate_of_return_on_equity_calculated_real",
                "rate_of_return_on_equity_nominal",
                "tax_rate_federal_state",
            ],
            "primary_key": [
                "report_year",
                "model_case_nrelatb",
                "projection_year",
                "technology_description",
            ],
        },
        "sources": ["nrelatb"],
        "etl_group": "nrelatb",
        "field_namespace": "nrelatb",
    },
    "core_nrelatb__yearly_projected_financial_cases_by_scenario": {
        "description": (
            "Additional financial assumptions for NREL ATB projections that also vary by "
            "technology innovation scenario (scenario_atb), tax credit case (model_tax_credit_case_nrelatb), "
            "and cost recovery period (cost_recovery_period_years). \nThere are a small number of records which have nulls in"
            "the cost_recovery_period_years column. Based on NREL's documentation, this seems to indicate "
            "that those records apply to any relevant cost_recovery_period_years. If those records were "
            "non-null, the primary keys of this table would be: "
            "['report_year', 'model_case_nrelatb', 'model_tax_credit_case_nrelatb', 'projection_year', 'technology_description', 'scenario_atb', 'cost_recovery_period_years']"
        ),
        "schema": {
            "fields": [
                "report_year",
                "model_case_nrelatb",
                "model_tax_credit_case_nrelatb",
                "projection_year",
                "technology_description",
                "scenario_atb",
                # there are nulls in ~%5 of the records in the field
                # it would be a part of a composite primary_key, but nulls.
                "cost_recovery_period_years",
                "capital_recovery_factor",
                "debt_fraction",
                "fixed_charge_rate",
                "wacc_nominal",
                "wacc_real",
            ],
        },
        "sources": ["nrelatb"],
        "etl_group": "nrelatb",
        "field_namespace": "nrelatb",
    },
    "core_nrelatb__yearly_projected_cost_performance": {
        "description": "Projections of costs and performance for various technologies.",
        "schema": {
            "fields": [
                "report_year",
                "model_case_nrelatb",
                "model_tax_credit_case_nrelatb",
                "projection_year",
                "technology_description",
                "cost_recovery_period_years",
                "scenario_atb",
                # These two detail fields have nulls in them! which is fine for the data but means we don't have non-null PKs
                "technology_description_detail_1",
                "technology_description_detail_2",
                "capacity_factor",
                "capex_per_kw",
                "capex_overnight_per_kw",
                "capex_overnight_additional_per_kw",
                "capex_grid_connection_per_kw",
                "capex_construction_finance_factor",
                "fuel_cost_per_mwh",
                "heat_rate_mmbtu_per_mwh",
                "heat_rate_penalty",
                "levelized_cost_of_energy_per_mwh",
                "net_output_penalty",
                "opex_fixed_per_kw",
                "opex_variable_per_mwh",
            ],
        },
        "sources": ["nrelatb"],
        "etl_group": "nrelatb",
        "field_namespace": "nrelatb",
    },
    "core_nrelatb__yearly_technology_status": {
        "description": (
            "Annual table indicating whether technologies in the ATB scenarios are mature,"
            "and whether they are the default technologies."
        ),
        "schema": {
            "fields": [
                "report_year",
                "technology_description",
                "technology_description_detail_1",
                "technology_description_detail_2",
                "is_technology_mature",
                "is_default",
            ],
        },
        "sources": ["nrelatb"],
        "etl_group": "nrelatb",
        "field_namespace": "nrelatb",
    },
}
