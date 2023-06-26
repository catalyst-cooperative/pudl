"""Tables definitions for data coming from the FERC Form 714."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "respondent_id_ferc714": {
        "description": "Respondent identification. FERC Form 714, Part I, Schedule 1.",
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_name_ferc714",
                "eia_code",
            ],
            "primary_key": ["respondent_id_ferc714"],
            "foreign_key_rules": {"fields": [["respondent_id_ferc714"]]},
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
    },
    "demand_hourly_pa_ferc714": {
        "description": (
            "Hourly electricity demand by planning area. FERC Form 714, Part III, "
            "Schedule 2a."
        ),
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "report_date",
                "utc_datetime",
                "timezone",
                "demand_mwh",
            ],
            "primary_key": ["respondent_id_ferc714", "utc_datetime"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
    },
    "fipsified_respondents_ferc714": {
        "description": (
            "Annual respondents with the county FIPS IDs for their service territories."
        ),
        "schema": {
            "fields": [
                "eia_code",
                "respondent_type",
                "respondent_id_ferc714",
                "respondent_name_ferc714",
                "report_date",
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
                "utility_id_eia",
                "utility_name_eia",
                "state",
                "county",
                "state_id_fips",
                "county_id_fips",
            ]
            # No primary key here because the state and county FIPS columns
            # which are part of the natural primary key can be null.
            # The natural primary key would be:
            # ['respondent_id_ferc714', 'report_date', 'state_id_fips', 'county_id_fips']
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "outputs",
    },
    "summarized_demand_ferc714": {
        "description": (
            "Compile FERC 714 annualized, categorized respondents and summarize values."
        ),
        "schema": {
            "fields": [
                "report_date",
                "respondent_id_ferc714",
                "demand_annual_mwh",
                "population",
                "area_km2",
                "population_density_km2",
                "demand_annual_per_capita_mwh",
                "demand_density_mwh_km2",
                "eia_code",
                "respondent_type",
                "respondent_name_ferc714",
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
                "utility_id_eia",
                "utility_name_eia",
            ],
            "primary_key": ["respondent_id_ferc714", "report_date"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "outputs",
    },
}
"""FERC Form 714 resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
