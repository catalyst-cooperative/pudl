"""Tables definitions for data coming from the FERC Form 714."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_ferc714__respondent_id": {
        "description": "Respondent identification. FERC Form 714, Part I, Schedule 1.",
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_id_ferc714_csv",
                "respondent_id_ferc714_xbrl",
                "respondent_name_ferc714",
                "eia_code",
            ],
            "primary_key": ["respondent_id_ferc714"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
    },
    "out_ferc714__hourly_planning_area_demand": {
        "description": (
            "Hourly electricity demand by planning area. FERC Form 714, Part III, "
            "Schedule 2a."
        ),
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_id_ferc714_csv",
                "respondent_id_ferc714_xbrl",
                "report_date",
                "datetime_utc",
                "timezone",
                "demand_mwh",
            ],
            "primary_key": ["respondent_id_ferc714", "datetime_utc"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
        "create_database_schema": False,
    },
    "out_ferc714__respondents_with_fips": {
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
    "out_ferc714__summarized_demand": {
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
    "core_ferc714__yearly_planning_area_demand_forecast": {
        "description": (
            "10-year forecasted summer and winter peak demand and annual net energy per planning area. FERC Form 714, Part III, "
            "Schedule 2b."
        ),
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "report_year",
                "forecast_year",
                "summer_peak_demand_forecast_mw",
                "winter_peak_demand_forecast_mw",
                "net_demand_forecast_mwh",
            ],
            "primary_key": ["respondent_id_ferc714", "report_year", "forecast_year"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
        "create_database_schema": True,
    },
    "core_pudl__assn_ferc714_pudl_respondents": {
        "description": (
            "Home table for PUDL derived FERC 714 respondent IDs. These ID's are used to connect "
            "older CSV data which uses different respondent IDs than the newer XBRL entity IDs. "
            "These IDs are manually assigned when new FERC 714 data is is integrated, and any "
            "newly found utilities are added to "
            "the list with a new ID. "
            "This table is read in from a CSV stored in the PUDL "
            "repository: src/pudl/package_data/glue/respondent_id_ferc714.xlsx"
        ),
        "schema": {
            "fields": ["respondent_id_ferc714"],
            "primary_key": ["respondent_id_ferc714"],
            "foreign_key_rules": {"fields": [["respondent_id_ferc714"]]},
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl", "ferc714"],
    },
    "core_pudl__assn_ferc714_csv_pudl_respondents": {
        "description": (
            "This table maps the PUDL-assigned respondent ID FERC714 to the native "
            "respondent ID from the FERC714 CSV inputs - originally reported as respondent_id."
        ),
        "schema": {
            "fields": ["respondent_id_ferc714", "respondent_id_ferc714_csv"],
            "primary_key": ["respondent_id_ferc714", "respondent_id_ferc714_csv"],
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl", "ferc714"],
    },
    "core_pudl__assn_ferc714_xbrl_pudl_respondents": {
        "description": (
            "This table maps the PUDL-assigned respondent ID FERC714 to the native "
            "respondent ID from the FERC714 XBRL inputs - originally reported as entity_id."
        ),
        "schema": {
            "fields": ["respondent_id_ferc714", "respondent_id_ferc714_xbrl"],
            "primary_key": ["respondent_id_ferc714", "respondent_id_ferc714_xbrl"],
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl", "ferc714"],
    },
}
"""FERC Form 714 resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
