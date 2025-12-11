"""Tables definitions for data coming from the FERC Form 714."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_ferc714__respondent_id": {
        "description": {
            "additional_summary_text": "Respondent identification.",
            "additional_source_text": "(Part I, Schedule 1)",
        },
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
    "core_ferc714__hourly_planning_area_demand": {
        "description": {
            "additional_summary_text": "electricity demand by planning area.",
            "additional_source_text": "(Part III, Schedule 2a)",
            "usage_warnings": [
                {
                    "type": "custom",
                    "description": "The datetime_utc timestamps have been cleaned due to inconsistent datetime reporting. See below for additional details.",
                }
            ],
            "additional_details_text": (
                "This table includes data from the pre-2021 CSV raw source "
                "as well as the newer 2021 through present XBRL raw source.\n\n"
                "This table includes three respondent ID columns: one from the "
                "CSV raw source, one from the XBRL raw source and another that is PUDL-derived "
                "that links those two source ID's together. This table has filled in source IDs "
                "for all records so you can select the full timeseries for a given respondent from "
                "any of these three IDs.\n\nAn important "
                "caveat to note is that there was some cleaning done to the datetime_utc "
                "timestamps. The Form 714 includes sparse documentation for respondents "
                "for how to interpret timestamps - the form asks respondents to provide "
                "24 instances of hourly demand for each day. The form is labeled with hour "
                "1-24. There is no indication if hour 1 begins at midnight.\n\nThe XBRL data "
                "contained several formats of timestamps. Most records corresponding to hour "
                "1 of the Form have a timestamp with hour 1 as T1. About two thirds of the records "
                "in the hour 24 location of the form have a timestamp with an hour reported as "
                "T24 while the remaining third report this as T00 of the next day. T24 is not a "
                "valid format for the hour of a datetime, so we convert these T24 hours into "
                "T00 of the next day. A smaller subset of the respondents reports the 24th hour "
                "as the last second of the day - we also convert these records to the T00 of the "
                "next day."
            ),
        },
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
    "out_ferc714__hourly_planning_area_demand": {
        "description": (
            {
                "additional_summary_text": "electricity demand by planning area.",
                "additional_source_text": "(Part III, Schedule 2a)",
                "usage_warnings": [
                    "imputed_values",
                    {
                        "type": "custom",
                        "description": (
                            "The datetime_utc timestamps have been cleaned due to "
                            "inconsistent datetime reporting. See :ref:`core_ferc714__hourly_planning_area_demand` for additional details."
                        ),
                    },
                ],
                "additional_details_text": (
                    "This table is based on :ref:`core_ferc714__hourly_planning_area_demand`, but adds "
                    "imputed demand values where the original data was missing or anomalous. "
                    "Codes explaining why values have been imputed can be found in the "
                    ":ref:`core_pudl__codes_imputation_reasons` table."
                ),
            }
        ),
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_id_ferc714_csv",
                "respondent_id_ferc714_xbrl",
                "report_date",
                "datetime_utc",
                "timezone",
                "demand_reported_mwh",
                "demand_imputed_pudl_mwh",
                "demand_imputed_pudl_mwh_imputation_code",
            ],
            "primary_key": ["respondent_id_ferc714", "datetime_utc"],
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
        "create_database_schema": False,
    },
    "out_ferc714__respondents_with_fips": {
        "description": {
            "additional_summary_text": "Annual respondents with the county FIPS IDs for their service territories.",
            "additional_primary_key_text": (
                "Note that the state and county FIPS columns can be null. Otherwise "
                "the natural primary key would be: "
                "['respondent_id_ferc714', 'report_date', 'county_id_fips']. The state "
                "FIPS code is not part of the primary key because the first two digits "
                "of the county FIPS code indicate the state."
            ),
        },
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
        },
        "sources": ["ferc714", "censusdp1tract", "eia861"],
        "field_namespace": "ferc714",
        "etl_group": "outputs",
    },
    "out_ferc714__georeferenced_respondents": {
        "description": """An annual summary of demand and other information about FERC-714 respondents.

This table differs from :ref:`out_ferc714__summarized_demand` in that it also
includes a geometry column describing the respondent's service territory in each year.
These service territories are based on the counties that the corresponding EIA-861
respondent reported serving in that year. There is sometimes ambiguity as to
whether a FERC-714 respondent should be interpreted as an individual utility or a
balancing authority. The ``respodent_type`` column indicates which type of entity has
been assumed in determining the service territory from EIA-861 data.""",
        "schema": {
            "fields": [
                "report_date",
                "respondent_id_ferc714",
                "geometry",
                "eia_code",
                "respondent_type",
                "respondent_name_ferc714",
                "balancing_authority_id_eia",
                "balancing_authority_code_eia",
                "balancing_authority_name_eia",
                "utility_id_eia",
                "utility_name_eia",
                "population",
                "area_km2",
                "demand_annual_mwh",
            ],
            "primary_key": ["respondent_id_ferc714", "report_date"],
        },
        "sources": ["ferc714", "censusdp1tract", "eia861"],
        "field_namespace": "ferc714",
        "etl_group": "outputs",
        "create_database_schema": False,
    },
    "out_ferc714__summarized_demand": {
        "description": {
            "additional_summary_text": (
                "Summarized demand statistics and FERC-714 respondent attributes by respondent-year."
            )
        },
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
        "description": {
            "additional_summary_text": "10-year forecasted summer and winter peak demand and annual net energy "
            "per planning area.",
            "additional_source_text": "(Part III, Schedule 2b)",
            "usage_warnings": [
                {
                    "type": "custom",
                    "description": (
                        "This table "
                        "includes data from the pre-2021 CSV raw source as well as the newer 2021 "
                        "through present XBRL raw source. We created the respondent_id_ferc714 "
                        "field to blend disparate IDs from the CSV and XBRL data over time. See "
                        "the :ref:`core_ferc714__respondent_id` table for links to the original source IDs."
                    ),
                },
                {
                    "type": "custom",
                    "description": "There is a small handful of respondents (~11) that report more than 10 "
                    "years and an even smaller handful that report less than 10 (~9).",
                },
            ],
            "additional_details_text": (
                "This table contains forecasted net demand (MWh) as well as summer and winter "
                "peak demand (MW) for the next ten years after after the report_year."
            ),
        },
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
        "description": {
            "table_type_code": "entity",
            "additional_summary_text": "PUDL-derived FERC 714 respondent IDs.",
            "additional_details_text": (
                "These IDs are used to connect "
                "older CSV data which uses different respondent IDs than the newer XBRL entity IDs. "
                "These IDs are manually assigned when new FERC 714 data is integrated, and any "
                "newly found utilities are added to "
                "the list with a new ID. "
                "This table is read in from a CSV stored in the PUDL "
                "repository: src/pudl/package_data/glue/respondent_id_ferc714.xlsx"
            ),
        },
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
        "description": {
            "additional_summary_text": "PUDL-assigned FERC714 respondent IDs and native FERC714 CSV respondent IDs originally reported as ``respondent_id``.",
        },
        "schema": {
            "fields": ["respondent_id_ferc714", "respondent_id_ferc714_csv"],
            "primary_key": ["respondent_id_ferc714", "respondent_id_ferc714_csv"],
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl", "ferc714"],
    },
    "core_pudl__assn_ferc714_xbrl_pudl_respondents": {
        "description": {
            "additional_summary_text": "PUDL-assigned FERC714 respondent IDs and native FERC714 XBRL respondent IDs originally reported as ``entity_id``.",
        },
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

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
