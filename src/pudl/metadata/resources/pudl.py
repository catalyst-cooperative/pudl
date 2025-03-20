"""Definitions for the connection between PUDL-specific IDs and other datasets.

Most of this is compiled from handmapping records.
"""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_pudl__codes_subdivisions": {
        "title": "Political Subdivisions",
        "description": (
            "Various static attributes associated with states, provinces, and other "
            "sub-national political subdivisions."
        ),
        "schema": {
            "fields": [
                "country_code",
                "country_name",
                "subdivision_code",
                "subdivision_name",
                "subdivision_type",
                "timezone_approx",
                "state_id_fips",
                "division_name_us_census",
                "division_code_us_census",
                "region_name_us_census",
                "is_epacems_state",
            ],
            "primary_key": ["country_code", "subdivision_code"],
        },
        "etl_group": "static_pudl",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
    "core_pudl__codes_imputation_reasons": {
        "title": "Imputation Reason Codes",
        "description": (
            "Contains codes and descriptions explaining why a value was flagged "
            "for imputation. Any time a column is imputed, there should be a corresponding "
            "column which contains codes for every value in the column that was imputed."
        ),
        "schema": {
            "fields": [
                "code",
                "description",
            ],
            "primary_key": ["code"],
        },
        "etl_group": "static_pudl",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
    "core_pudl__entity_plants_pudl": {
        "title": "PUDL Plants",
        "description": "Home table for PUDL assigned plant IDs. These IDs are manually generated each year when new FERC and EIA reporting is integrated, and any newly identified plants are added to the list with a new ID. Each ID maps to a power plant which is reported in at least one FERC or EIA data set. This table is read in from a spreadsheet stored in the PUDL repository: src/pudl/package_data/glue/pudl_id_mapping.xlsx",
        "schema": {
            "fields": ["plant_id_pudl", "plant_name_pudl"],
            "primary_key": ["plant_id_pudl"],
            "foreign_key_rules": {"fields": [["plant_id_pudl"]]},
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
    "core_pudl__entity_utilities_pudl": {
        "title": "PUDL Utilities",
        "description": "Home table for PUDL assigned utility IDs. These IDs are manually generated each year when new FERC and EIA reporting is integrated, and any newly found utilities are added to the list with a new ID. Each ID maps to a power plant owning or operating entity which is reported in at least one FERC or EIA data set. This table is read in from a spreadsheet stored in the PUDL repository: src/pudl/package_data/glue/pudl_id_mapping.xlsx",
        "schema": {
            "fields": ["utility_id_pudl", "utility_name_pudl"],
            "primary_key": ["utility_id_pudl"],
            "foreign_key_rules": {"fields": [["utility_id_pudl"]]},
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
    "core_pudl__assn_utilities_plants": {
        "title": "PUDL Utility-Plant Associations",
        "description": "Associations between PUDL utility IDs and PUDL plant IDs. This table is read in from a spreadsheet stored in the PUDL repository: src/pudl/package_data/glue/pudl_id_mapping.xlsx",
        "schema": {
            "fields": ["utility_id_pudl", "plant_id_pudl"],
            "primary_key": ["utility_id_pudl", "plant_id_pudl"],
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
    "core_pudl__codes_datasources": {
        "title": "PUDL Data Sources",
        "description": "Static table defining codes associated with the data sources that PUDL integrates.",
        "schema": {
            "fields": [
                "datasource",
                "partitions",
                "doi",
                "pudl_version",
            ],
            "primary_key": ["datasource"],
        },
        "etl_group": "static_pudl",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
    "out_ferc714__hourly_estimated_state_demand": {
        "title": "Estimated Hourly State Electricity Demand",
        "description": "Estimated hourly electricity demand for each state, scaled such that it matches the total electricity sales by state reported in EIA 861.",
        "schema": {
            "fields": [
                "state_id_fips",
                "datetime_utc",
                "demand_mwh",
                "scaled_demand_mwh",
            ],
            "primary_key": ["state_id_fips", "datetime_utc"],
        },
        "etl_group": "state_demand",
        "field_namespace": "pudl",
        "sources": ["ferc714", "eia861", "censusdp1"],
        "create_database_schema": False,
    },
}
"""PUDL-specifiic resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
