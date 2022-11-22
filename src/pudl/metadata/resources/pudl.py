"""Definitions for the connection between PUDL-specific IDs and other datasets.

Most of this is compiled from handmapping records.
"""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "political_subdivisions": {
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
    "plants_pudl": {
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
    "utilities_pudl": {
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
    "utility_plant_assn": {
        "schema": {
            "fields": ["utility_id_pudl", "plant_id_pudl"],
            "primary_key": ["utility_id_pudl", "plant_id_pudl"],
        },
        "etl_group": "glue",
        "field_namespace": "pudl",
        "sources": ["pudl"],
    },
}
"""
PUDL-specifiic resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
