"""Definitions for the glue/crosswalk tables that connect data groups."""
from typing import Any, Dict

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    "assn_gen_eia_unit_epa": {
        "schema": {
            "fields": [
                "generator_id",
                "plant_id_eia",
                "unit_id_epa",
            ],
        },
        "field_namespace": "glue",
        "etl_group": "glue",
        "sources": ["eia_epa_crosswalk"]
    },
    "assn_plant_id_eia_epa": {
        "schema": {
            "fields": [
                "plant_id_eia",
                "plant_id_epa",
            ],
        },
        "field_namespace": "glue",
        "etl_group": "glue",
        "sources": ["eia_epa_crosswalk"]
    },
    "plant_unit_epa": {
        "schema": {
            "fields": [
                "plant_id_epa",
                "unit_id_epa",
            ],
        },
        "field_namespace": "glue",
        "etl_group": "glue",
        "sources": ["eia_epa_crosswalk"]
    },
}
"""
PUDL-specifiic resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
