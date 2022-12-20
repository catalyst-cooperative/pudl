"""Definitions for the glue/crosswalk tables that connect data groups."""
from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "epacamd_eia": {
        "schema": {
            "fields": [
                "plant_id_epa",
                "emissions_unit_id_epa",
                "generator_id_epa",
                "plant_id_eia",
                "boiler_id",
                "generator_id",
            ],
        },
        "field_namespace": "glue",
        "etl_group": "glue",
        "sources": ["epacamd_eia"],
    },
}
"""PUDL-specifiic resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
