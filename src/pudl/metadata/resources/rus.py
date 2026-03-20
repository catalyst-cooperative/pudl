"""Table definitions for the RUS tables."""

from typing import Any

from pudl.metadata.codes import CODE_METADATA

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_rus__codes_investment_types": {
        "description": {
            "additional_summary_text": "investment types.",
        },
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["investment_type_code"]]},
        },
        "encoder": CODE_METADATA["core_rus__codes_investment_types"],
        "field_namespace": "rus",
        "sources": ["rus"],
        # I added this as RUS instead of RUS7 so we can compile any RUS code table
        # in one static_assets function
        "etl_group": "static_rus",
    },
    "core_rus__codes_fuel_types": {
        "description": {
            "additional_summary_text": "fuel types.",
        },
        "schema": {
            "fields": ["code", "description"],
            "primary_key": ["code"],
            "foreign_key_rules": {"fields": [["fuel_type_code"]]},
        },
        "encoder": CODE_METADATA["core_rus__codes_fuel_types"],
        "field_namespace": "rus",
        "sources": ["rus"],
        "etl_group": "static_rus",
    },
}
