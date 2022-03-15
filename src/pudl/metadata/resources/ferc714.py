"""Tables definitions for data coming from the FERC Form 714."""

from typing import Any, Dict

RESOURCE_METADATA: Dict[str, Dict[str, Any]] = {
    "respondent_id_ferc714": {
        "description": "Respondent identification. FERC Form 714, Part I, Schedule 1.",
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "respondent_name_ferc714",
                "eia_code",
            ],
            "primary_key": ["respondent_id_ferc714"],
            "foreign_key_rules": {"fields": [["respodnent_id_ferc714"]]}
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
    },
    "demand_hourly_pa_ferc714": {
        "description": "Hourly electricity demand by plannting area. FERC Form 714, Part III, Schedule 2a.",
        "schema": {
            "fields": [
                "respondent_id_ferc714",
                "report_date",
                "utc_datetime",
                "timezone",
                "demand_mwh",
            ],
            "primary_key": ["respondent_id_ferc714", "utc_datetime"]
        },
        "sources": ["ferc714"],
        "field_namespace": "ferc714",
        "etl_group": "ferc714",
    }
}
"""
FERC Form 714 resource attributes by PUDL identifier (``resource.name``).

Keys are in alphabetical order.

See :func:`pudl.metadata.helpers.build_foreign_keys` for the expected format of
``foreign_key_rules``.
"""
