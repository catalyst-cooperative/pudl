"""Definitions of data tables derived from the EIA-930."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia930__hourly_balancing_authority_net_generation": {
        "description": ("""EIA-930 hourly balancing authority net generation."""),
        "schema": {
            "fields": [
                "report_datetime_local",
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "energy_source",
                "net_generation_reported_mw",
                "net_generation_adjusted_mw",
                "net_generation_imputed_mw",
            ],
            "primary_key": [
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "energy_source",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
    },
    "core_eia930__hourly_balancing_authority_demand": {
        "description": ("""EIA-930 hourly balancing authority demand."""),
        "schema": {
            "fields": [
                "report_datetime_local",
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "demand_forecast_mw",
                "demand_reported_mw",
                "demand_adjusted_mw",
                "demand_imputed_mw",
            ],
            "primary_key": [
                "report_datetime_utc",
                "balancing_authority_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
    },
    "core_eia930__hourly_subregion_demand": {
        "description": ("""EIA-930 hourly subregion demand."""),
        "schema": {
            "fields": [
                "report_datetime_local",
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "subregion_code_eia",
                "demand_reported_mw",
            ],
            "primary_key": [
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "subregion_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
    },
    "core_eia930__hourly_balancing_authority_interchange": {
        "description": ("""EIA-930 hourly balancing authority interchange."""),
        "schema": {
            "fields": [
                "report_datetime_local",
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "adjacent_balancing_authority_code_eia",
                "region_code_eia",
                "adjacent_region_code_eia",
                "interchange_mw",
            ],
            "primary_key": [
                "report_datetime_utc",
                "balancing_authority_code_eia",
                "adjacent_balancing_authority_code_eia",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
    },
    "core_eia930__assn_balancing_authority_region": {
        "description": """EIA-930 balancing authority to region association.""",
        "schema": {
            "fields": [
                "balancing_authority_code_eia",
                "region_code_eia",
            ],
            "primary_key": ["balancing_authority_code_eia"],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
    },
    "core_eia930__assn_balancing_authority_subregion": {
        "description": """EIA-930 balancing authority to subregion association.""",
        "schema": {
            "fields": ["subregion_code_eia", "balancing_authority_code_eia"],
            "primary_key": ["subregion_code_eia"],
        },
        "field_namespace": "eia",
        "sources": ["eia930"],
        "etl_group": "eia930",
    },
}
