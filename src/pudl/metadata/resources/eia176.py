"""Definitions of data tables primarily coming from EIA-860."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia176__yearly_gas_disposition_by_consumer": {
        "description": {
            "additional_summary_text": (
                "a company's natural gas deliveries to end-use consumers within the report state."
            ),
            "additional_source_text": "(Part 6, Lines 10.0-11.6)",
        },
        "schema": {
            "fields": [
                "report_year",
                "operator_id_eia",
                "operating_state",
                "customer_class",
                "revenue_class",
                "consumers",
                "revenue",
                "volume_mcf",
            ],
            "primary_key": [
                "report_year",
                "operator_id_eia",
                "customer_class",
                "revenue_class",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    },
    "core_eia176__yearly_liquefied_natural_gas_inventory": {
        "description": {
            "additional_summary_text": "LNG storage volume at end of the year",
            "additional_source_text": "(Part 5, Lines 8.0-8.2)",
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "lng_inventory_volume",
                "lng_facility_volume",
                "marine_terminal_facility_volume",
                "lng_facility_capacity",
                "marine_terminal_facility_capacity",
            ],
            "primary_key": [
                "operator_id_eia",
                "report_year",
            ],
        },
    },
}
