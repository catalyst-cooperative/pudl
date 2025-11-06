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
                "customer_type",
                "sales_consumers",
                "sales_revenue",
                "sales_volume",
                "transport_consumers",
                "transport_revenue",
                "transport_volume",
                "volume",
            ],
            "primary_key": ["report_year", "operator_id_eia", "consumer_type"],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    }
}
