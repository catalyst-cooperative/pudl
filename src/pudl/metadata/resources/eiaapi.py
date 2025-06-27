"""Tables definitions for data from the EIA bulk electricity aggregates."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia__yearly_fuel_receipts_costs_aggs": {
        "description": (
            "Aggregated fuel receipts and costs from the EIA bulk electricity data."
        ),
        "schema": {
            "fields": [
                "fuel_agg",
                "geo_agg",
                "sector_agg",
                "temporal_agg",
                "report_date",
                "fuel_received_mmbtu",
                "fuel_cost_per_mmbtu",
            ],
            "primary_key": [
                "fuel_agg",
                "geo_agg",
                "sector_agg",
                "temporal_agg",
                "report_date",
            ],
        },
        "sources": ["eiaapi"],
        "field_namespace": "eiaapi",
        "etl_group": "eiaapi",
    },
}
