"""Tables definitions for data from the EIA bulk electricity aggregates."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "fuel_receipts_costs_aggs_eia": {
        "description": (
            "Aggregated fuel receipts and costs from the EIA bulk " "electricty data."
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
        "sources": ["eia_bulk_elec"],
        "field_namespace": "eia_bulk_elec",
        "etl_group": "eia_bulk_elec",
    },
}
