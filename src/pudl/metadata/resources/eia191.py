"""Definitions of data tables primarily coming from EIA-191."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia191__monthly_gas_storage": {
        "description": {
            "additional_summary_text": (
                "monthly underground natural gas storage activity reported by operators "
                "of all storage fields on EIA Form 191 (RP8 monthly dataset, 2014–present). "
                "One row per storage reservoir per month."
            ),
            "additional_details_text": (
                "Storage field IDs are assigned per company and state; the same physical "
                "reservoir may appear with multiple IDs if ownership changes over time."
            ),
            "usage_warnings": [
                {
                    "type": "custom",
                    "description": (
                        "``total_field_capacity_mcf`` is not reliably equal to the sum of "
                        "``working_gas_capacity_mcf`` and ``base_gas_mcf``: approximately "
                        "23% of records differ. This reflects loose EIA definitions and "
                        "operator self-reporting practices, not data errors. Do not assume "
                        "additivity among these three fields."
                    ),
                }
            ],
        },
        "schema": {
            "fields": [
                "storage_field_id_eia191",
                "report_date",
                "state",
                "gas_field_id_eia",
                "reservoir_code",
                "company_name",
                "field_name",
                "reservoir_name",
                "field_type",
                "county",
                "status",
                "base_gas_mcf",
                "working_gas_capacity_mcf",
                "total_field_capacity_mcf",
                "maximum_daily_delivery_mcf",
                "region",
            ],
            "primary_key": [
                "storage_field_id_eia191",
                "report_date",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia191"],
        "etl_group": "eia191",
    },
}
