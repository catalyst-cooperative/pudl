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
    "core_eia176__yearly_gas_disposition": {
        "description": {
            "additional_summary_text": (
                "a company's natural and supplemental gas disposition withn the report state."
            ),
            "additional_source_text": "(Part6, Lines 9, 12-20)",
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "heat_content_of_delivered_gas",
                "op_gas_consumption_by_facility_space_heat",
                "op_gas_consumption_by_new_pipeline_fill",
                "op_gas_consumption_by_pipeline_dist_storage_compressor_use",
                "op_gas_consumption_by_vaporization_liquefaction_lng_fuel",
                "op_gas_consumption_by_vehicle_fuel",
                "op_gas_consumption_by_other",
                "operations_volume_underground_storage_injections",
                "operations_volume_lng_storage_injections",
                "lease_use_volume",
                "returns_for_repress_reinjection_volume",
                "losses_from_leaks_volume",
                "disposition_to_distribution_companies_volume",
                "disposition_to_other_pipelines_volume",
                "disposition_to_storage_operators_volume",
                "total_disposition_volume",
                "unaccounted_for",
                "out_of_state_deliveries_volume",
                "disposition_to_other_volume",
            ],
            "primary_key": [
                "operator_id_eia",
                "report_year",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    },
}
