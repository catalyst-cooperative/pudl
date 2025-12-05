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
            # TODO (12-03-25): Once we add the granular records for these two fields,
            # update to point at these tables for the unaggregated data.
            "additional_summary_text": (
                "a company's natural and supplemental gas disposition for the report state."
            ),
            "additional_source_text": "(Part 6, Lines 9, 12-20)",
            "additional_details_text": """The ``deliveries_out_of_state_volume_mcf`` (Line 14.0) are reported as one aggregated volume,
calculated by summing the original granular data. Similarly, ``disposition_to_other_volume_mcf``
(Line 18.4) is summed from the original granular data and reported as one aggregate field.

The ``delivered_gas_heat_content_mmbtu_per_mcf`` is expected to be between 0.8 and 1.2
by the EIA. We find that less than 0.5 percent of data falls outside of these expected bounds.

The ``total_disposition_mcf`` field includes disposition to consumers which is reported in core_eia176__yearly_gas_disposition_by_consumer.
Note that the reported total disposition and the sum of values in this table and core_eia176__yearly_gas_disposition_by_consumer often don't match as would be expected.
Thus, we have preserved both the total field and the sub-components in these two tables.
            """,
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "delivered_gas_heat_content_mmbtu_per_mcf",
                "operational_consumption_facility_space_heat_mcf",
                "operational_consumption_new_pipeline_fill_mcf",
                "operational_consumption_compressors_mcf",
                "operational_consumption_lng_vaporization_liquefaction_mcf",
                "operational_consumption_vehicle_fuel_mcf",
                "operational_consumption_other_mcf",
                "operational_consumption_other_detail",
                "operational_storage_underground_mcf",
                "operational_lng_storage_injections_mcf",
                "producer_lease_use_mcf",
                "producer_returned_for_repressuring_reinjection_mcf",
                "disposition_distribution_companies_mcf",
                "disposition_storage_operators_mcf",
                "disposition_other_pipelines_mcf",
                "disposition_out_of_state_mcf",
                "other_disposition_all_other_mcf",
                "total_disposition_mcf",
                "losses_mcf",
                "unaccounted_for_mcf",
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
