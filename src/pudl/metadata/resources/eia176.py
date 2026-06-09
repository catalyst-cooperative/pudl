"""Definitions of data tables primarily coming from EIA-176."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_eia176__yearly_gas_imports": {
        "description": {
            "additional_summary_text": (
                "a company's detailed natural gas receipts from another state or a "
                "U.S. border."
            ),
            "additional_source_text": "(Part 4, Line 3.0)",
            "usage_warnings": [
                {
                    "type": "ambiguous_supplier_names",
                    "description": (
                        "The supplier_name field is a free-text field with observed inconsistency in reporting "
                        "standards. It may contain a company or country name,"
                        "non-standard locations such as 'outside usa', or not be reported at all. Truncation of text has "
                        "also been observed in earlier years of data."
                    ),
                },
                {
                    "type": "inconsistent_supplier_locations",
                    "description": (
                        "The supplier_location field is a free-text field that contains "
                        "U.S. state codes, country codes that don't conform to the ISO 3166-1 alpha-2 "
                        "standards (e.g., C2, JA), and codes that do not appear to correspond to other "
                        "geographies (e.g., FX for Gulf of Mexico). Notably, there are a few instances "
                        "where supplier names and supplier locations do not appear to correspond. "
                        "Exercise caution when using this field, which requires further normalization."
                        "To focus on imports within the U.S. vs. outside of it, filter on the supplier_location_type"
                        "column."
                    ),
                },
            ],
            "additional_primary_key_text": (
                "This table has no enforced primary key because some operators do not "
                "report a supplier name, a few operators report more than one shipment from a supplier within a given year, "
                "and transportation mode was not reported prior to 2014. "
                "The natural primary key would be one record per report_year, operating_state, supplier_location, "
                "supplier_name, and mode of transportation."
            ),
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "supplier_location",
                "supplier_location_type",
                "supplier_name",
                "mode_of_transportation",
                "volume_mcf",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    },
    "core_eia176__yearly_gas_exports": {
        "description": {
            "additional_summary_text": (
                "a company's detailed natural gas deliveries out of the report state."
            ),
            "additional_source_text": "(Part 6, Line 14.0)",
            "usage_warnings": [
                {
                    "type": "ambiguous_recipient_names",
                    "description": (
                        "The recipient_name field is a free-text field with observed inconsistency in reporting "
                        "standards. It may contain a company or country name,"
                        "non-standard locations such as 'outside usa', or not be reported at all. Truncation of text has "
                        "also been observed in earlier years of data."
                    ),
                },
                {
                    "type": "inconsistent_destination_codes",
                    "description": (
                        "The recipient_location field is a free-text field that contains "
                        "U.S. state codes, country codes that don't conform to the ISO 3166-1 alpha-2 "
                        "standards (e.g., C2, JA), and codes that do not appear to correspond to other "
                        "geographies (e.g., FX for Gulf of Mexico). Notably, there are a few instances "
                        "where recipient names and locations do not appear to correspond. "
                        "Exercise caution when using this field, which requires further normalization."
                        "To focus on imports within the U.S. vs. outside of it, filter on the recipient_location_type"
                        "column."
                    ),
                },
            ],
            "additional_primary_key_text": (
                "This table has no enforced primary key because some records do not "
                "report a recipient name, location or mode of transportation details. "
                "Additionally, over a hundred operators report more than one delivery to a recipient within a given year. "
                "The natural primary key would be one record per report_year, operating_state, supplier_location, "
                "supplier_name, and mode of transportation."
            ),
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "recipient_location",
                "recipient_location_type",
                "recipient_name",
                "mode_of_transportation",
                "volume_mcf",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    },
    "core_eia176__yearly_supplemental_gaseous_fuel_supplies": {
        "description": {
            "additional_summary_text": (
                "a company's detailed supplemental gaseous fuel supplies by fuel type."
            ),
            "additional_source_text": "(Part 4, Line 6.0)",
            "additional_details_text": (
                "The reported supplemental gaseous fuel types are normalized into categories from "
                "free-text descriptions."
            ),
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "fuel_type",
                "volume_mcf",
            ],
            "primary_key": [
                "operator_id_eia",
                "report_year",
                "fuel_type",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    },
    "core_eia176__yearly_gas_disposition_other": {
        "description": {
            "additional_summary_text": (
                "a company's detailed other natural gas disposition within the report "
                "state."
            ),
            "usage_warnings": [
                {
                    "type": "duplicate_fuel_types",
                    "description": (
                        "Where an operator has reported multiple dispositions of the same 'other' type "
                        "within a year, we aggregate volumes to be able to enforce the table's natural primary key."
                    ),
                }
            ],
            "additional_source_text": "(Part 6, Line 18.4)",
            "additional_details_text": (
                "The EIA-176 instructions describe Line 18.4 as other disposition "
                "within the report state and ask respondents to specify the type. "
                "Reporting instructions also note that vented/flared volumes and "
                "extraction loss volumes should be reported on this line. "
                "A number of records originally reported as 'no label' are "
                "reported as 'unknown' in this table, otherwise reported values are lightly cleaned."
            ),
        },
        "schema": {
            "fields": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "disposition_type",
                "volume_mcf",
            ],
            "primary_key": [
                "operator_id_eia",
                "report_year",
                "operating_state",
                "disposition_type",
            ],
        },
        "field_namespace": "eia",
        "sources": ["eia176"],
        "etl_group": "eia176",
    },
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
                "a company's natural and supplemental gas disposition for the report state."
            ),
            "additional_source_text": "(Part 6, Lines 9, 12-20)",
            "additional_details_text": """The ``deliveries_out_of_state_volume_mcf`` (Line 14.0) are reported as one aggregated volume,
calculated by summing the original granular data. Similarly, ``other_disposition_all_other_mcf``
(Line 18.4) is summed from the original granular data and reported as one aggregate field. Use
``core_eia176__yearly_gas_exports`` and ``core_eia176__yearly_gas_disposition_other`` to inspect
these unaggregated records.

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
