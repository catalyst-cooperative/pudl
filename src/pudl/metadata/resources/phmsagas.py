"""Tables definitions for data coming from PHMSA natural gas data."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_phmsagas__yearly_distribution_operators": {
        "description": (
            "This table contains operator-level natural gas distribution"
            "data, corresponding to Parts A and D-I of the 2023 PHMSA gas "
            "distribution system annual report. That includes data on the "
            "operator name and location, the type of operator "
            "(e.g., investor-owned, municipally-owned), the type of gas "
            "being transported, and information on system-wide excavation "
            "damage, leaks, and unaccounted for gas. Each row corresponds "
            "to one report for one operator in one state, where revisions "
            "to the original submission have a different report ID."
        ),
        "schema": {
            "fields": [
                "report_date",
                "report_number",
                "report_submission_type",
                "report_year",
                "operator_id_phmsa",
                "operator_name_phmsa",
                "office_street_address",
                "office_city",
                "office_county",
                "office_zip",
                "office_state",
                "headquarters_street_address",
                "headquarters_city",
                "headquarters_county",
                "headquarters_state",
                "headquarters_zip",
                "excavation_damage_excavation_practices",
                "excavation_damage_locating_practices",
                "excavation_damage_one_call_notification",
                "excavation_damage_other",
                "excavation_damage_total",
                "excavation_tickets",
                "services_efv_in_system",
                "services_efv_installed",
                "services_shutoff_valve_in_system",
                "services_shutoff_valve_installed",
                "federal_land_leaks_repaired_or_scheduled",
                "unaccounted_for_gas_fraction",
                "additional_information",
                "preparer_email",
                "preparer_fax",
                "preparer_name",
                "preparer_phone",
                "preparer_title",
            ],
            "primary_key": ["report_number"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_by_material": {
        "description": {
            "additional_summary_text": (
                "miles of mains and the number of services in operation at "
                "the end of the year by material for each gas distribution "
                "operator."
            ),
            "additional_source_text": "(Part B - System Description / Section 1 - General)",
            "usage_warnings": ["aggregation_hazard"],
        },
        "schema": {
            "fields": [
                "report_year",
                "report_number",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "material",
                "mains_miles",
                "services",
            ],
            "primary_key": ["report_number", "material"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_by_install_decade": {
        "description": {
            "additional_summary_text": (
                "miles of mains and the number of services in operation at "
                "the end of the year by install decade."
            ),
            "additional_source_text": "(Part B - System Description / Section 4)",
            "usage_warnings": ["aggregation_hazard"],
            "additional_details_text": (
                "The records with an install decade of all_time are a total - "
                "beware of aggregating these values."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "report_number",
                "report_submission_type",
                "report_year",
                "operator_id_phmsa",
                "install_decade",
                "mains_miles",
                "services",
            ],
            "primary_key": ["report_number", "install_decade"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_leaks": {
        "description": {
            "additional_summary_text": (
                "total and hazardous leaks eliminated or repaired during the report year."
            ),
            "additional_source_text": "(Part C)",
            "usage_warnings": ["aggregation_hazard"],
        },
        "schema": {
            "fields": [
                "report_year",
                "report_number",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "leak_severity",
                "leak_source",
                "mains_miles",
                "services",
            ],
            "primary_key": ["report_number", "leak_severity", "leak_source"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_by_material_and_size": {
        "description": {
            "additional_summary_text": (
                "miles of mains and the number of services in operation at "
                "the end of the year by material and size of pipe."
            ),
            "additional_source_text": "(Part B - System Description / Section 3)",
            "usage_warnings": ["aggregation_hazard"],
            "additional_details_text": (
                "There is a column called main_other_material_detail which contains "
                "notes about the other material types "
                "that is reported in core_phmsagas__yearly_distribution_operators. See "
                "that table for further details."
            ),
        },
        "schema": {
            "fields": [
                "report_year",
                "report_number",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "main_size",
                "material",
                "mains_miles",
                "services",
            ],
            "primary_key": ["report_number", "main_size", "material"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_excavation_damages": {
        "description": {
            "additional_summary_text": ("excavation damages from various sources."),
            "additional_source_text": "(Part D - Excavation Damage)",
            "usage_warnings": ["aggregation_hazard"],
        },
        "schema": {
            "fields": [
                "report_year",
                "report_number",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "damage_type",
                "damage_sub_type",
                "damages",
            ],
            "primary_key": ["report_number", "damage_type", "damage_sub_type"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
}
