"""Tables definitions for data coming from PHMSA natural gas data."""

from typing import Any

GENERIC_CLEANING_STATE_WARNING = {
    "type": "custom",
    "description": "This table has been concatenated across all years and re-organized into a logical structure, but the data has not been fully cleaned. Except some inconsistent units, data types and values over the years of reported data. Once fully cleaned, this table will be deprecated and replaced with a core table.",
}

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "_core_phmsagas__yearly_distribution_filings": {
        "description": {
            "additional_summary_text": (
                "filings (aka submissions) from gas distribution system operators."
            ),
            "usage_warnings": [GENERIC_CLEANING_STATE_WARNING],
            "additional_details_text": (
                "This table contains information about the filer and filing type. "
                "This includes information about who filed but also whether this was "
                "an original filing or a correction."
            ),
        },
        "schema": {
            "fields": [
                "report_id",
                "operator_id_phmsa",
                "report_date",
                "filing_date",
                "initial_filing_date",
                "filing_correction_date",
                "report_filing_type",
                "data_date",
                "form_revision_id",
                "preparer_name",
                "preparer_title",
                "preparer_phone",
                "preparer_fax",
                "preparer_email",
            ],
            "primary_key": ["report_id", "report_date", "operator_id_phmsa"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "core_phmsagas__yearly_distribution_operators": {
        "description": {
            "additional_summary_text": ("distribution operator information."),
            "additional_source_text": "(Part A)",
            "additional_details_text": (
                "This table contains operator-level information including "
                "office and headquarter location."
            ),
        },
        "schema": {
            "fields": [
                "report_id",
                "report_date",
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
                "additional_information",
            ],
            "primary_key": ["report_id", "report_date", "operator_id_phmsa"],
            "foreign_key_rules": {
                "fields": [
                    ["report_id", "report_date", "operator_id_phmsa"],
                ],
            },
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
            "usage_warnings": [
                GENERIC_CLEANING_STATE_WARNING,
                "aggregation_hazard",
                {
                    "type": "custom",
                    "description": "The categories of material types have changed slightly over the years (ex: cast and wrought iron were broken up in two categories before 1984).",
                },
                {
                    "type": "custom",
                    "description": "Beginning in 2004, companies file one report per state. The operating_state column has not been normalized and may contain more than one state in earlier years of data.",
                },
            ],
            "additional_primary_key_text": (
                "We expect the primary key for this table should be report_id, "
                "operator_id_phmsa, operating_state and material. However, there are nulls in "
                "the operating_state across several years of reporting."
            ),
        },
        "schema": {
            "fields": [
                "report_id",
                "report_date",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "material",
                "mains_miles",
                "services",
            ],
            # "primary_key": [
            #     "report_id",
            #     "operator_id_phmsa",
            #     "operating_state",
            #     "material",
            # ],
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
            "usage_warnings": [GENERIC_CLEANING_STATE_WARNING, "aggregation_hazard"],
            "additional_details_text": (
                "The records with an install decade of total_decade are a total - "
                "beware of aggregating these values."
            ),
        },
        "schema": {
            "fields": [
                "report_id",
                "report_date",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "install_decade",
                "mains_miles",
                "services",
            ],
            "primary_key": [
                "report_id",
                "report_date",
                "operator_id_phmsa",
                "operating_state",
                "install_decade",
            ],
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
            "usage_warnings": [
                GENERIC_CLEANING_STATE_WARNING,
                "aggregation_hazard",
                {
                    "type": "custom",
                    "description": "Beginning in 2004, companies file one report per state. The operating_state column has not been normalized and may contain more than one state in earlier years of data.",
                },
            ],
            "additional_primary_key_text": (
                "We expect the primary key for this table should be report_id, "
                "operator_id_phmsa, operating_state, leak_severity and leak_source. "
                "There are nulls in the operating_state across several years of "
                "reporting."
            ),
        },
        "schema": {
            "fields": [
                "report_id",
                "report_date",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "leak_severity",
                "leak_source",
                "mains",
                "services",
            ],
            # "primary_key": [
            #     "report_id",
            #     "operator_id_phmsa",
            #     "operating_state",
            #     "leak_severity",
            #     "leak_source",
            # ],
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
            "usage_warnings": [
                GENERIC_CLEANING_STATE_WARNING,
                "aggregation_hazard",
                {
                    "type": "custom",
                    "description": "The size ranges in main_size have changed slightly over the years (ex: before 1984 they reported 0.5_in_or_less whereas after they reported 1_in_or_less)",
                },
                {
                    "type": "custom",
                    "description": "The categories of material types have changed slightly over the years (ex: cast and wrought iron were broken up in two categories before 1984).",
                },
                {
                    "type": "custom",
                    "description": "Beginning in 2004, companies file one report per state. The operating_state column has not been normalized and may contain more than one state in earlier years of data.",
                },
            ],
            "additional_primary_key_text": (
                "We expect the primary key for this table should be report_id, "
                "operator_id_phmsa, operating_state, main_size and material. "
                "There are nulls in the operating_state across several years of "
                "reporting."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "report_id",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "main_size",
                "material",
                "mains_miles",
                "services",
                "main_other_material_detail",
            ],
            # "primary_key": [
            #     "report_id",
            #     "operator_id_phmsa",
            #     "operating_state",
            #     "main_size",
            #     "material",
            # ],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_excavation_damages": {
        "description": {
            "additional_summary_text": ("excavation damages from various sources."),
            "additional_source_text": "(Part D - Excavation Damage)",
            "usage_warnings": [GENERIC_CLEANING_STATE_WARNING, "aggregation_hazard"],
        },
        "schema": {
            "fields": [
                "report_id",
                "report_date",
                "operator_id_phmsa",
                "commodity",
                "operating_state",
                "damage_type",
                "damage_sub_type",
                "damages",
            ],
            "primary_key": [
                "report_id",
                "damage_type",
                "damage_sub_type",
            ],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
    "_core_phmsagas__yearly_distribution_misc": {
        "description": {
            "additional_summary_text": ("miscellaneous distribution information."),
            "additional_source_text": "(Part B & C)",
            "usage_warnings": [
                GENERIC_CLEANING_STATE_WARNING,
                {
                    "type": "custom",
                    "description": "Beginning in 2004, companies file one report per state. The operating_state column has not been normalized and may contain more than one state in earlier years of data.",
                },
            ],
            "additional_primary_key_text": (
                "We expect the primary key for this table should be report_id, "
                "operator_id_phmsa, and operating_state. "
                "There are nulls in the operating_state across several years of "
                "reporting."
            ),
        },
        "schema": {
            "fields": [
                "report_date",
                "report_id",
                "operator_id_phmsa",
                "operating_state",
                "all_known_leaks_scheduled_for_repair",
                "all_known_leaks_scheduled_for_repair_main",
                "hazardous_leaks_mechanical_joint_failure",
                "federal_land_leaks_repaired_or_scheduled",
                "average_service_length_feet",
                "services_efv_in_system",
                "services_efv_installed",
                "services_shutoff_valve_in_system",
                "services_shutoff_valve_installed",
                "unaccounted_for_gas_fraction",
                "excavation_tickets",
            ],
            # "primary_key": ["report_id", "operating_state", "operator_id_phmsa"],
        },
        "sources": ["phmsagas"],
        "field_namespace": "phmsagas",
        "etl_group": "phmsagas",
    },
}
