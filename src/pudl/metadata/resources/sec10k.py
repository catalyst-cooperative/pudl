"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__filings": {
        "description": "Metadata describing all submitted SEC 10k filings.",
        "schema": {
            "fields": [
                "sec10k_filename",
                "central_index_key",
                "company_name",
                "form_type",
                "date_filed",
                "exhibit_21_version",
                "year_quarter",
            ],
            "primary_key": [
                "sec10k_filename",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "create_database_schema": False,
        "field_namespace": "sec10k",
    },
    "core_sec10k__exhibit_21_company_ownership": {
        "description": "Company ownership data extracted from Exhibit 21 attachments to SEC 10k filings.",
        "schema": {
            "fields": [
                "sec10k_filename",
                "subsidiary",
                "location",
                "ownership_percentage",
                "year_quarter",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "create_database_schema": False,
        "field_namespace": "sec10k",
    },
    "core_sec10k__company_information": {
        "description": "Company information extracted from SEC 10k filings.",
        "schema": {
            "fields": [
                "sec10k_filename",
                "filer_count",
                "block",
                "block_count",
                "key",
                "value",
                "year_quarter",
            ],
            "primary_key": [
                "sec10k_filename",
                "filer_count",
                "block",
                "block_count",
                "key",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "create_database_schema": False,
        "field_namespace": "sec10k",
    },
    "out_sec10k__parents_and_subsidiaries": {
        "description": "Denormalized table containing SEC 10k company information with mapping between subsidiary and parent companies, as well as a linkage to EIA companies.",
        "schema": {
            "fields": [
                "sec_company_id",
                "sec10k_filename",
                "report_date",
                "report_year",
                "central_index_key",
                "utility_id_eia",
                "street_address",
                "street_address_2",
                "city",
                "state",
                "company_name_raw",
                "date_of_name_change",
                "former_conformed_name",
                "standard_industrial_classification",
                "state_of_incorporation",
                "location_of_inc",
                "irs_number",
                "files_10k",
                "parent_company_cik",
                "ownership_percentage",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "create_database_schema": False,
        "field_namespace": "sec10k",
    },
}
