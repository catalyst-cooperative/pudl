"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__filings": {
        "description": "Metadata describing all submitted SEC 10k filings.",
        "schema": {
            "fields": [
                "filename_sec10k",
                "central_index_key",
                "company_name",
                "sec10k_version",
                "date_filed",
                "exhibit_21_version",
                "year_quarter",
            ],
            "primary_key": [
                "filename_sec10k",
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
                "filename_sec10k",
                "subsidiary_company_name",
                "subsidiary_location",
                "fraction_owned",
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
                "filename_sec10k",
                "filer_count",
                "company_information_block",
                "company_information_block_count",
                "company_information_fact_name",
                "company_information_fact_value",
                "year_quarter",
            ],
            "primary_key": [
                "filename_sec10k",
                "filer_count",
                "company_information_block",
                "company_information_block_count",
                "company_information_fact_name",
                "company_information_fact_value",
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
                "company_id_sec",
                "filename_sec10k",
                "report_date",
                "report_year",
                "central_index_key",
                "utility_id_eia",
                "street_address",
                "address_2",
                "city",
                "state",
                "company_name_raw",
                "date_of_name_change",
                "company_name_former",
                "standard_industrial_classification",
                "state_of_incorporation",
                "location_of_incorporation",
                "company_id_irs",
                "files_10k",
                "parent_company_central_index_key",
                "fraction_owned",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "create_database_schema": False,
        "field_namespace": "sec10k",
    },
}
