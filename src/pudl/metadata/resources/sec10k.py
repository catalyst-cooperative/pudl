"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__quarterly_filings": {
        "description": "Metadata describing all submitted SEC 10k filings.",
        "schema": {
            "fields": [
                "filename_sec10k",
                "central_index_key",
                "company_name",
                "sec10k_version",
                "filing_date",
                "exhibit_21_version",
                "report_date",
            ],
            "primary_key": [
                "filename_sec10k",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
    "core_sec10k__quarterly_exhibit_21_company_ownership": {
        "description": "Company ownership data extracted from Exhibit 21 attachments to SEC 10k filings.",
        "schema": {
            "fields": [
                "filename_sec10k",
                "subsidiary_company_name",
                "subsidiary_company_location",
                "fraction_owned",
                "report_date",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
    # TODO: update this with the real schema and primary key
    "core_sec10k__quarterly_company_information": {
        "description": "Company information harvested from headers of SEC10k filings.",
        "schema": {
            "fields": [
                "filename_sec10k",
                "filer_count",
                "company_information_block",
                "company_information_block_count",
                "company_information_fact_name",
                "company_information_fact_value",
                "report_date",
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
        "field_namespace": "sec10k",
    },
    "raw_sec10k__quarterly_company_information": {
        "description": "Raw company information harvested from headers of SEC10k filings.",
        "schema": {
            "fields": [
                "filename_sec10k",
                "filer_count",
                "company_information_block",
                "company_information_block_count",
                "company_information_fact_name",
                "company_information_fact_value",
                "report_date",
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
        "field_namespace": "sec10k",
    },
    "out_sec10k__parents_and_subsidiaries": {
        "description": (
            "Denormalized table containing SEC 10-K company information with mapping "
            "between subsidiary and parent companies, as well as a linkage to EIA "
            "utilities."
        ),
        "schema": {
            "fields": [
                "company_id_sec10k",
                "filename_sec10k",
                "report_date",
                "central_index_key",
                "utility_id_eia",
                "street_address",
                "address_2",
                "city",
                "state",
                "company_name_raw",
                "name_change_date",
                "company_name_former",
                "industry_description_sic",
                "industry_id_sic",
                "state_of_incorporation",
                "location_of_incorporation",
                "company_id_irs",
                "files_sec10k",
                "parent_company_central_index_key",
                "fraction_owned",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
}
