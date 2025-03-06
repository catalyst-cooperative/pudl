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
    "core_sec10k__quarterly_company_information": {
        "description": (
            """Company information extracted from SEC10k filings.
This table provides attributes about SEC 10k filing companies across time.
It represents a pivoted version of the raw company information table with extracted
field values from the raw table as columns in this core table."""
        ),
        "schema": {
            "fields": [
                "central_index_key",
                "report_date",
                "filename_sec10k",
                "phone_number",
                "city",
                "company_name",
                "name_change_date",
                "film_number",
                "fiscal_year_end",
                "sec10k_version",
                "company_name_former",
                "company_id_irs",
                "sec_act",
                "filing_number_sec",
                "industry_id_sic",
                "industry_name_sic",
                "state",
                "state_of_incorporation",
                "street_address",
                "address_2",
                "zip_code",
                "zip_code_4",
            ],
            "primary_key": ["central_index_key", "report_date"],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
    "core_sec10k__changelog_company_name": {
        "description": (
            """A record of SEC company name changes and the reported date of the name change.
This table is pulled from ``core_sec10k__quarterly_company_information`` and contains
data extracted from SEC 10k filings."""
        ),
        "schema": {
            "fields": [
                "central_index_key",
                "report_date",
                "company_name",
                "name_change_date",
                "company_name_former",
            ],
            "primary_key": ["central_index_key", "report_date"],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
    "out_sec10k__quarterly_company_information": {
        "description": (
            """Company information extracted from SEC10k filings and matched
to EIA utilities using probabilistic record linkage. This table provides
attributes about SEC 10k filing companies across time. The match between ``central_index_key``
and ``utility_id_eia`` is one to one and doesn't change over time - the highest probability
EIA utility match for each SEC company is used for all dates of reported information for
that CIK."""
        ),
        "schema": {
            "fields": [
                "central_index_key",
                "report_date",
                "filename_sec10k",
                "utility_id_eia",
                "utility_name_eia",
                "phone_number",
                "city",
                "company_name",
                "name_change_date",
                "film_number",
                "fiscal_year_end",
                "sec10k_version",
                "company_name_former",
                "company_id_irs",
                "sec_act",
                "filing_number_sec",
                "industry_name_sic",
                "industry_id_sic",
                "state",
                "state_of_incorporation",
                "street_address",
                "address_2",
                "zip_code",
                "zip_code_4",
            ],
            "primary_key": ["central_index_key", "report_date"],
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
                "industry_name_sic",
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
