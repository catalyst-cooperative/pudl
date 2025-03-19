"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__assn__exhibit_21_subsidiaries_and_filers": {
        "description": """A table matching subsidiary companies listed in Exhibit 21
attachments to its SEC central index key when the subsidiary also files a 10k.
We match these subsidiaries to companies that file an SEC 10k based on an
exact match on name and shared location of incorporation attributes.
``subsidiary_company_id_sec10k`` is an ID created from the subsidiary's filing,
name, and location of incorporation.""",
        "schema": {
            "fields": ["subsidiary_company_id_sec10k", "central_index_key"],
            "primary_key": ["subsidiary_company_id_sec10k"],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
    "core_sec10k__assn__exhibit_21_subsidiaries_and_eia_utilities": {
        "description": """A table matching subsidiary companies in Exhibit 21 attachments to
EIA utilities. Only subsidiaries which don't file an SEC 10-K filing themselves are included in
this table. We match these subsidiaries to EIA utilities with an exact match on company
name. ``subsidiary_company_id_sec10k`` is an ID created from the subsidiary's filing,
name, and location of incorporation.""",
        "schema": {
            "fields": ["subsidiary_company_id_sec10k", "utility_id_eia"],
            "primary_key": ["subsidiary_company_id_sec10k"],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
    "core_sec10k__assn__sec10k_filers_and_eia_utilities": {
        "description": """Associations between SEC 10k filing companies and EIA utilities.
SEC company index keys are matched to EIA utility IDs using probabilistic
record linkage. The match between ``central_index_key``and ``utility_id_eia`` is one to one
and doesn't change over time - the highest probability
EIA utility match for each SEC company is used for all dates of reported information for
that CIK.""",
        "schema": {"fields": ["central_index_key", "utility_id_eia"]},
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
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
                "report_date",
                "parent_company_central_index_key",
                "parent_company_name",
                "subsidiary_company_name",
                "subsidiary_company_location",
                "subsidiary_company_id_sec10k",
                "fraction_owned",
                "filing_date",
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
In the raw data, company information may be reported in multiple SEC 10k
filings from the same filing date. In this table, only one of these reported
blocks of information is kept. Records from filings where that company's extracted
``central_index_key`` matches the filer's central index key (meaning that
that company filed the 10k itself) are prioritized."""
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
                "taxpayer_id_irs",
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
                "company_name",
                "name_change_date",
            ],
            "primary_key": ["central_index_key", "company_name"],
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
                "taxpayer_id_irs",
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
            """Denormalized table containing SEC company ownership data
extracted from Exhibit 21 attachments with attributes about the parent and
subsidiary companies. Company attributes are extracted from the headers of
the SEC 10k filing. Company information is present for subsidiary companies
when that subsidiary in turn files a 10k. The connection between SEC filers
and EIA utilities is conducted with probabilistic record linkage. The connection
between Ex. 21 subsidiaries (who don't file a 10k) and EIA utilities is done with
a fuzzy match on the company name."""
        ),
        "schema": {
            "fields": [
                "filename_sec10k",
                "subsidiary_company_name",
                "subsidiary_company_location",
                "subsidiary_company_id_sec10k",
                "fraction_owned",
                "parent_company_central_index_key",
                "parent_company_name",
                "filing_date",
                "report_date",
                "parent_company_phone_number",
                "parent_company_city",
                "parent_company_name_change_date",
                "parent_company_name_former",
                "parent_company_state",
                "parent_company_state_of_incorporation",
                "parent_company_street_address",
                "parent_company_address_2",
                "parent_company_zip_code",
                "parent_company_zip_code_4",
                "parent_company_utility_id_eia",
                "parent_company_utility_name_eia",
                "subsidiary_company_central_index_key",
                "subsidiary_company_phone_number",
                "subsidiary_company_city",
                "subsidiary_company_name_change_date",
                "subsidiary_company_name_former",
                "subsidiary_company_state",
                "subsidiary_company_state_of_incorporation",
                "subsidiary_company_street_address",
                "subsidiary_company_address_2",
                "subsidiary_company_zip_code",
                "subsidiary_company_zip_code_4",
                "subsidiary_company_utility_id_eia",
                "subsidiary_company_utility_name_eia",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "pudl_models",
        "field_namespace": "sec10k",
    },
}
