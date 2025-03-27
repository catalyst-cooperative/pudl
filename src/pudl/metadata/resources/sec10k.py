"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__assn__sec10k_filers_and_eia_utilities": {
        "description": """Associations between SEC 10k filing companies and EIA utilities.
SEC company index keys are matched to EIA utility IDs using probabilistic
record linkage. The match between ``central_index_key``and ``utility_id_eia`` is one to one
and doesn't change over time - the highest probability
EIA utility match for each SEC company is used for all dates of reported information for
that CIK.""",
        "schema": {"fields": ["central_index_key", "utility_id_eia"]},
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__quarterly_filings": {
        "description": "Metadata describing all submitted SEC 10k filings.",
        "schema": {
            "fields": [
                "filename_sec10k",
                "central_index_key",
                "company_name",
                "sec10k_type",
                "filing_date",
                "exhibit_21_version",
                "report_date",
            ],
            "primary_key": [
                "filename_sec10k",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
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
        "etl_group": "sec10k",
        "field_namespace": "sec",
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
                "filename_sec10k",
                "filer_count",
                "central_index_key",
                "company_name",
                "fiscal_year_end",
                "taxpayer_id_irs",
                "incorporation_state",
                "industry_name_sic",
                "industry_id_sic",
                "film_number",
                "sec10k_type",
                "sec_act",
                "filing_number_sec",
                "phone_number",
                "business_street_address",
                "business_street_address_2",
                "business_city",
                "business_state",
                "business_zip_code",
                "business_zip_code_4",
                "business_postal_code",
                "mail_street_address",
                "mail_street_address_2",
                "mail_city",
                "mail_state",
                "mail_zip_code",
                "mail_zip_code_4",
                "mail_postal_code",
            ],
            "primary_key": ["filename_sec10k", "filer_count"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__changelog_company_name": {
        "description": (
            """A record of SEC company name changes and the reported date of the name change.

This table is pulled from the same SEC 10-K filing header information as
``core_sec10k__quarterly_company_information``. There are many duplicate reports of
historical name changes in different filings, which are deduplicated to create this
table. The original name change data only contains the former name and the date of the
change. We use the most recently reported company name associated with the
``central_index_key`` to fill in the most recent value of ``company_name_new``.

Roughly 2% of all records describe multiple name changes happening on the same date
(they are duplicates on the basis of ``central_index_key`` and ``name_change_date``).
This may be due to company name reporting inconsistencies or reporting errors in which
the old and new company names have been swapped. Rougly 1000 reported "name changes"
in which the old and new names were identical have been dropped."""
        ),
        "schema": {
            "fields": [
                "central_index_key",
                "name_change_date",
                "company_name_old",
                "company_name_new",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
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
                "sec10k_type",
                "company_name_old",
                "taxpayer_id_irs",
                "sec_act",
                "filing_number_sec",
                "industry_name_sic",
                "industry_id_sic",
                "state",
                "incorporation_state",
                "street_address",
                "address_2",
                "zip_code",
                "zip_code_4",
            ],
            "primary_key": ["central_index_key", "report_date"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
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
                "company_name_old",
                "industry_name_sic",
                "industry_id_sic",
                "incorporation_state",
                "location_of_incorporation",
                "taxpayer_id_irs",
                "files_sec10k",
                "parent_company_central_index_key",
                "fraction_owned",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
}
