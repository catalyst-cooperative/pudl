"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__assn_exhibit_21_subsidiaries_and_filers": {
        "description": """A table associating subsidiaries listed in Exhibit 21 with
their SEC central index key, if the subsidiary also files Form 10-K.

Exhibit 21 subsidiaries and SEC 10-K filers are considered matched if they have
identical names and the same location of incorporation.""",
        "schema": {
            "fields": ["subsidiary_company_id_sec10k", "central_index_key"],
            "primary_key": ["subsidiary_company_id_sec10k"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__assn_exhibit_21_subsidiaries_and_eia_utilities": {
        "description": """A table matching subsidiaries listed in Exhibit 21 with EIA
utilities.

An Exhibit 21 subsidiary is considered matched to an EIA utility if their names are
identical. Only subsidiaries that don't file SEC 10-K themselves are included in this
table. SEC 10-K filers have much more information available and can be matched using
probabilistic record linkage.""",
        "schema": {
            "fields": ["subsidiary_company_id_sec10k", "utility_id_eia"],
            "primary_key": ["subsidiary_company_id_sec10k"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__assn_sec10k_filers_and_eia_utilities": {
        "description": """An association (crosswalk) table relating SEC 10-K filers and EIA utilities.

SEC central index keys are matched to EIA utility IDs using probabilistic record
linkage based on associated company information like company name, business and mailing
addresses, and state of incorporation. The match between ``central_index_key`` and
``utility_id_eia`` is one to one and is not allowed to change over time. In cases where
there were multiple candidate matches, the match with the highest probability is
selected.""",
        "schema": {
            "fields": ["central_index_key", "utility_id_eia"],
            "primary_key": ["central_index_key", "utility_id_eia"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__quarterly_filings": {
        "description": """Metadata describing SEC 10-K filings.

Each SEC 10-K filing is submitted by a single company, but may contain information about
numerous other companies. This table indicates the company submitting the filing, as
well as some information about the overall filing. Each filing is guaranteed to have a
unique filename, but ~1% of all filings are one company submitting the same form
multiple times on the same day, so the filename is the only available natural primary
key.""",
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
    "out_sec10k__quarterly_filings": {
        "description": """Metadata describing SEC 10-K filings.

Each SEC 10-K filing is submitted by a single company, but may contain information about
numerous other companies. This table indicates the company submitting the filing, as
well as some information about the overall filing. Each filing is guaranteed to have a
unique filename, but ~1% of all filings are one company submitting the same form
multiple times on the same day, so the filename is the only available natural primary
key. This output table adds a link to the source URL for the filing, which is
constructed from the filename.""",
        "schema": {
            "fields": [
                "filename_sec10k",
                "central_index_key",
                "company_name",
                "sec10k_type",
                "filing_date",
                "exhibit_21_version",
                "report_date",
                "source_url",
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
        "description": """Subsidiary company ownership data from the SEC 10-K Exhibit 21 attachments.

Exhibit 21 is an unstructured text or PDF attachment to the main SEC 10-K filing
that is used to describe the subsidiaries owned by the filing company. It may or may not
provide the percentage of the subsidiary that is owned by the filing company, or the
location of the subsidiary. This data has been extracted probabilistically using
a machine learning model and contains some incompletions and errors. It should not be
treated as ground truth data.""",
        "schema": {
            "fields": [
                "filename_sec10k",
                "subsidiary_company_name",
                "subsidiary_company_location",
                "subsidiary_company_id_sec10k",
                "fraction_owned",
            ],
            "primary_key": ["filename_sec10k", "subsidiary_company_id_sec10k"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__quarterly_company_information": {
        "description": (
            """Company and filing information extracted from SEC 10-K filing headers.

While the SEC 10-K filings are submitted by a single company, they often contain
references to many other related companies. Information about these companies and the
filing itself are contained in text headers at the beginning of the filing. This table
contains data extracted from those headers. Each record in this table represents a
single observation of a company in a single filing. The ``filer_count`` indicates which
referenced company within a filing header the record corresponds to.

Because the same company may be referenced in a number of different filings submitted in
the same reporting period or even on the same day, this table contains apparently
duplicative records about many companies, that may be distinguished only by the filename
associated with the filing they appeared in and their filer count. Note that all
references to a particular company may not be perfectly consistent across all filings in
which they appear. The various company names, addresses, and other information
associated with the company's unique and permanent ``central_index_key`` are later used
as inputs into the probabilistic record linkage process."""
        ),
        "schema": {
            "fields": [
                "filename_sec10k",
                "central_index_key",
                "filer_count",
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
            "primary_key": ["filename_sec10k", "central_index_key"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "core_sec10k__changelog_company_name": {
        "description": (
            """A historical record of the names each SEC 10-K filer has used.

This table is extracted from the same SEC 10-K filing header information as
``core_sec10k__quarterly_company_information``. Each filing reports the full history of
name change associated with a company up to the date of that filing. Because individual
companies may appear in multiple filings in the same year, and the samy historical name
changes will be reported in multiple years, the raw input data contains many duplicate
entries, which are deduplicated to create this table. The original name change data only
contains the former name and the date of the change.

Roughly 2% of all records describe multiple name changes happening on the same date
(they are duplicates on the basis of ``central_index_key`` and ``name_change_date``).
This may be due to company name reporting inconsistencies or reporting errors in which
the old and new company names have been swapped."""
        ),
        "schema": {
            "fields": [
                "central_index_key",
                "name_change_date",
                "company_name_old",
                "company_name",
            ],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "out_sec10k__changelog_company_name": {
        "description": """Denormalized table for company name changes from SEC 10-K filings.

We use the company name reported in association each name change block in the company
information table to fill in the most recent value of ``company_name_new``. Roughly
1000 reported "name changes" in which the old and new names were identical have been
dropped.""",
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
            """Denormalized company and filing data extracted from SEC 10-K filings.

In addition to the information provided by the
``core_sec10k__quarterly_company_information`` table, this output table merges in the
associated ``utility_id_eia`` (and utility name) if it is available, as well as the
report and filing dates associated with the filing each record was extracted from, as
well as providing a link to the source URL for the filing."""
        ),
        "schema": {
            "fields": [
                "filename_sec10k",
                "central_index_key",
                "filer_count",
                "utility_id_eia",
                "utility_name_eia",
                "report_date",
                "filing_date",
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
                "source_url",
            ],
            "primary_key": ["filename_sec10k", "central_index_key"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
    "out_sec10k__parents_and_subsidiaries": {
        "description": (
            """A denormalized table containing information about parent companies that
file SEC Form 10-K and their subsidiaries, which may or may not file Form 10-K.

Company ownership fractions are extracted from SEC 10-K Exhibit 21. Information about
the companies is extracted primarily from the headers of the SEC 10-K filing.
Subsidiaries that file Form 10-K will have much more information available than those
that only appear as subsidiaries in Exhibit 21.

SEC 10-K filers and EIA utilities are matched using probabilistic record linkage.
Exhibit 21 subsidiaries that don't file a Form 10-K are matched to EIA utilities using
the company name."""
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
                "parent_company_business_city",
                "parent_company_business_state",
                "parent_company_business_street_address",
                "parent_company_business_street_address_2",
                "parent_company_business_zip_code",
                "parent_company_business_zip_code_4",
                "parent_company_mail_city",
                "parent_company_mail_state",
                "parent_company_mail_street_address",
                "parent_company_mail_street_address_2",
                "parent_company_mail_zip_code",
                "parent_company_mail_zip_code_4",
                "parent_company_incorporation_state",
                "parent_company_utility_id_eia",
                "parent_company_utility_name_eia",
                "parent_company_industry_name_sic",
                "parent_company_industry_id_sic",
                "parent_company_taxpayer_id_irs",
                "subsidiary_company_central_index_key",
                "subsidiary_company_phone_number",
                "subsidiary_company_business_city",
                "subsidiary_company_business_state",
                "subsidiary_company_business_street_address",
                "subsidiary_company_business_street_address_2",
                "subsidiary_company_business_zip_code",
                "subsidiary_company_business_zip_code_4",
                "subsidiary_company_mail_city",
                "subsidiary_company_mail_state",
                "subsidiary_company_mail_street_address",
                "subsidiary_company_mail_street_address_2",
                "subsidiary_company_mail_zip_code",
                "subsidiary_company_mail_zip_code_4",
                "subsidiary_company_incorporation_state",
                "subsidiary_company_utility_id_eia",
                "subsidiary_company_utility_name_eia",
                "subsidiary_company_industry_name_sic",
                "subsidiary_company_industry_id_sic",
                "subsidiary_company_taxpayer_id_irs",
            ],
            "primary_key": ["filename_sec10k", "subsidiary_company_id_sec10k"],
        },
        "sources": ["sec10k"],
        "etl_group": "sec10k",
        "field_namespace": "sec",
    },
}
