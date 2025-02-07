"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__quarterly_filings": {
        "description": (
            """Metadata describing all submitted SEC 10k filings.
This metadata contains information about the filing from the SEC's EDGAR database."""
        ),
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
        "description": (
            """Company ownership data extracted from Exhibit 21 attachments to SEC 10k filings.
This data is extracted from PDFs of the Exhibit 21 attachment using an information extraction
machine learning model. This table is connected to the SEC 10k filer information to create
a dataset on SEC ownership relationships in `out_sec10k__parents_and_subsidiaries`.
We only completed a first iteration of modeling to extract this data
and thus it contains errors in the parsing and structure of the data."""
        ),
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
            """Company information extracted from SEC 10K filings.
This table contains all year-quarters of data from SEC 10K filers.
The data is extracted from filings using a regex-based model.
It does not contain parent to subsidiary ownership information."""
        ),
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
            """Denormalized table containing SEC 10K filer company information as well
as the subsidiary companies reported in Exhibit 21 attachments to the 10K filing. The table
contains ownership information about parent to subsidiary company relationships as well as
a connection to EIA utility owner and operator companies. This output table contains only the
most recent quarter for each unique company name, address, and parent company combination. It
doesn't represent a time-dependent mapping between EIA and SEC companies. The `core_sec10k__company_information`
table contains all quarters of SEC 10K filer data.

The ownership data is extracted using a machine learning model and thus is probabilistic in nature.
Additionally, the connection to EIA is modeled using an entity matching model.
The creation of this table was done through the support of The Mozilla Foundation.
As we only had funding to conduct a first pass at modeling, we know this table has errors and
are seeking additional funding to conduct another round of model improvements."""
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
