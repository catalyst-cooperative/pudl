"""Table definitions for the SEC10k tables."""

from typing import Any

RESOURCE_METADATA: dict[str, dict[str, Any]] = {
    "core_sec10k__quarterly_filings": {
        "description": (
            """Metadata describing all submitted SEC 10-K filings.
This metadata contains information about all year-quarters of SEC 10-K filings from the SEC's EDGAR database
for any company that files a 10-K. The central index key can be used to link company information
in other tables back to its raw filing filename and EDGAR metadata."""
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
a dataset on SEC ownership relationships in :ref:`out_sec10k__parents_and_subsidiaries`.
The table contains information about subsidiary company names, location of incorporation,
and ownership fraction. Often, location and ownership fraction aren't reported in the Exhibit 21
attachment. The filename can be linked back to information about the parent company of each
subsidiary company.
We only completed a first iteration of modeling to extract this data
and thus the data is imperfect due to the nature of the probabilistic modeling used to
extract and structure the data."""
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
This data is extracted from a header in the 10k filings and contains some high-level
data about the filing company. This header is a collection of key value pairs that
organized into a couple different "blocks" (contains key values
city, state, street_address etc.). This table does not contain parent to subsidiary
ownership information."""
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
as the subsidiary companies reported in Exhibit 21 attachments to the 10K filing.
For companies that are a subsidiary of another company, the table provides the
``company_id_sec`` of the parent company and, when available, the fraction of the company is owned by the parent company.
Additionally, the table provides a modeled connection to EIA utility owner and operator
companies when that company also reports to EIA. Only the most recently reported
company information and ownership data for each company is provided in this table. It
doesn't represent a time-dependent mapping between EIA and SEC companies.
The :ref:`core_sec10k__quarterly_company_information` table contains all quarters of SEC 10K filer data.

The ownership data is extracted using a machine learning model and thus is probabilistic in nature.
Additionally, the connection to EIA is modeled using an entity matching model.
The creation of this table was done through the support of The Mozilla Foundation.
As we only had funding to conduct a first pass at modeling, we know this table is imperfect
due to the nature of the probabilistic modeling used to extract the data and
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
