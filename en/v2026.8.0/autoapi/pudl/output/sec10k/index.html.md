# pudl.output.sec10k

Denormalized output tables for the SEC 10-K assets.

These tables are created by joining the raw SEC 10-K tables with other data from the
PUDL database, and enriching them with additional information. The resulting tables are
more user-friendly and easier to work with than the normalized core tables.

## Attributes

| [`logger`](#pudl.output.sec10k.logger)   |    |
|------------------------------------------|----|

## Functions

| [`_filename_sec10k_to_source_url`](#pudl.output.sec10k._filename_sec10k_to_source_url)(→ pandas.Series)                | Construct the source URL for SEC 10-K filings.                                  |
|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`_fill_sics`](#pudl.output.sec10k._fill_sics)(→ pandas.DataFrame)                                                     | Fill missing SIC IDs and names where possible.                                  |
| [`out_sec10k__quarterly_filings`](#pudl.output.sec10k.out_sec10k__quarterly_filings)(→ pandas.DataFrame)               | Denormalized table for SEC 10-K quarterly filings.                              |
| [`out_sec10k__quarterly_company_information`](#pudl.output.sec10k.out_sec10k__quarterly_company_information)(...)      | Company information extracted from SEC10k filings and matched to EIA utilities. |
| [`out_sec10k__changelog_company_name`](#pudl.output.sec10k.out_sec10k__changelog_company_name)(→ pandas.DataFrame)     | Denormalized table for company name changes from SEC 10-K filings.              |
| [`out_sec10k__parents_and_subsidiaries`](#pudl.output.sec10k.out_sec10k__parents_and_subsidiaries)(→ pandas.DataFrame) | Denormalized output table linking SEC 10-K company ownership to EIA Utilities.  |

## Module Contents

### pudl.output.sec10k.logger

### pudl.output.sec10k.\_filename_sec10k_to_source_url(filename_sec10k: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Construct the source URL for SEC 10-K filings.

### pudl.output.sec10k.\_fill_sics(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill missing SIC IDs and names where possible.

Within the reporting history for each company, as identified by Central Index Key
(CIK), fill in missing values of `industry_id_sic` when the values reported before
and after the gap are the same. If the beginning of the series is missing, backfill
it. If the end of the series is missing, forward fill it. Assign industry groups and
specific industry names based on the canonical descriptions from the SEC which
correspond to the reported industry ID.

This step is deferred until the output table because it requires the `report_date`
column, which is not part of the `core_sec10k__quarterly_company_information`
table

### pudl.output.sec10k.out_sec10k_\_quarterly_filings(core_sec10k_\_quarterly_filings: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalized table for SEC 10-K quarterly filings.

This table contains the basic information about the quarterly filings, including
the filing date, report date, and the URL to the filing.

### pudl.output.sec10k.out_sec10k_\_quarterly_company_information(core_sec10k_\_quarterly_company_information: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_sec10k_\_quarterly_filings: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_sec10k_\_assn_sec10k_filers_and_eia_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_entity_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Company information extracted from SEC10k filings and matched to EIA utilities.

### pudl.output.sec10k.out_sec10k_\_changelog_company_name(core_sec10k_\_changelog_company_name: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalized table for company name changes from SEC 10-K filings.

The original data contains only the former name and date of the name change, leaving
the current name out. This asset constructs a column with the new company name in
it, by shifting the old name column by one row within each central_index_key group,
and then fills in the last new company name value with the current company name.

### pudl.output.sec10k.out_sec10k_\_parents_and_subsidiaries(core_sec10k_\_quarterly_exhibit_21_company_ownership: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_sec10k_\_quarterly_company_information: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_sec10k_\_assn_exhibit_21_subsidiaries_and_filers: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_sec10k_\_assn_exhibit_21_subsidiaries_and_eia_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_entity_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Denormalized output table linking SEC 10-K company ownership to EIA Utilities.
