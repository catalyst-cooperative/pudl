# pudl.metadata.dfs

Static database tables.

## Attributes

| [`IMPUTATION_REASON_CODES`](#pudl.metadata.dfs.IMPUTATION_REASON_CODES)                       |                                                                                       |
|-----------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`FERC_ACCOUNTS`](#pudl.metadata.dfs.FERC_ACCOUNTS)                                           | FERC electric plant account IDs with associated row numbers and descriptions.         |
| [`BALANCING_AUTHORITY_SUBREGIONS_EIA`](#pudl.metadata.dfs.BALANCING_AUTHORITY_SUBREGIONS_EIA) |                                                                                       |
| [`EIA_SECTOR_AGGREGATE_ASSN`](#pudl.metadata.dfs.EIA_SECTOR_AGGREGATE_ASSN)                   | Association table describing the many-to-many relationships between plant sectors and |
| [`EIA_FUEL_AGGREGATE_ASSN`](#pudl.metadata.dfs.EIA_FUEL_AGGREGATE_ASSN)                       | Association table describing the many-to-many relationships between fuel types and    |
| [`POLITICAL_SUBDIVISIONS`](#pudl.metadata.dfs.POLITICAL_SUBDIVISIONS)                         | Static attributes of sub-national political jurisdictions.                            |
| [`SEC_EDGAR_STATE_AND_COUNTRY_CODES`](#pudl.metadata.dfs.SEC_EDGAR_STATE_AND_COUNTRY_CODES)   | State and country codes and their names as are reported to SEC's EDGAR database.      |
| [`ALPHA_2_COUNTRY_CODES`](#pudl.metadata.dfs.ALPHA_2_COUNTRY_CODES)                           | Alpha 2 country codes and the country's name.                                         |
| [`STANDARD_INDUSTRIAL_CLASSIFICATION`](#pudl.metadata.dfs.STANDARD_INDUSTRIAL_CLASSIFICATION) | A table of Standard Industrial Classification codes and descriptions used by SEC.     |

## Classes

| [`ImputationReasonCodes`](#pudl.metadata.dfs.ImputationReasonCodes)   | Defines all reasons a value might be flagged for imputation.   |
|-----------------------------------------------------------------------|----------------------------------------------------------------|

## Module Contents

### *class* pudl.metadata.dfs.ImputationReasonCodes(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Defines all reasons a value might be flagged for imputation.

#### MISSING_VALUE *= 'Indicates that reported value was already NULL.'*

#### ANOMALOUS_REGION *= 'Indicates that value is surrounded by flagged values.'*

#### NEGATIVE_OR_ZERO *= 'Indicates value is negative or zero.'*

#### IDENTICAL_RUN *= 'Indicates value is part of an identical run of values, excluding first value in run.'*

#### GLOBAL_OUTLIER *= 'Indicates value is greater or less than n times the global median.'*

#### GLOBAL_OUTLIER_NEIGHBOR *= 'Indicates value neighbors global outliers.'*

#### LOCAL_OUTLIER_HIGH *= 'Indicates value is a local outlier on the high end.'*

#### LOCAL_OUTLIER_LOW *= 'Indicates value is a local outlier on the low end.'*

#### DOUBLE_DELTA *= 'Indicates value is very different from neighbors on either side.'*

#### SINGLE_DELTA *= 'Indicates value is significantly different from nearest unflagged value.'*

#### BAD_YEAR *= 'Indicates the entire year of data for a respondent was dropped due to too much missing data.'*

#### SIMULATED *= 'Used for scoring imputation using simulated data. SHOULD NOT APPEAR IN PRODUCTION DATA.'*

### pudl.metadata.dfs.IMPUTATION_REASON_CODES

### pudl.metadata.dfs.FERC_ACCOUNTS *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

FERC electric plant account IDs with associated row numbers and descriptions.
From FERC Form 1 pages 204-207, Electric Plant in Service. Descriptions from:
[https://www.law.cornell.edu/cfr/text/18/part-101](https://www.law.cornell.edu/cfr/text/18/part-101)

### pudl.metadata.dfs.BALANCING_AUTHORITY_SUBREGIONS_EIA *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

### pudl.metadata.dfs.EIA_SECTOR_AGGREGATE_ASSN *= None*

Association table describing the many-to-many relationships between plant sectors and
various aggregates in core_eia_\_yearly_fuel_receipts_costs_aggs.

### pudl.metadata.dfs.EIA_FUEL_AGGREGATE_ASSN *= None*

Association table describing the many-to-many relationships between fuel types and
various aggregates in core_eia_\_yearly_fuel_receipts_costs_aggs.

Missing from these aggregates are all the “other” categories of gases: OG, BFG, SGP, SC,
PG. But those gases combine for about 0.2% of total MMBTU of reported fuel receipts.

### pudl.metadata.dfs.POLITICAL_SUBDIVISIONS *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)* *= None*

Static attributes of sub-national political jurisdictions.

Note AK and PR have incomplete EPA CEMS data, and so are excluded from is_epacems_state:
See
[https://github.com/catalyst-cooperative/pudl/issues/1264](https://github.com/catalyst-cooperative/pudl/issues/1264)

### pudl.metadata.dfs.SEC_EDGAR_STATE_AND_COUNTRY_CODES

State and country codes and their names as are reported to SEC’s EDGAR database.

These codes are used for XML filings of Ownership Reports (Forms 3, 4, 5), Form D and Form ID in EDGAR.
Table found at [https://www.sec.gov/submit-filings/filer-support-resources/edgar-state-country-codes](https://www.sec.gov/submit-filings/filer-support-resources/edgar-state-country-codes) .
Used in PUDL to standardize the state codes in the SEC 10K filings.

### pudl.metadata.dfs.ALPHA_2_COUNTRY_CODES

Alpha 2 country codes and the country’s name.

Most SEC locations from Ex. 21 attachments match the two digit EDGAR codes, however
some use alpha 2 country codes, i.e. us -> united states and ch -> switzerland.
Map these codes as well for location standardization.

### pudl.metadata.dfs.STANDARD_INDUSTRIAL_CLASSIFICATION *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)* *= None*

A table of Standard Industrial Classification codes and descriptions used by SEC.

These codes have mostly been supplanted by the NAICS codes, which provide more detail
and modernized industrial categories, but are still in use by SEC. Crosswalks exist that
can convert between SIC and NAICS codes, but the correspondence isn’t perfect.

#### SEE ALSO
* [https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list](https://www.sec.gov/search-filings/standard-industrial-classification-sic-code-list)
* [https://www.osha.gov/data/sic-manual](https://www.osha.gov/data/sic-manual)
* [https://www.zacharyschaller.com/data-and-crosswalks](https://www.zacharyschaller.com/data-and-crosswalks)
