# FERC Form 714 – Annual Electric Balancing Authority Area and Planning Area Report

| Source URL                      | [https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric](https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric)                                                                                                                                                               |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Source Description              | Electric transmitting utilities operating balancing authority areas and planning areas with annual peak demand over 200MW are required to file Form 714 with the Federal Energy Regulatory Commission (FERC), reporting balancing authority area generation, actual and scheduled inter-balancing authority area power transfers, and net energy for load, summer-winter generation peaks and system lambda. |
| Download Size                   | 194 MB                                                                                                                                                                                                                                                                                                                                                                                                       |
| Temporal Coverage               | 2006-2024                                                                                                                                                                                                                                                                                                                                                                                                    |
| PUDL Code                       | `ferc714`                                                                                                                                                                                                                                                                                                                                                                                                    |
| Unprocessed Source Data Archive | [10.5281/zenodo.4127100](https://doi.org/10.5281/zenodo.4127100)                                                                                                                                                                                                                                                                                                                                             |
| Issues                          | [Open FERC Form 714 – Annual Electric Balancing Authority Area and Planning Area Report issues](https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aferc714)                                                                                                                                                                                                   |

## PUDL Database Tables

We’ve segmented the processed data into the following normalized data tables.
Clicking on the links will show you a description of the table as well as
the names and descriptions of each of its fields.

| Data Dictionary                                                                                                                          | Browse Online                                                                                                                                                                             |
|------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [core_ferc714_\_hourly_planning_area_demand](../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand)                   | [https://data.catalyst.coop/preview/pudl/core_ferc714_\_hourly_planning_area_demand](https://data.catalyst.coop/preview/pudl/core_ferc714__hourly_planning_area_demand)                   |
| [core_ferc714_\_respondent_id](../data_dictionaries/pudl_db.md#core-ferc714-respondent-id)                                               | [https://data.catalyst.coop/preview/pudl/core_ferc714_\_respondent_id](https://data.catalyst.coop/preview/pudl/core_ferc714__respondent_id)                                               |
| [core_ferc714_\_yearly_planning_area_demand_forecast](../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast) | [https://data.catalyst.coop/preview/pudl/core_ferc714_\_yearly_planning_area_demand_forecast](https://data.catalyst.coop/preview/pudl/core_ferc714__yearly_planning_area_demand_forecast) |
| [out_ferc714_\_hourly_planning_area_demand](../data_dictionaries/pudl_db.md#out-ferc714-hourly-planning-area-demand)                     | [https://data.catalyst.coop/preview/pudl/out_ferc714_\_hourly_planning_area_demand](https://data.catalyst.coop/preview/pudl/out_ferc714__hourly_planning_area_demand)                     |

## Background

FERC Form 714, otherwise known as the Annual Electric Balancing Authority Area and
Planning Area Report, collects data and provides insights about balancing authority
area and planning area operations.

### Download additional documentation

* [`Ferc714 Blank 2022-06-30 (PDF)`](ferc714/ferc714_blank_2022-06-30.pdf)
* [`Ferc714 Blank 2025-07-31 (HTML)`](ferc714/ferc714_blank_2025-07-31.html)
* [`Ferc714 Instructions 2021-04-16 (PDF)`](ferc714/ferc714_instructions_2021-04-16.pdf)

### Data available through PUDL

The data we’ve integrated from FERC Form 714 includes:

* Hourly electricity demand by utility or balancing authority.
* Annual demand forecast.
* A table identifying the form respondents including their EIA utility or balancing
  authority ID, which allows us to link the FERC-714 data to other information
  reported in [EIA Form 860 – Annual Electric Generator Report](eia860.md) and [EIA Form 861 – Annual Electric Power Industry Report](eia861.md).

With the EIA IDs we can link the hourly electricity demand to a particular geographic
region at the county level because utilities and balancing authorities report their
service territories in [core_eia861_\_yearly_service_territory](../data_dictionaries/pudl_db.md#core-eia861-yearly-service-territory). From that
information we estimate historical hourly electricity demand by state.

Plant operators reported in [core_eia860_\_scd_plants](../data_dictionaries/pudl_db.md#core-eia860-scd-plants) and generator ownership
information reported in [core_eia860_\_scd_ownership](../data_dictionaries/pudl_db.md#core-eia860-scd-ownership) are linked to
[core_eia860_\_scd_utilities](../data_dictionaries/pudl_db.md#core-eia860-scd-utilities) and [core_eia861_\_yearly_balancing_authority](../data_dictionaries/pudl_db.md#core-eia861-yearly-balancing-authority) and
can therefore be linked to the [core_ferc714_\_respondent_id](../data_dictionaries/pudl_db.md#core-ferc714-respondent-id) table.

### Who submits this data?

Electric utilities operating balancing authority areas and planning areas with annual
peak demand over 200MW are required to file FERC Form 714.

### What does the original data look like?

There are several epochs of FERC-714 and its predecessor data, published in various
formats:

* **1993-1999**: Data collected by NERC regions without standardized electronic filing.
* **1999-2004**: Data collected by FERC without standardized electronic filing.
* **2005**: Data collected by FERC without standardized electronic filing but not
  posted on their website and only available through the regulatory filing eLibrary.
* **2006-2020**: Standardized electronic filing. ASCII encoded CSV files exported from
  a VisualFoxPro database.
* **2021-present**: Standardized electronic filing using the XBRL (eXtensible Business
  Reporting Language) dialect of XML.

We only plan to integrate the data from the standardized electronic reporting era
(2006+) since the format of the earlier data varies for each reporting balancing authority
and utility, and would be very labor intensive to parse and reconcile.

## Notable Irregularities

### Timezone errors

The original hourly electricity demand time series is plagued with timezone and daylight
savings vs. standard time irregularities, which we have done our best to clean up. The
timestamps in the clean data are all in UTC, with a timezone code stored in a separate
column, so that the times can be easily localized or converted. It’s certainly not
perfect, but its much better than the original data and it’s easy to work with!

### Sign errors

Not all respondents use the same sign convention for reporting “demand.” The vast
majority consider demand / load that they serve to be a positive number, and so we’ve
standardized the data to use that convention.

### Reporting gaps

There are a lot of reporting gaps, especially for smaller respondents. Sometimes these
are brief, and sometimes they are entire years. There are also a number of outliers and
suspicious values (e.g. a long series of identical consecutive values). For the FERC-714
output tables published in PUDL, in addition to the original reported electricity
demand, we provide a column with anomalous and missing values replaced by imputed values
so the data can be more easily used in capacity expansion modeling and other
applications where having complete and plausible (if not exactly correct) values is
desirable. For an overview of that process and links to additional references, see
[Timeseries Imputation](../methodology/timeseries_imputation.md)

### Respondent-to-balancing-authority inconsistencies

Because utilities and balancing authorities occasionally change their service
territories or merge, the demand reported by any individual “respondent” may correspond
to wildly different consumers in different years. To make it at least somewhat possible
to compare the reported data across time, we’ve also compiled historical service
territory maps for the respondents based on data reported in [EIA Form 861 – Annual Electric Power Industry Report](eia861.md). However,
it’s not always easy to identify which EIA utility or balancing authority corresponds to
a FERC-714 respondent. See the `pudl.output.ferc714.Respondent` class for some
tooling that we’ve built to address this issue. Other code that underlies this work can
be found in [`pudl.analysis.service_territory`](../autoapi/pudl/analysis/service_territory/index.md#module-pudl.analysis.service_territory) and [`pudl.analysis.spatial`](../autoapi/pudl/analysis/spatial/index.md#module-pudl.analysis.spatial).

The [`pudl.analysis.state_demand`](../autoapi/pudl/analysis/state_demand/index.md#module-pudl.analysis.state_demand) script brings together all of the above to
estimate historical hourly electricity demand by state for 2006-2020.

### Combining XBRL and CSV data

The format of the company identifiers (CIDs) used in the CSV data (2006-2020) and the
XBRL data (2021+) differs. To link respondents between both data formats, we manually
map the IDs from both datasets and create a `respondent_id_ferc714` in
`pudl.package_data.glue.respondent_id_ferc714.csv`.

This CSV builds on the [migrated data](https://www.ferc.gov/filing-forms/eforms-refresh/migrated-data-downloads) provided
by FERC during the transition from CSV to XBRL data, which notes that:

> Companies that did not have a CID prior to the migration have been assigned a CID that
> begins with R, i.e., a temporary RID. These RIDs will be replaced in future with the
> accurate CIDs and new datasets will be published.

The file names of the migrated data (which correspond to CSV IDs) and the respondent
CIDs in the migrated files provide the basis for ID mapping. Though CIDs are intended to
be static, some of the CIDs in the migrated data weren’t found in the actual XBRL data,
and the same respondents were reporting data using different CIDs. To ensure accurate
record matching, we manually reviewed the CIDs for each respondent, matching based on
name and location. Some quirks to note:

* All respondents are matched 1:1 from CSV to XBRL data. Unmatched respondents mostly
  occur due to mergers, splits, acquisitions, and companies that no longer exist.
* Some CIDs assigned during the migration process do not appear in the data. Given the
  intention by FERC to make these CIDs permanent, they are still included in the mapping
  CSV in case these respondents re-appear. All temporary IDs (beginning with R) were
  removed.

## PUDL Data Transformations

To see the transformations applied to the data in each table, you can read the
docstrings for [`pudl.transform.ferc714`](../autoapi/pudl/transform/ferc714/index.md#module-pudl.transform.ferc714) created for each table’s
respective transform function.
