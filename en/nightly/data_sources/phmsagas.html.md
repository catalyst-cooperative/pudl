# Pipelines and Hazardous Materials Safety Administration (PHMSA) Annual Natural Gas Report

| Source URL                      | [https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids)                                                              |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Source Description              | Annual reports submitted to PHMSA from gas distribution, gas gathering, gas transmission, liquefied natural gas, and underground gas storage system operators. Annual reports include information such as total pipeline mileage, facilities, commodities transported, miles by material, and installation dates. |
| Download Size                   | 220 MB                                                                                                                                                                                                                                                                                                            |
| Temporal Coverage               | 1970-2024                                                                                                                                                                                                                                                                                                         |
| PUDL Code                       | `phmsagas`                                                                                                                                                                                                                                                                                                        |
| Unprocessed Source Data Archive | [10.5281/zenodo.7683351](https://doi.org/10.5281/zenodo.7683351)                                                                                                                                                                                                                                                  |
| Issues                          | [Open Pipelines and Hazardous Materials Safety Administration (PHMSA) Annual Natural Gas Report issues](https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aphmsagas)                                                                                               |

## PUDL Database Tables

We’ve segmented the processed data into the following normalized data tables.
Clicking on the links will show you a description of the table as well as
the names and descriptions of each of its fields.

| Data Dictionary                                                                                                                                        | Browse Online                                                                                                                                                                                         |
|--------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [\_core_phmsagas_\_yearly_distribution_by_install_decade](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-by-install-decade)       | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_by_install_decade](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_by_install_decade)       |
| [\_core_phmsagas_\_yearly_distribution_by_material](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-by-material)                   | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_by_material](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_by_material)                   |
| [\_core_phmsagas_\_yearly_distribution_by_material_and_size](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-by-material-and-size) | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_by_material_and_size](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_by_material_and_size) |
| [\_core_phmsagas_\_yearly_distribution_excavation_damages](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-excavation-damages)     | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_excavation_damages](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_excavation_damages)     |
| [\_core_phmsagas_\_yearly_distribution_filings](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-filings)                           | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_filings](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_filings)                           |
| [\_core_phmsagas_\_yearly_distribution_leaks](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-leaks)                               | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_leaks](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_leaks)                               |
| [\_core_phmsagas_\_yearly_distribution_misc](../data_dictionaries/pudl_db.md#i-core-phmsagas-yearly-distribution-misc)                                 | [https://data.catalyst.coop/preview/pudl/_core_phmsagas_\_yearly_distribution_misc](https://data.catalyst.coop/preview/pudl/_core_phmsagas__yearly_distribution_misc)                                 |
| [core_phmsagas_\_yearly_distribution_operators](../data_dictionaries/pudl_db.md#core-phmsagas-yearly-distribution-operators)                           | [https://data.catalyst.coop/preview/pudl/core_phmsagas_\_yearly_distribution_operators](https://data.catalyst.coop/preview/pudl/core_phmsagas__yearly_distribution_operators)                         |

## Background

The PHMSA Natural Gas Annual Report, published by the Pipeline and Hazardous Materials
Safety Administration (part of the US Dept. of Transportation), collects data about
natural gas gathering and transmission and distribution systems (including their age,
length, diameter, materials, and carrying capacity). PHMSA also has information about
natural gas storage facilities and liquefied natural gas shipping facilities.

There are six different forms used by PHMSA. To begin, PUDL will focus on integrating
the transmission and distribution data, which is available from 1970 to the present.
For more details, see the [official PHMSA data page](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids).

### Download additional documentation

Documentation for the gas distribution form:

* [`Gas Distribution Annual Form - Phmsa F7100.1-1 (2005) - Data Fields (PDF/TXT)`](phmsagas/Gas Distribution Annual Form - PHMSA F7100.1-1 (2005) - Data Fields.pdf)
* [`Gas Distribution Annual Form - Phmsa F7100.1-1 (2021) - Data Fields (PDF/TXT)`](phmsagas/Gas Distribution Annual Form - PHMSA F7100.1-1 (2021) - Data fields.pdf)
* [`Gas Distribution Annual Form - Rspa F7100.1-1 (1985) - Data Fields (PDF/TXT)`](phmsagas/Gas Distribution Annual Form - RSPA F7100.1-1 (1985) - Data fields.txt)

Documentation for the gas transmission and gathering form:

* [`Gas Transmission And Gathering Pipeline Annual Form - Phmsa F7100.2-1 (2005) - Data Fields (PDF/TXT)`](phmsagas/Gas Transmission and Gathering Pipeline Annual Form - PHMSA F7100.2-1 (2005) - Data Fields.pdf)
* [`Gas Transmission And Gathering Pipeline Annual Form - Phmsa F7100.2-1 (2005) - Data Fields Not On The Form (PDF/TXT)`](phmsagas/Gas Transmission and Gathering Pipeline Annual Form - PHMSA F7100.2-1 (2005) - Data fields not on the form.txt)
* [`Gas Transmission And Gathering Pipeline Annual Form - Phmsa F7100.2-1 (2014) - Data Fields (PDF/TXT)`](phmsagas/Gas Transmission and Gathering Pipeline Annual Form - PHMSA F7100.2-1 (2014) - Data Fields.pdf)
* [`Gas Transmission And Gathering Pipeline Annual Form - Phmsa F7100.2-1 (2022) - Data Fields (PDF/TXT)`](phmsagas/Gas Transmission and Gathering Pipeline Annual Form - PHMSA F7100.2-1 (2022) - Data Fields.pdf)
* [`Gas Transmission And Gathering Pipeline Annual Form - Rspa F7100.2-1 (1985) - Data Fields (PDF/TXT)`](phmsagas/Gas Transmission and Gathering Pipeline Annual Form - RSPA F7100.2-1 (1985) - Data fields.txt)

### Data available through PUDL

PHMSA data goes back to 1970 and is formatted as Microsoft Excel spreadsheets. To begin,
PUDL will focus on integrating tables from the distribution and transmission forms, from
1990-present. Data prior to 1989 and from other tables will be integrated as funding
allows.

### Who submits this data?

The Code of Federal Regulations (49 CFR Parts 191, 195) requires operators of gas
distribution, gas gathering, gas transmission, hazardous liquid, LNG, and UNGS
to submit annual reports to PHMSA. For further details, see the PHMSA’s [pipeline safety
regulations](https://www.ecfr.gov/current/title-49/subtitle-B/chapter-I/subchapter-D).

### What does the original data look like?

PHMSA typically publishes both CSV and Excel spreadsheets for each form once a year. The
content of the spreadsheets varies from year to year as the questions in the form are
updated, with new questions and parts of the form added over time. For some sections of
the form, respondents must respond to each question one time per commodity and/or state
of operation, meaning that different form sections have different primary keys. Older
data maybe be revised after publication through the filing of a supplementary report. To
ensure reproducible analyses, we archive [versioned snapshots of the PHSMA data on
Zenodo](https://doi.org/10.5281/zenodo.7683351). These archives are
periodically refreshed with new data from the [PHMSA website](https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids).

To understand the details of how the form and data have evolved over time, we recommend
reading the Form Instructions from different years, linked above.

## Notable Irregularities

At this moment, we are still in the early stages of cleaning and integrating PHMSA data
into PUDL. This section will be updated as we learn more about the particularities of
this dataset.

## PUDL Data Transformations

To see the transformations applied to the data in each table, you can read the
docstrings for [`pudl.transform.phmsagas`](../autoapi/pudl/transform/phmsagas/index.md#module-pudl.transform.phmsagas) created for each table’s
respective transform function.
