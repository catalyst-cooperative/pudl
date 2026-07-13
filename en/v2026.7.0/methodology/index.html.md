# Methodology

This section of the PUDL documentation describes our methodologies for more involved
data processing that are unique to PUDL, often affecting multiple tables or datasets.
For example:

* Estimating per-unit heat rates (thermal efficiency)
* Allocating reported fuel consumption and net generation to individual generators
* Estimating generator capacity factors
* Estimating CapEx and O&M costs by plant based on FERC Form 1 data
* Matching FERC & EIA plants and utilities
* Extending EIA’s boiler-generator association to cover more units
* Matching EIA Utilities and SEC Companies
* Estimating state-level hourly electricity demand based on overlapping utility service
  territories

It’s primarily intended to help end users of the data understand what went into making
the data, even if they aren’t digging into the code itself.  We’re just getting started
fleshing it out, in response to our 2025 PUDL User Survey.

* [Entity Resolution](entity_resolution.md)
  * [Overview](entity_resolution.md#overview)
  * [How Entity Resolution Works in PUDL](entity_resolution.md#how-entity-resolution-works-in-pudl)
  * [Interpreting Discrepancies](entity_resolution.md#interpreting-discrepancies)
  * [Improving PUDL Entity Resolution](entity_resolution.md#improving-pudl-entity-resolution)
  * [Related Tables And Source Documentation](entity_resolution.md#related-tables-and-source-documentation)
* [Timeseries Imputation](timeseries_imputation.md)
  * [Overview](timeseries_imputation.md#overview)
  * [How it Works](timeseries_imputation.md#how-it-works)
  * [Evaluating Imputation Performance](timeseries_imputation.md#evaluating-imputation-performance)
  * [Programming Interface (for developers)](timeseries_imputation.md#programming-interface-for-developers)
* [SEC 10-K Ownership Data Extraction Modeling](sec10k_modeling.md)
  * [Overview](sec10k_modeling.md#overview)
  * [Extracting Ownership Data From Exhibit 21 Attachments](sec10k_modeling.md#extracting-ownership-data-from-exhibit-21-attachments)
  * [Assigning `subsidiary_company_id_sec10k` to Extracted Subsidiary Companies](sec10k_modeling.md#assigning-subsidiary-company-id-sec10k-to-extracted-subsidiary-companies)
  * [Matching Subsidiary Companies to a Central Index Key](sec10k_modeling.md#matching-subsidiary-companies-to-a-central-index-key)
  * [Matching SEC Filing Companies to EIA Utilities](sec10k_modeling.md#matching-sec-filing-companies-to-eia-utilities)
  * [Matching SEC Subsidiary Companies to EIA Utilities](sec10k_modeling.md#matching-sec-subsidiary-companies-to-eia-utilities)
  * [Assumptions](sec10k_modeling.md#assumptions)
  * [Future Improvements](sec10k_modeling.md#future-improvements)
