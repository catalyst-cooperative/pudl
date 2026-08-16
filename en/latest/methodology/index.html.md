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

* [Entity Resolution](entity_resolution.html.md)
  * [Overview](entity_resolution.html.md#overview)
  * [How Entity Resolution Works in PUDL](entity_resolution.html.md#how-entity-resolution-works-in-pudl)
  * [Interpreting Discrepancies](entity_resolution.html.md#interpreting-discrepancies)
  * [Improving PUDL Entity Resolution](entity_resolution.html.md#improving-pudl-entity-resolution)
  * [Related Tables And Source Documentation](entity_resolution.html.md#related-tables-and-source-documentation)
* [Timeseries Imputation](timeseries_imputation.html.md)
  * [Overview](timeseries_imputation.html.md#overview)
  * [How it Works](timeseries_imputation.html.md#how-it-works)
  * [Evaluating Imputation Performance](timeseries_imputation.html.md#evaluating-imputation-performance)
  * [Programming Interface (for developers)](timeseries_imputation.html.md#programming-interface-for-developers)
* [SEC 10-K Ownership Data Extraction Modeling](sec10k_modeling.html.md)
  * [Overview](sec10k_modeling.html.md#overview)
  * [Extracting Ownership Data From Exhibit 21 Attachments](sec10k_modeling.html.md#extracting-ownership-data-from-exhibit-21-attachments)
  * [Assigning `subsidiary_company_id_sec10k` to Extracted Subsidiary Companies](sec10k_modeling.html.md#assigning-subsidiary-company-id-sec10k-to-extracted-subsidiary-companies)
  * [Matching Subsidiary Companies to a Central Index Key](sec10k_modeling.html.md#matching-subsidiary-companies-to-a-central-index-key)
  * [Matching SEC Filing Companies to EIA Utilities](sec10k_modeling.html.md#matching-sec-filing-companies-to-eia-utilities)
  * [Matching SEC Subsidiary Companies to EIA Utilities](sec10k_modeling.html.md#matching-sec-subsidiary-companies-to-eia-utilities)
  * [Assumptions](sec10k_modeling.html.md#assumptions)
  * [Future Improvements](sec10k_modeling.html.md#future-improvements)
* [Generator Operational Characteristics](operational_characteristics.html.md)
  * [Overview](operational_characteristics.html.md#overview)
  * [Scope: EPA CEMS Units, Gross Generation](operational_characteristics.html.md#scope-epa-cems-units-gross-generation)
  * [A Rolling Window](operational_characteristics.html.md#a-rolling-window)
  * [Load Factor Bins](operational_characteristics.html.md#load-factor-bins)
  * [Minimum Stable Load](operational_characteristics.html.md#minimum-stable-load)
  * [Minimum Up and Down Time](operational_characteristics.html.md#minimum-up-and-down-time)
  * [Heat Rates](operational_characteristics.html.md#heat-rates)
  * [Ramp Rates](operational_characteristics.html.md#ramp-rates)
  * [Feedback Welcome](operational_characteristics.html.md#feedback-welcome)
