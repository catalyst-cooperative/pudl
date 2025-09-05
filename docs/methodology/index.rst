===============================================================================
Methodology
===============================================================================

This section of the PUDL documentation describes our methodologies for more involved
data processing that are unique to PUDL, often affecting multiple tables or datasets.
For example:

* Imputation of missing values in time series data
* Estimating per-unit heat rates (thermal efficiency)
* Allocating reported fuel consumption and net generation to individual generators
* Estimating generator capacity factors
* Estimating CapEx and O&M costs by plant based on FERC Form 1 data
* Matching FERC & EIA plants and utilities
* Extending EIA's boiler-generator association to cover more units
* Matching EIA Utilities and SEC Companies
* Reconciling values reported inconsistently across multiple EIA spreadsheets
* Estimating state-level hourly electricity demand based on overlapping utility service
  territories

It's primarily intended to help end users of the data understand what went into making
the data, even if they aren't digging into the code itself.  We're just getting started
fleshing it out, in response to our 2025 PUDL User Survey.

.. toctree::
  :maxdepth: 2

  timeseries_imputation
  sec10k_modeling
