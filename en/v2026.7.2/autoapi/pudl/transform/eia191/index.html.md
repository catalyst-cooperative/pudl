# pudl.transform.eia191

Transform EIA Form 191 monthly underground natural gas storage data.

## Attributes

| [`logger`](#pudl.transform.eia191.logger)   |    |
|---------------------------------------------|----|

## Functions

| [`core_eia191__monthly_gas_storage`](#pudl.transform.eia191.core_eia191__monthly_gas_storage)(→ pandas.DataFrame)   | Transform raw EIA-191 monthly underground natural gas storage data.   |
|---------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|

## Module Contents

### pudl.transform.eia191.logger

### pudl.transform.eia191.core_eia191_\_monthly_gas_storage(raw_eia191_\_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform raw EIA-191 monthly underground natural gas storage data.

One row per storage reservoir per month, identified by the EIA-assigned
reservoir id and report_date.

Transformations include:

* Combine `year` and `month` into a single `report_date` column.
* Normalize free-text string columns with
  [`pudl.helpers.simplify_strings()`](../../helpers/index.md#pudl.helpers.simplify_strings) (lower-case, strip whitespace,
  remove non-standard characters).
* Pass `base_gas_mcf`, `working_gas_capacity_mcf`, and
  `total_field_capacity_mcf` through as reported. These are operator
  self-reports under a loose EIA definition and are not additively
  consistent: `total` does not reliably equal `working + base` (~23%
  of rows differ). Do not assume additivity.

* **Parameters:**
  **raw_eia191_\_data** – Concatenated raw CSV data from the PUDL EIA-191
  Zenodo archive (RP8 monthly dataset, 2014–present).
* **Returns:**
  Cleaned monthly gas storage DataFrame with a combined report_date column.
