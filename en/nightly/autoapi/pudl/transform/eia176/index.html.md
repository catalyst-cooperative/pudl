# pudl.transform.eia176

Module to perform data cleaning functions on EIA176 data tables.

## Attributes

| [`logger`](#pudl.transform.eia176.logger)                                                                                     |    |
|-------------------------------------------------------------------------------------------------------------------------------|----|
| [`name_cleaner`](#pudl.transform.eia176.name_cleaner)                                                                         |    |
| [`DROP_OPERATING_STATES`](#pudl.transform.eia176.DROP_OPERATING_STATES)                                                       |    |
| [`CONTINUATION_LINES_ALLOWED_NON_SUBDIVISION_CODES`](#pudl.transform.eia176.CONTINUATION_LINES_ALLOWED_NON_SUBDIVISION_CODES) |    |
| [`UNKNOWN_TYPES`](#pudl.transform.eia176.UNKNOWN_TYPES)                                                                       |    |
| [`SUPPLEMENTAL_GASEOUS_FUEL_TYPE_MAP`](#pudl.transform.eia176.SUPPLEMENTAL_GASEOUS_FUEL_TYPE_MAP)                             |    |
| [`OTHER_DISPOSITION_TYPE_MAP`](#pudl.transform.eia176.OTHER_DISPOSITION_TYPE_MAP)                                             |    |

## Functions

| [`_core_eia176__numeric_data`](#pudl.transform.eia176._core_eia176__numeric_data)(→ tuple[dagster.Output, ...)                                 | Process EIA 176 custom report data into company and aggregate outputs.                                        |
|------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| [`get_wide_table`](#pudl.transform.eia176.get_wide_table)(→ pandas.DataFrame)                                                                  | Take a 'long' or entity-attribute-value table and return a wide table with one column per attribute/variable. |
| [`_subdivision_code_map`](#pudl.transform.eia176._subdivision_code_map)(→ pandas.Series)                                                       | Map subdivision names and codes to canonical two-letter codes.                                                |
| [`normalize_continuation_line_location_codes`](#pudl.transform.eia176.normalize_continuation_line_location_codes)(...)                         | Validate and normalize EIA-176 continuation line location codes.                                              |
| [`_get_continuation_code_type`](#pudl.transform.eia176._get_continuation_code_type)(→ pandas.Series)                                           | Classify EIA-176 continuation codes as subnational or national/other codes.                                   |
| [`_find_continuation_line_total_mismatches`](#pudl.transform.eia176._find_continuation_line_total_mismatches)(...)                             | Compare detailed continuation line totals with reported company-level totals.                                 |
| [`validate_totals`](#pudl.transform.eia176.validate_totals)(→ dagster.AssetCheckResult)                                                        | Compare reported and calculated totals for different geographical aggregates.                                 |
| [`core_eia176__yearly_gas_disposition_by_consumer`](#pudl.transform.eia176.core_eia176__yearly_gas_disposition_by_consumer)(...)               | Produce annual company-level gas disposition by consumer class (EIA-176).                                     |
| [`core_eia176__yearly_gas_imports`](#pudl.transform.eia176.core_eia176__yearly_gas_imports)(→ pandas.DataFrame)                                | Produce company-level detailed annual gas imports (EIA-176, Line 3.0).                                        |
| [`core_eia176__yearly_supplemental_gaseous_fuel_supplies`](#pudl.transform.eia176.core_eia176__yearly_supplemental_gaseous_fuel_supplies)(...) | Produce detailed annual supplemental gaseous fuel supplies (EIA-176, Line 6.0).                               |
| [`core_eia176__yearly_gas_exports`](#pudl.transform.eia176.core_eia176__yearly_gas_exports)(→ pandas.DataFrame)                                | Produce detailed annual out-of-state gas deliveries (EIA-176, Line 14.0).                                     |
| [`core_eia176__yearly_gas_disposition_other`](#pudl.transform.eia176.core_eia176__yearly_gas_disposition_other)(...)                           | Produce detailed annual gas disposition to other uses (EIA-176, Line 18.4).                                   |
| [`core_eia176__yearly_gas_supply`](#pudl.transform.eia176.core_eia176__yearly_gas_supply)(→ pandas.DataFrame)                                  | Produce company-level natural and supplemental gas supply (EIA176, Lines 1.0-7.0).                            |
| [`_compare_eia176_continuation_line_total`](#pudl.transform.eia176._compare_eia176_continuation_line_total)(→ pandas.DataFrame)                | Compare a wide EIA-176 value column against summed continuation-line values.                                  |
| [`core_eia176__yearly_gas_disposition`](#pudl.transform.eia176.core_eia176__yearly_gas_disposition)(→ pandas.DataFrame)                        | Produce company-level gas disposition (EIA176, Lines 9.0 and 12.0-20.0).                                      |
| [`core_eia176__yearly_liquefied_natural_gas_inventory`](#pudl.transform.eia176.core_eia176__yearly_liquefied_natural_gas_inventory)(...)       | Operator's LNG storage volume and capacity (EIA176, lines 8.0-8.2).                                           |
| [`_normalize_operating_states`](#pudl.transform.eia176._normalize_operating_states)(...[, column])                                             | Map full state names to their postal abbreviations.                                                           |

## Module Contents

### pudl.transform.eia176.logger

### pudl.transform.eia176.name_cleaner

### pudl.transform.eia176.DROP_OPERATING_STATES *= ('fed. gulf of mexico', 'mexico', 'other')*

### pudl.transform.eia176.CONTINUATION_LINES_ALLOWED_NON_SUBDIVISION_CODES

### pudl.transform.eia176.UNKNOWN_TYPES *= ('', '.', '0', 'na', 'nan', 'n/a', 'n.a.', 'none', 'not applicable', 'not available')*

### pudl.transform.eia176.SUPPLEMENTAL_GASEOUS_FUEL_TYPE_MAP

### pudl.transform.eia176.OTHER_DISPOSITION_TYPE_MAP

### pudl.transform.eia176.\_core_eia176_\_numeric_data(raw_eia176_\_numeric_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[dagster.Output](https://docs.dagster.io/api/dagster/ops/#dagster.Output), [dagster.Output](https://docs.dagster.io/api/dagster/ops/#dagster.Output)]

Process EIA 176 custom report data into company and aggregate outputs.

Take raw dataframe produced by querying all forms from the EIA 176 custom report
and return two wide tables with primary keys and one column per variable.

One table with data for each year and company, one with state- and US-level
aggregates per year.

### pudl.transform.eia176.get_wide_table(long_table: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), primary_key: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Take a ‘long’ or entity-attribute-value table and return a wide table with one column per attribute/variable.

### pudl.transform.eia176.\_subdivision_code_map(core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Map subdivision names and codes to canonical two-letter codes.

### pudl.transform.eia176.normalize_continuation_line_location_codes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), column: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Validate and normalize EIA-176 continuation line location codes.

Values matching `core_pudl__codes_subdivisions` by name or code are converted
to canonical two-letter state, province, or territory codes. Known EIA
continuation line codes that are not state, province, or territory codes are
allowed through unchanged so unexpected values still fail loudly. These allowed
values are raw EIA `REF_CODE` values, not standardized ISO country codes.

### pudl.transform.eia176.\_get_continuation_code_type(continuation_code: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Classify EIA-176 continuation codes as subnational or national/other codes.

### pudl.transform.eia176.\_find_continuation_line_total_mismatches(detail_records: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), company_total_column: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compare detailed continuation line totals with reported company-level totals.

### pudl.transform.eia176.validate_totals(\_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia176_\_yearly_aggregate_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [dagster.AssetCheckResult](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetCheckResult)

Compare reported and calculated totals for different geographical aggregates.

EIA reports an adjustment company at the area-level, so these values are expected to
be identical.  Once we validate this, we can preserve the detailed data and discard
the aggregate data to remove duplicate information.

### pudl.transform.eia176.core_eia176_\_yearly_gas_disposition_by_consumer(\_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce annual company-level gas disposition by consumer class (EIA-176).

Transforms company-level EIA-176 data into a normalized table with one row per
(“report_year”, “operator_id_eia”, “operating_state”, “customer_class”, “revenue_class”)
and three value columns: “consumers”, “revenue”, and “volume_mcf”.

Processing:

* Select sales and transport metrics for residential, commercial, industrial,
  electric_power, vehicle_fuel, and other.
* Validate that `sales_volume + transport_volume == total volume` per customer
  class.
* Normalize `operating_state` to two-letter subdivision codes via
  `core_pudl__codes_subdivisions`; drop rows with unknown states (these rows must
  contain zeros across value columns).
* Drop rows where all of `consumers`/`revenue`/`volume_mcf` are NULL.

* **Parameters:**
  * **\_core_eia176_\_yearly_company_data** – Wide company-level EIA-176 data with
    per-metric columns.
  * **core_pudl_\_codes_subdivisions** – Mapping from `subdivision_name` to
    `subdivision_code` used to normalize `operating_state`.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If component volumes don’t sum to totals, or if rows with
  unknown `operating_state` contain non-zero values.

### Notes

- `volume_mcf` is thousand cubic feet (MCF).
- `consumers` is a count; `revenue` is nominal USD as reported.
- `customer_class` and `revenue_class` are returned as categoricals.

### pudl.transform.eia176.core_eia176_\_yearly_gas_imports(raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce company-level detailed annual gas imports (EIA-176, Line 3.0).

### pudl.transform.eia176.core_eia176_\_yearly_supplemental_gaseous_fuel_supplies(raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce detailed annual supplemental gaseous fuel supplies (EIA-176, Line 6.0).

### pudl.transform.eia176.core_eia176_\_yearly_gas_exports(raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce detailed annual out-of-state gas deliveries (EIA-176, Line 14.0).

### pudl.transform.eia176.core_eia176_\_yearly_gas_disposition_other(raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce detailed annual gas disposition to other uses (EIA-176, Line 18.4).

### pudl.transform.eia176.core_eia176_\_yearly_gas_supply(\_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce company-level natural and supplemental gas supply (EIA176, Lines 1.0-7.0).

### pudl.transform.eia176.\_compare_eia176_continuation_line_total(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), , line: [int](https://docs.python.org/3/library/functions.html#int), value_col: [str](https://docs.python.org/3/library/stdtypes.html#str), continuation_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compare a wide EIA-176 value column against summed continuation-line values.

### pudl.transform.eia176.core_eia176_\_yearly_gas_disposition(\_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia176_\_continuation_text_lines: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce company-level gas disposition (EIA176, Lines 9.0 and 12.0-20.0).

### pudl.transform.eia176.core_eia176_\_yearly_liquefied_natural_gas_inventory(\_core_eia176_\_yearly_company_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_pudl_\_codes_subdivisions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Operator’s LNG storage volume and capacity (EIA176, lines 8.0-8.2).

### pudl.transform.eia176.\_normalize_operating_states(core_pudl_\_codes_subdivisions, df, column: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'operating_state')

Map full state names to their postal abbreviations.

This uses the latest year of Census PEP data as the reference. If a full state name is not included in this data, it is set to NA.
