# pudl.transform.epamats

Module to perform data cleaning functions on EPA MATS data tables.

Several transformation steps are identical to those used for EPA CEMS data
(crosswalk-based plant ID harmonization, UTC conversion, plant UTC offset
loading, and crosswalk validation), so they are imported directly from
`pudl.transform.epacems` rather than duplicated here.

## Attributes

| [`logger`](#pudl.transform.epamats.logger)                 |                                                              |
|-------------------------------------------------------------------------|--------------------------------------------------------------|
| [`MATS_MEASUREMENT_CODES`](#pudl.transform.epamats.MATS_MEASUREMENT_CODES) | Mapping from raw MATS measurement codes to canonical values. |
| [`MEASUREMENT_CODE_COLS`](#pudl.transform.epamats.MEASUREMENT_CODE_COLS)  |                                                              |
| [`HF_COLUMNS`](#pudl.transform.epamats.HF_COLUMNS)             | HF emissions columns in the raw MATS data.                   |

## Functions

| [`_map_measurement_codes`](#pudl.transform.epamats._map_measurement_codes)(→ polars.LazyFrame)         | Map non-canonical measurement codes to canonical values.           |
|-----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| [`_replace_missing_placeholders`](#pudl.transform.epamats._replace_missing_placeholders)(→ polars.LazyFrame)  | Convert EPA's -1 placeholder for missing values into null.         |
| [`_validate_and_drop_hf_columns`](#pudl.transform.epamats._validate_and_drop_hf_columns)(→ polars.LazyFrame)  | Assert all HF columns are null, then drop them from the data.      |
| [`transform_epamats`](#pudl.transform.epamats.transform_epamats)(→ polars.LazyFrame)              | Transform EPA MATS hourly data and ready it for export to Parquet. |
| [`core_epamats__hourly_emissions`](#pudl.transform.epamats.core_epamats__hourly_emissions)(→ polars.LazyFrame) | Transform raw EPA MATS hourly emissions data and write to Parquet. |

## Module Contents

### pudl.transform.epamats.logger

### pudl.transform.epamats.MATS_MEASUREMENT_CODES *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]*

Mapping from raw MATS measurement codes to canonical values.

RAW values seen in the data: `Measured`, `Startup or Shutdown`,
`Unavailable`, `Manually Calculated`, `MEASURE`, `UNAVAIL`,
`UPDOWN`, or empty string.

### pudl.transform.epamats.MEASUREMENT_CODE_COLS *: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]* *= ['hg_mass_measurement_code', 'hcl_mass_measurement_code', 'hf_mass_measurement_code']*

### pudl.transform.epamats.HF_COLUMNS *: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]* *= ['hf_output_rate_lb_per_mwh', 'hf_input_rate_lb_per_mmbtu', 'hf_mass_lbs', 'hf_mass_measurement_code']*

HF emissions columns in the raw MATS data.

MATS does not require reporting of hydrogen fluoride (HF) emissions, so these
columns are expected to be entirely null. They are dropped from the core table;
see [`_validate_and_drop_hf_columns()`](#pudl.transform.epamats._validate_and_drop_hf_columns).

### pudl.transform.epamats.\_map_measurement_codes(lf: polars.LazyFrame) → polars.LazyFrame

Map non-canonical measurement codes to canonical values.

Codes not in the mapping (e.g. `Measured`, `Manually Calculated`) are
left unchanged. Empty strings are set to null.

* **Parameters:**
  **lf** – MATS hourly data as a Polars LazyFrame.
* **Returns:**
  The same data with measurement codes normalized.

### pudl.transform.epamats.\_replace_missing_placeholders(lf: polars.LazyFrame) → polars.LazyFrame

Convert EPA’s -1 placeholder for missing values into null.

EPA hourly emissions data reports -1 when gross load is unknown, rather
than leaving it blank. Standardize these as NA like other missing data so
downstream users don’t have to special-case them.

* **Parameters:**
  **lf** – MATS hourly data as a Polars LazyFrame.
* **Returns:**
  The same data with -1 placeholders replaced by null.

### pudl.transform.epamats.\_validate_and_drop_hf_columns(lf: polars.LazyFrame) → polars.LazyFrame

Assert all HF columns are null, then drop them from the data.

MATS does not require reporting of hourly hydrogen fluoride (HF) emissions,
and currently all four HF columns are entirely null in the raw data. Rather than
carrying them through the core table, we assert that they’re empty and then
drop them. If EPA ever starts reporting HF emissions, this assertion will
fail loudly and we can decide whether to keep the columns.

* **Parameters:**
  **lf** – MATS hourly data as a Polars LazyFrame.
* **Returns:**
  The same data, without the four all-null HF columns.

### pudl.transform.epamats.transform_epamats(raw_lf: polars.LazyFrame, core_epa_\_assn_eia_epacamd: polars.DataFrame, plant_utc_offset: polars.DataFrame) → polars.LazyFrame

Transform EPA MATS hourly data and ready it for export to Parquet.

* **Parameters:**
  * **raw_lf** – LazyFrame pointing to raw EPA MATS data.
  * **core_epa_\_assn_eia_epacamd** – EPA-EIA crosswalk DataFrame.
  * **plant_utc_offset** – Plant UTC offset DataFrame.
* **Returns:**
  A transformed LazyFrame of EPA MATS data.

### pudl.transform.epamats.core_epamats_\_hourly_emissions(context, raw_epamats_\_hourly_emissions: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_epa_\_assn_eia_epacamd_unique: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_entity_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → polars.LazyFrame

Transform raw EPA MATS hourly emissions data and write to Parquet.
