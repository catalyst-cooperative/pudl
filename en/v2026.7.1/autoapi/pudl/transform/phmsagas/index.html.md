# pudl.transform.phmsagas

Classes & functions to process PHMSA natural gas data before loading into the PUDL DB.

## Attributes

| [`logger`](#pudl.transform.phmsagas.logger)                                                               |    |
|-----------------------------------------------------------------------------------------------------------|----|
| [`YEARLY_DISTRIBUTION_FILING_COLUMNS`](#pudl.transform.phmsagas.YEARLY_DISTRIBUTION_FILING_COLUMNS)       |    |
| [`YEARLY_DISTRIBUTION_OPERATORS_COLUMNS`](#pudl.transform.phmsagas.YEARLY_DISTRIBUTION_OPERATORS_COLUMNS) |    |
| [`YEARLY_DISTRIBUTION_MISC_COLUMNS`](#pudl.transform.phmsagas.YEARLY_DISTRIBUTION_MISC_COLUMNS)           |    |
| [`YEARLY_DISTRIBUTION_IDX_ISH`](#pudl.transform.phmsagas.YEARLY_DISTRIBUTION_IDX_ISH)                     |    |
| [`MELT_PATTERNS`](#pudl.transform.phmsagas.MELT_PATTERNS)                                                 |    |

## Functions

| [`_check_all_raw_columns_being_transformed`](#pudl.transform.phmsagas._check_all_raw_columns_being_transformed)(raw_df)                              | Check to ensure that we are transforming all of the raw columns.                        |
|------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| [`_dedupe_year_distribution_idx`](#pudl.transform.phmsagas._dedupe_year_distribution_idx)(→ pandas.DataFrame)                                        | Remove the rare duplicates in the expected primary key.                                 |
| [`_assign_cols_from_patterns`](#pudl.transform.phmsagas._assign_cols_from_patterns)(→ pandas.DataFrame)                                              | Add new columns based on regex patterns within an existing column.                      |
| [`backfill_zero_operator_id_phmsa`](#pudl.transform.phmsagas.backfill_zero_operator_id_phmsa)(→ pandas.DataFrame)                                    | Backfill some of the 0's in the operator_id_phmsa.                                      |
| [`_core_phmsagas__yearly_distribution`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution)(→ pandas.DataFrame)                            | Clean up the raw yearly distribution table for future transforms.                       |
| [`_melt_col_pattern`](#pudl.transform.phmsagas._melt_col_pattern)(df, filter_pattern, value_name, ...)                                               | Melt a dataframe based on a filter regex pattern and assign pattern columns.            |
| [`_melt_merge_main_services`](#pudl.transform.phmsagas._melt_merge_main_services)(→ pandas.DataFrame)                                                | Filter, melt, add columns then merge miles of main and service.                         |
| [`_check_and_drop_log_if_always_in_report_id`](#pudl.transform.phmsagas._check_and_drop_log_if_always_in_report_id)(df)                              | Check to ensure we can drop the log column w/o losing information.                      |
| [`_core_phmsagas__yearly_distribution_filings`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_filings)(...)                           | Transform information about filings (with PK report_id).                                |
| [`core_phmsagas__yearly_distribution_operators`](#pudl.transform.phmsagas.core_phmsagas__yearly_distribution_operators)(...)                         | Pull and transform the yearly distribution PHMSA data into operator-level data.         |
| [`_core_phmsagas__yearly_distribution_by_material`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_by_material)(...)                   | Transform the \_core table of the miles of main and services by material.               |
| [`_core_phmsagas__yearly_distribution_by_install_decade`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_by_install_decade)(...)       | Transform the \_core table of the miles of main and services by decade.                 |
| [`_core_phmsagas__yearly_distribution_by_material_and_size`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_by_material_and_size)(...) | Transform the \_core table of the miles of main and services by material type and size. |
| [`_core_phmsagas__yearly_distribution_leaks`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_leaks)(...)                               | Transform table of leaks - broken out by source and leak severity.                      |
| [`_core_phmsagas__yearly_distribution_excavation_damages`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_excavation_damages)(...)     | Transform table of damages - broken out by type and sub-type.                           |
| [`_core_phmsagas__yearly_distribution_misc`](#pudl.transform.phmsagas._core_phmsagas__yearly_distribution_misc)(...)                                 | Transform this distribution table of miscellaneous numeric values.                      |

## Module Contents

### pudl.transform.phmsagas.logger

### pudl.transform.phmsagas.YEARLY_DISTRIBUTION_FILING_COLUMNS *= ['report_id', 'log_number', 'operator_id_phmsa', 'report_date', 'operating_state',...*

### pudl.transform.phmsagas.YEARLY_DISTRIBUTION_OPERATORS_COLUMNS

### pudl.transform.phmsagas.YEARLY_DISTRIBUTION_MISC_COLUMNS *= ['all_known_leaks_scheduled_for_repair_main', 'all_known_leaks_scheduled_for_repair',...*

### pudl.transform.phmsagas.YEARLY_DISTRIBUTION_IDX_ISH *= ['report_date', 'report_id', 'operator_id_phmsa', 'commodity', 'operating_state']*

### pudl.transform.phmsagas.MELT_PATTERNS

### pudl.transform.phmsagas.\_check_all_raw_columns_being_transformed(raw_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Check to ensure that we are transforming all of the raw columns.

Because we are using a lot of regex patterns to identify the columns
to transform into various tables, we run this check to make sure the
we are actually finding all of the raw columns. If a column is flagged
here, check the column mapping and the patterns in MELT_PATTERNS.1

### pudl.transform.phmsagas.\_dedupe_year_distribution_idx(raw_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Remove the rare duplicates in the expected primary key.

There are 51 found records which have duplicate values for the expected
primary key of this table. Many manipulations are much easier when we have a
unique primary key - merges for instance! So we want to remove these duplicates.

On visual inspection, these duplicates either look mostly the same or have one
record with most or all of the non-null or non-zero values. Therefore, we sort
the total columns and then drop duplicates so we kept the records which have the
most information. This is not the most robust method of de-duplicating but there
are so few records compared to the >90k total records.

Then there are a few extra fun records that are still duplicated. These all have
the know null-like operator_id_phmsa of 0 so we are going to drop them.

### pudl.transform.phmsagas.\_assign_cols_from_patterns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), col_patterns: [dict](https://docs.python.org/3/library/stdtypes.html#dict), pattern_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add new columns based on regex patterns within an existing column.

* **Parameters:**
  * **df** – the dataframe with `pattern_col`
  * **col_patterns** – dictionary with new column name (keys) and the regex
    pattern found within `pattern_col` to extract into the new
    column (values)
  * **pattern_col** – name of column to extract the patterns from.

### pudl.transform.phmsagas.backfill_zero_operator_id_phmsa(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Backfill some of the 0’s in the operator_id_phmsa.

We are trying to backfill PHMSA’s version of null operator_id_phmsa’s which is 0.
These 0’s show up particularly in the pre-1990’s years of data. It would be ideal
to figure out ways to confidently replace all of the 0’s, but for now we are only
replacing 0’s when the operator_name_phmsa is exactly the same.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution(raw_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean up the raw yearly distribution table for future transforms.

This function mostly deduplicates the records on the core primary key.

### pudl.transform.phmsagas.\_melt_col_pattern(df, filter_pattern, value_name, col_patterns)

Melt a dataframe based on a filter regex pattern and assign pattern columns.

### pudl.transform.phmsagas.\_melt_merge_main_services(cleaned_raw: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), main_pattern: [str](https://docs.python.org/3/library/stdtypes.html#str), services_pattern: [str](https://docs.python.org/3/library/stdtypes.html#str), col_patterns: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Filter, melt, add columns then merge miles of main and service.

### pudl.transform.phmsagas.\_check_and_drop_log_if_always_in_report_id(df)

Check to ensure we can drop the log column w/o losing information.

The log column is only reported for a few years and we assume its
duplicative because its just the suffix of report_id. This function
checks that assumption and then deletes the log column.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_filings(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform information about filings (with PK report_id).

### pudl.transform.phmsagas.core_phmsagas_\_yearly_distribution_operators(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the yearly distribution PHMSA data into operator-level data.

Transformations include:

* Standardize NAs.
* Strip blank spaces around string values.
* Convert specific columns to integers.
* Standardize address columns.
* Standardize phone and fax numbers.

* **Parameters:**
  **raw_phmsagas_\_yearly_distribution** – The raw `raw_phmsagas__yearly_distribution` dataframe.
* **Returns:**
  Transformed `core_phmsagas__yearly_distribution_operators` dataframe.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_by_material(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the \_core table of the miles of main and services by material.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_by_install_decade(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the \_core table of the miles of main and services by decade.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_by_material_and_size(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the \_core table of the miles of main and services by material type and size.

This table represents the bulk of the wide raw columns, which means it ends up being
nearly 8 million records. This transform includes the standard
`_melt_merge_main_services` as well as adding in a column describing the “other”
material type (`main_other_material_detail`).

This table takes by far the longest to generate because of how large it is.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_leaks(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform table of leaks - broken out by source and leak severity.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_excavation_damages(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform table of damages - broken out by type and sub-type.

### pudl.transform.phmsagas.\_core_phmsagas_\_yearly_distribution_misc(\_core_phmsagas_\_yearly_distribution: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform this distribution table of miscellaneous numeric values.
