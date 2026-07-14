# pudl.scripts.pudl_null_cols

A CLI tool for generating expect_column_not_all_null dbt test conditions.

## Attributes

| [`logger`](#pudl.scripts.pudl_null_cols.logger)         |    |
|---------------------------------------------------------|----|
| [`ALL_TABLES`](#pudl.scripts.pudl_null_cols.ALL_TABLES) |    |

## Functions

| [`max_eia860_year`](#pudl.scripts.pudl_null_cols.max_eia860_year)(→ int)                         | Get the maximum year available in the EIA-860 dataset.                         |
|--------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`date_column_to_year_expr`](#pudl.scripts.pudl_null_cols.date_column_to_year_expr)(date_column) | Convert a date column to a year extraction expression.                         |
| [`get_null_years`](#pudl.scripts.pudl_null_cols.get_null_years)(→ list[int])                     | Find years where a specific column is entirely null.                           |
| [`get_available_years`](#pudl.scripts.pudl_null_cols.get_available_years)(→ list[int])           | Generate a list of all years present in the named table.                       |
| [`infer_row_conditions`](#pudl.scripts.pudl_null_cols.infer_row_conditions)(→ dict[str, str])    | Analyze a single table for null columns and generate conditions.               |
| [`compact_row_condition`](#pudl.scripts.pudl_null_cols.compact_row_condition)(→ str)             | Generate a compact SQL condition that excludes entirely null years.            |
| [`main`](#pudl.scripts.pudl_null_cols.main)(→ int)                                               | Generate row_conditions for use with the expect_columns_not_all_null dbt test. |

## Module Contents

### pudl.scripts.pudl_null_cols.logger

### pudl.scripts.pudl_null_cols.ALL_TABLES

### pudl.scripts.pudl_null_cols.max_eia860_year() → [int](https://docs.python.org/3/library/functions.html#int)

Get the maximum year available in the EIA-860 dataset.

### pudl.scripts.pudl_null_cols.date_column_to_year_expr(date_column: [str](https://docs.python.org/3/library/stdtypes.html#str))

Convert a date column to a year extraction expression.

### pudl.scripts.pudl_null_cols.get_null_years(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), column: [str](https://docs.python.org/3/library/stdtypes.html#str), date_column: [str](https://docs.python.org/3/library/stdtypes.html#str), max_year: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]

Find years where a specific column is entirely null.

### pudl.scripts.pudl_null_cols.get_available_years(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), date_column: [str](https://docs.python.org/3/library/stdtypes.html#str), max_year: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]

Generate a list of all years present in the named table.

### pudl.scripts.pudl_null_cols.infer_row_conditions(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), date_column: [str](https://docs.python.org/3/library/stdtypes.html#str), max_year: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Analyze a single table for null columns and generate conditions.

### pudl.scripts.pudl_null_cols.compact_row_condition(null_years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], available_years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], date_column: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Generate a compact SQL condition that excludes entirely null years.

This function generates conditions that are compatible with the
expect_columns_not_all_null test, which automatically excludes recent years when
ignore_eia860m_nulls=true. Any column that’s entirely null across all available
years will result in an error – such columns need to be debugged, or explicitly
excluded from the data test. They may also be a sign that you’ve run the script
against incomplete output (e.g. the Fast ETL, not the Full ETL).

* **Parameters:**
  * **null_years** – List of years where the column is entirely null
  * **available_years** – List of all years present in the dataset
  * **date_column** – The date column to use for year extraction.
* **Returns:**
  A compact SQL condition string that works with the dbt test’s automatic year
  exclusion

### pudl.scripts.pudl_null_cols.main(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), ignore_eia860m: [bool](https://docs.python.org/3/library/functions.html#bool), date_column: [str](https://docs.python.org/3/library/stdtypes.html#str), max_year: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None)) → [int](https://docs.python.org/3/library/functions.html#int)

Generate row_conditions for use with the expect_columns_not_all_null dbt test.

While these row conditions will work out of the box in many cases, they need to be
reviewed and potentially adjusted to ensure they are appropriate for the
specific table and its data.
