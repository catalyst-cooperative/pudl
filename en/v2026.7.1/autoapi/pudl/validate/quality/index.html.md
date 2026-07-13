# pudl.validate.quality

Bespoke data quality checking utilities for PUDL data.

This module contains Python-implemented data quality checks that are either too
complex to express in SQL for dbt, or that need to be called from outside the Dagster
context. Add validation functions here when they are meant to be reused across multiple
asset checks, or when they don’t cleanly apply to an individual asset.

## Attributes

| [`logger`](#pudl.validate.quality.logger)   |    |
|---------------------------------------------|----|

## Exceptions

| [`ExcessiveNullRowsError`](#pudl.validate.quality.ExcessiveNullRowsError)   | Exception raised when rows have excessive null values.   |
|-----------------------------------------------------------------------------|----------------------------------------------------------|

## Functions

| [`no_null_rows`](#pudl.validate.quality.no_null_rows)(→ pandas.DataFrame)   | Check for rows with excessive missing values, usually due to a merge gone wrong.   |
|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------|
| [`weighted_quantile`](#pudl.validate.quality.weighted_quantile)(→ float)    | Calculate the weighted quantile of a Series or DataFrame column.                   |

## Module Contents

### pudl.validate.quality.logger

### *exception* pudl.validate.quality.ExcessiveNullRowsError(message: [str](https://docs.python.org/3/library/stdtypes.html#str), null_rows: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Bases: [`ValueError`](https://docs.python.org/3/library/exceptions.html#ValueError)

Exception raised when rows have excessive null values.

#### null_rows

### pudl.validate.quality.no_null_rows(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [str](https://docs.python.org/3/library/stdtypes.html#str) = 'all', df_name: [str](https://docs.python.org/3/library/stdtypes.html#str) = '', max_null_fraction: [float](https://docs.python.org/3/library/functions.html#float) = 0.9) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Check for rows with excessive missing values, usually due to a merge gone wrong.

Sum up the number of NA values in each row and the columns specified by `cols`.
If the NA values make up more than `max_null_fraction` of the columns overall, the
row is considered Null and the check fails.

* **Parameters:**
  * **df** – Table to check for null rows.
  * **cols** – Columns to check for excessive null value. If “all” check all columns.
  * **df_name** – Name of the dataframe, to aid in debugging/logging.
  * **max_null_fraction** – The maximum fraction of NA values allowed in any row.
* **Returns:**
  The input DataFrame, for use with DataFrame.pipe().
* **Raises:**
  * [**ExcessiveNullRowsError**](#pudl.validate.quality.ExcessiveNullRowsError) – If the fraction of NA values in any row is greater than
  * **max_null_fraction\`** – 

### pudl.validate.quality.weighted_quantile(data: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), weights: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), quantile: [float](https://docs.python.org/3/library/functions.html#float)) → [float](https://docs.python.org/3/library/functions.html#float)

Calculate the weighted quantile of a Series or DataFrame column.

This function allows us to take two columns from a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) one of
which contains an observed value (data) like heat content per unit of fuel, and the
other of which (weights) contains a quantity like quantity of fuel delivered which
should be used to scale the importance of the observed value in an overall
distribution, and calculate the values that the scaled distribution will have at
various quantiles.

* **Parameters:**
  * **data** – A series containing numeric data.
  * **weights** – Weights to use in scaling the data. Must have the same length as data.
  * **quantile** – A number between 0 and 1, representing the quantile at which we want
    to find the value of the weighted data.
* **Returns:**
  The value in the weighted data corresponding to the given quantile. If there are
  no values in the data, return `numpy.nan`.
