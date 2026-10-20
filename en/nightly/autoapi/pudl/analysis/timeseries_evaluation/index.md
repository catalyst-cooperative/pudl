# pudl.analysis.timeseries_evaluation

Routines for evaluating and visualizing timeseries data.

Some types of evaluations we’d like to enable:

Scatter plot colored and/or down-selected by an ID or categorical column, comparing two
highly correlated time series. Allow user to select time range or ID/categorical to
plot.

Weighted histogram of a given timeseries variable. Allow selection by ID or categorical
column.

Line plot showing two different timeseries overlaid on each other. Allow zooming and
panning along the whole series. Color code values by a categorical column (imputation
codes).

Static reported vs. imputed values with color coded points for the imputations

## Functions

| [`_filter_df`](#pudl.analysis.timeseries_evaluation._filter_df)(→ pandas.DataFrame)                              | Filter a dataframe based on index columns and date range.               |
|--------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`extract_baseline_eia930_imputation`](#pudl.analysis.timeseries_evaluation.extract_baseline_eia930_imputation)(→ pandas.DataFrame)      | Download and extract an existing imputation of the EIA-930 demand data. |
| [`plot_correlation`](#pudl.analysis.timeseries_evaluation.plot_correlation)(df, timeseries_x, timeseries_y, idx_cols)  | Plot the correlation between two analogous time series.                 |
| [`plot_imputation`](#pudl.analysis.timeseries_evaluation.plot_imputation)(df, idx_cols, idx_vals, start_date, ...)    | Compare reported values with imputed values visually.                   |
| [`plot_compare_imputation`](#pudl.analysis.timeseries_evaluation.plot_compare_imputation)(df, idx_cols, idx_vals, ...[, ...]) | Plot two timeseries of the same information like demand for comparison. |

## Module Contents

### pudl.analysis.timeseries_evaluation.\_filter_df(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], idx_vals: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[Any, ...], start_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None) = None, end_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None) = None, time_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'datetime_utc') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Filter a dataframe based on index columns and date range.

### pudl.analysis.timeseries_evaluation.extract_baseline_eia930_imputation() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Download and extract an existing imputation of the EIA-930 demand data.

Useful as a baseline for evaluating our imputation results in development.
Originally by Tyler Ruggles, Alicia Wongel, and David Farnham (2025). See:
[https://doi.org/10.5281/zenodo.14768167](https://doi.org/10.5281/zenodo.14768167) (data)
and [https://doi.org/10.5281/zenodo.14768152](https://doi.org/10.5281/zenodo.14768152) (code).

### pudl.analysis.timeseries_evaluation.plot_correlation(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), timeseries_x: [str](https://docs.python.org/3/builtins/stdtypes.html#str), timeseries_y: [str](https://docs.python.org/3/builtins/stdtypes.html#str), idx_cols: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], idx_vals: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[Any, ...] | [str](https://docs.python.org/3/builtins/stdtypes.html#str)] | [None](https://docs.python.org/3/builtins/constants.html#None) = None, xylim: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[float](https://docs.python.org/3/builtins/functions.html#float), [float](https://docs.python.org/3/builtins/functions.html#float)] | [None](https://docs.python.org/3/builtins/constants.html#None) = None, xlabel: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = '', ylabel: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = '', title: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = '', time_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'datetime_utc', start_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None) = None, end_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str) | [None](https://docs.python.org/3/builtins/constants.html#None) = None, log: [bool](https://docs.python.org/3/builtins/functions.html#bool) = True, legend: [bool](https://docs.python.org/3/builtins/functions.html#bool) = True, alpha: [float](https://docs.python.org/3/builtins/functions.html#float) = 0.1)

Plot the correlation between two analogous time series.

### pudl.analysis.timeseries_evaluation.plot_imputation(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], idx_vals: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[Any, ...], start_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str), end_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str), reported_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str), imputed_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str), time_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'datetime_utc', ylabel: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'Demand [MWh]')

Compare reported values with imputed values visually.

Select a particular time series based on the ID columns and limit the data displayed
based on the provided start and end dates. Plot both the reported and imputed
values, color coding imputed values based on the reason for imputation.

### pudl.analysis.timeseries_evaluation.plot_compare_imputation(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)], idx_vals: [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[Any], start_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str), end_date: [str](https://docs.python.org/3/builtins/stdtypes.html#str), reported_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str), timeseries_a: [str](https://docs.python.org/3/builtins/stdtypes.html#str), timeseries_b: [str](https://docs.python.org/3/builtins/stdtypes.html#str), time_col: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'datetime_utc', ylabel: [str](https://docs.python.org/3/builtins/stdtypes.html#str) = 'Demand [MWh]')

Plot two timeseries of the same information like demand for comparison.
