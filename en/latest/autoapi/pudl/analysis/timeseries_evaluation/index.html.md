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

| [`_filter_df`](#pudl.analysis.timeseries_evaluation._filter_df)(→ pandas.DataFrame)                                                 | Filter a dataframe based on index columns and date range.               |
|-------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`extract_baseline_eia930_imputation`](#pudl.analysis.timeseries_evaluation.extract_baseline_eia930_imputation)(→ pandas.DataFrame) | Download and extract an existing imputation of the EIA-930 demand data. |
| [`plot_correlation`](#pudl.analysis.timeseries_evaluation.plot_correlation)(df, timeseries_x, timeseries_y, idx_cols)               | Plot the correlation between two analogous time series.                 |
| [`plot_imputation`](#pudl.analysis.timeseries_evaluation.plot_imputation)(df, idx_cols, idx_vals, start_date, ...)                  | Compare reported values with imputed values visually.                   |
| [`plot_compare_imputation`](#pudl.analysis.timeseries_evaluation.plot_compare_imputation)(df, idx_cols, idx_vals, ...[, ...])       | Plot two timeseries of the same information like demand for comparison. |
| [`encode_run_length`](#pudl.analysis.timeseries_evaluation.encode_run_length)(→ tuple[numpy.ndarray, numpy.ndarray])                | Encode vector with run-length encoding.                                 |
| [`insert_run_length`](#pudl.analysis.timeseries_evaluation.insert_run_length)(→ numpy.ndarray)                                      | Insert run-length encoded values into a vector.                         |
| [`summarize_flags`](#pudl.analysis.timeseries_evaluation.summarize_flags)(→ pandas.DataFrame)                                       | Summarize flagged values by flag, count and median.                     |
| [`plot_flags`](#pudl.analysis.timeseries_evaluation.plot_flags)(→ None)                                                             | Plot cleaned series and anomalous values colored by flag.               |
| [`simulate_nulls`](#pudl.analysis.timeseries_evaluation.simulate_nulls)(→ numpy.ndarray)                                            | Find non-null values to null to match a run-length distribution.        |

## Module Contents

### pudl.analysis.timeseries_evaluation.\_filter_df(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], idx_vals: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[Any], start_date: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, end_date: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, time_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'datetime_utc') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Filter a dataframe based on index columns and date range.

### pudl.analysis.timeseries_evaluation.extract_baseline_eia930_imputation() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Download and extract an existing imputation of the EIA-930 demand data.

Useful as a baseline for evaluating our imputation results in development.
Originally by Tyler Ruggles, Alicia Wongel, and David Farnham (2025). See:
[https://doi.org/10.5281/zenodo.14768167](https://doi.org/10.5281/zenodo.14768167) (data)
and [https://doi.org/10.5281/zenodo.14768152](https://doi.org/10.5281/zenodo.14768152) (code).

### pudl.analysis.timeseries_evaluation.plot_correlation(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), timeseries_x: [str](https://docs.python.org/3/library/stdtypes.html#str), timeseries_y: [str](https://docs.python.org/3/library/stdtypes.html#str), idx_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], idx_vals: [list](https://docs.python.org/3/library/stdtypes.html#list)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[Any] | [str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None, xylim: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[float](https://docs.python.org/3/library/functions.html#float)] | [None](https://docs.python.org/3/library/constants.html#None) = None, xlabel: [str](https://docs.python.org/3/library/stdtypes.html#str) = '', ylabel: [str](https://docs.python.org/3/library/stdtypes.html#str) = '', title: [str](https://docs.python.org/3/library/stdtypes.html#str) = '', time_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'datetime_utc', start_date: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, end_date: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, log: [bool](https://docs.python.org/3/library/functions.html#bool) = True, legend: [bool](https://docs.python.org/3/library/functions.html#bool) = True, alpha: [float](https://docs.python.org/3/library/functions.html#float) = 0.1)

Plot the correlation between two analogous time series.

### pudl.analysis.timeseries_evaluation.plot_imputation(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], idx_vals: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[Any], start_date: [str](https://docs.python.org/3/library/stdtypes.html#str), end_date: [str](https://docs.python.org/3/library/stdtypes.html#str), reported_col: [str](https://docs.python.org/3/library/stdtypes.html#str), imputed_col: [str](https://docs.python.org/3/library/stdtypes.html#str), time_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'datetime_utc', ylabel: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'Demand [MWh]')

Compare reported values with imputed values visually.

Select a particular time series based on the ID columns and limit the data displayed
based on the provided start and end dates. Plot both the reported and imputed
values, color coding imputed values based on the reason for imputation.

### pudl.analysis.timeseries_evaluation.plot_compare_imputation(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], idx_vals: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[Any], start_date: [str](https://docs.python.org/3/library/stdtypes.html#str), end_date: [str](https://docs.python.org/3/library/stdtypes.html#str), reported_col: [str](https://docs.python.org/3/library/stdtypes.html#str), timeseries_a: [str](https://docs.python.org/3/library/stdtypes.html#str), timeseries_b: [str](https://docs.python.org/3/library/stdtypes.html#str), time_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'datetime_utc', ylabel: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'Demand [MWh]')

Plot two timeseries of the same information like demand for comparison.

### pudl.analysis.timeseries_evaluation.encode_run_length(x: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence) | [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)]

Encode vector with run-length encoding.

* **Parameters:**
  **x** – Vector to encode.
* **Returns:**
  Values and their run lengths.

### Examples

```pycon
>>> x = np.array([0, 1, 1, 0, 1])
>>> encode_run_length(x)
(array([0, 1, 0, 1]), array([1, 2, 1, 1]))
>>> encode_run_length(x.astype('bool'))
(array([False,  True, False,  True]), array([1, 2, 1, 1]))
>>> encode_run_length(x.astype('<U1'))
(array(['0', '1', '0', '1'], dtype='<U1'), array([1, 2, 1, 1]))
>>> encode_run_length(np.where(x == 0, np.nan, x))
(array([nan,  1., nan,  1.]), array([1, 2, 1, 1]))
```

### pudl.analysis.timeseries_evaluation.insert_run_length(x: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence) | [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), values: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence) | [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), lengths: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)], mask: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[bool](https://docs.python.org/3/library/functions.html#bool)] = None, padding: [int](https://docs.python.org/3/library/functions.html#int) = 0, intersect: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Insert run-length encoded values into a vector.

* **Parameters:**
  * **x** – Vector to insert values into.
  * **values** – Values to insert.
  * **lengths** – Length of run to insert for each value in values.
  * **mask** – Boolean mask, of the same length as x, where values can be inserted.
    By default, values can be inserted anywhere in x.
  * **padding** – Minimum space between inserted runs and,
    if mask is provided, the edges of masked-out areas.
  * **intersect** – Whether to allow inserted runs to intersect each other.
* **Raises:**
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Padding must zero or greater.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Run length must be greater than zero.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Could not find space for run of length {length}.
* **Returns:**
  Copy of array x with values inserted.

### Example

```pycon
>>> x = [0, 0, 0, 0]
>>> mask = [True, False, True, True]
>>> insert_run_length(x, values=[1, 2], lengths=[1, 2], mask=mask)
array([1, 0, 2, 2])
```

If we use unique values for the background and each inserted run,
the run length encoding of the result (ignoring the background)
is the same as the inserted run, albeit in a different order.

```pycon
>>> x = np.zeros(10, dtype=int)
>>> values = [1, 2, 3]
>>> lengths = [1, 2, 3]
>>> x = insert_run_length(x, values=values, lengths=lengths)
>>> rvalues, rlengths = encode_run_length(x[x != 0])
>>> order = np.argsort(rvalues)
>>> all(rvalues[order] == values) and all(rlengths[order] == lengths)
True
```

Null values can be inserted into a vector such that the new null runs
match the run length encoding of the existing null runs.

```pycon
>>> x = [1, 2, np.nan, np.nan, 5, 6, 7, 8, np.nan]
>>> is_nan = np.isnan(x)
>>> rvalues, rlengths = encode_run_length(is_nan)
>>> xi = insert_run_length(
...     x,
...     values=[np.nan] * rvalues.sum(),
...     lengths=rlengths[rvalues],
...     mask=~is_nan
... )
>>> np.isnan(xi).sum() == 2 * is_nan.sum()
np.True_
```

The same as above, with non-zero padding, yields a unique solution:

```pycon
>>> insert_run_length(
...     x,
...     values=[np.nan] * rvalues.sum(),
...     lengths=rlengths[rvalues],
...     mask=~is_nan,
...     padding=1
... )
array([nan,  2., nan, nan,  5., nan, nan,  8., nan])
```

### pudl.analysis.timeseries_evaluation.summarize_flags(self) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Summarize flagged values by flag, count and median.

### pudl.analysis.timeseries_evaluation.plot_flags(self, name: Any = 0) → [None](https://docs.python.org/3/library/constants.html#None)

Plot cleaned series and anomalous values colored by flag.

* **Parameters:**
  **name** – Series to plot, as either an integer index or name in `columns`.

### pudl.analysis.timeseries_evaluation.simulate_nulls(self, lengths: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = None, padding: [int](https://docs.python.org/3/library/functions.html#int) = 1, intersect: [bool](https://docs.python.org/3/library/functions.html#bool) = False, overlap: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Find non-null values to null to match a run-length distribution.

* **Parameters:**
  * **length** – Length of null runs to simulate for each series.
    By default, uses the run lengths of null values in each series.
  * **padding** – Minimum number of non-null values between simulated null runs
    and between simulated and existing null runs.
  * **intersect** – Whether simulated null runs can intersect each other.
  * **overlap** – Whether simulated null runs can overlap existing null runs.
    If True, padding is ignored.
* **Returns:**
  Boolean mask of current non-null values to set to null.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Could not find space for run of length {length}.
