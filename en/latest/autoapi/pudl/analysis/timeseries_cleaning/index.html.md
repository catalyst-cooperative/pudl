# pudl.analysis.timeseries_cleaning

Screen timeseries for anomalies and impute missing and anomalous values.

For a narrative discussion of these methods aimed at data users, see
[Timeseries Imputation](../../../../methodology/timeseries_imputation.md).

The screening methods were originally designed to identify unrealistic data in the
electricity demand timeseries reported in [EIA Form 930 – Hourly and Daily Balancing Authority Operations Report](../../../../data_sources/eia930.md), and we have also
applied them to demand data from [FERC Form 714 – Annual Electric Balancing Authority Area and Planning Area Report](../../../../data_sources/ferc714.md).

Screening methods are adapted from code written and maintained by:

* [Tyler Ruggles](https://github.com/truggles)
* [Alicia Wongel](https://github.com/awongel)
* [Greg Schivley](https://github.com/gschivley)
* [David Farnham](https://github.com/d-farnham)

And described at:

* [https://doi.org/10.1038/s41597-020-0483-x](https://doi.org/10.1038/s41597-020-0483-x)
* [https://zenodo.org/record/3737085](https://zenodo.org/record/3737085)
* [https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code](https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code)

The imputation methods were designed for multivariate time series forecasting. They are
adapted from code published by [Xinyu Chen](https://xinychen.github.io/) and
described at:

* [https://arxiv.org/abs/2006.10436](https://arxiv.org/abs/2006.10436)
* [https://arxiv.org/abs/2008.03194](https://arxiv.org/abs/2008.03194)
* [https://github.com/xinychen/tensor-learning](https://github.com/xinychen/tensor-learning)

## Attributes

| [`logger`](#pudl.analysis.timeseries_cleaning.logger)                             |                                                                 |
|-----------------------------------------------------------------------------------|-----------------------------------------------------------------|
| [`STANDARD_UTC_OFFSETS`](#pudl.analysis.timeseries_cleaning.STANDARD_UTC_OFFSETS) | Hour offset from Coordinated Universal Time (UTC) by time zone. |

## Classes

| [`UTCTimeseriesDataFrame`](#pudl.analysis.timeseries_cleaning.UTCTimeseriesDataFrame)         | Define schema of input tables for timeseries cleaning.                      |
|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`AlignedTimeseriesDataFrame`](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame) | Define schema of input tables for timeseries cleaning.                      |
| [`TimeseriesMatrix`](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)                     | Define schema for timeseries matrix used during imputation.                 |
| [`FlaggedTimeseries`](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)                   | Container class used to flag values in a timeseries matrix for imputation.  |
| [`SimulateFlagsSettings`](#pudl.analysis.timeseries_cleaning.SimulateFlagsSettings)           | Define settings used to simulate flagged values for scoring imputation.     |
| [`SimulationDataFrame`](#pudl.analysis.timeseries_cleaning.SimulationDataFrame)               | Collection of months of data which will be used to simulate flagged values. |
| [`ImputeTimeseriesSettings`](#pudl.analysis.timeseries_cleaning.ImputeTimeseriesSettings)     | Define settings used for timeseries imputation.                             |

## Functions

| [`_shift_utc`](#pudl.analysis.timeseries_cleaning._shift_utc)(→ pandas.Series)                                                               | Shift `utc` by UTC offset.                                                                           |
|----------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| [`utc_dataframe_to_aligned`](#pudl.analysis.timeseries_cleaning.utc_dataframe_to_aligned)(...)                                               | Return DataFrame with `datetime_utc` shifted by offset to align timeseries'.                         |
| [`pivot_aligned_timeseries_dataframe`](#pudl.analysis.timeseries_cleaning.pivot_aligned_timeseries_dataframe)(...)                           | Pivot aligned timeseries dataframe into timeseries matrix and pad if needed.                         |
| [`melt_imputed_timeseries_matrix`](#pudl.analysis.timeseries_cleaning.melt_imputed_timeseries_matrix)(...)                                   | Melt imputed timeseries matrix and flag matrix to time-aligned dataframe.                            |
| [`slice_axis`](#pudl.analysis.timeseries_cleaning.slice_axis)(→ tuple[slice, Ellipsis])                                                      | Return an index that slices an array along an axis.                                                  |
| [`array_diff`](#pudl.analysis.timeseries_cleaning.array_diff)(→ numpy.ndarray)                                                               | First discrete difference of array elements.                                                         |
| [`encode_run_length`](#pudl.analysis.timeseries_cleaning.encode_run_length)(→ tuple[numpy.ndarray, numpy.ndarray])                           | Encode vector with run-length encoding.                                                              |
| [`insert_run_length`](#pudl.analysis.timeseries_cleaning.insert_run_length)(→ numpy.ndarray)                                                 | Insert run-length encoded values into a vector.                                                      |
| [`_mat2ten`](#pudl.analysis.timeseries_cleaning._mat2ten)(→ numpy.ndarray)                                                                   | Fold matrix into a tensor.                                                                           |
| [`_ten2mat`](#pudl.analysis.timeseries_cleaning._ten2mat)(→ numpy.ndarray)                                                                   | Unfold tensor into a matrix.                                                                         |
| [`_svt_tnn`](#pudl.analysis.timeseries_cleaning._svt_tnn)(→ numpy.ndarray)                                                                   | Singular value thresholding (SVT) truncated nuclear norm (TNN) minimization.                         |
| [`impute_latc_tnn`](#pudl.analysis.timeseries_cleaning.impute_latc_tnn)(→ numpy.ndarray)                                                     | Impute tensor values with LATC-TNN method by Chen and Sun (2020).                                    |
| [`_tsvt`](#pudl.analysis.timeseries_cleaning._tsvt)(→ numpy.ndarray)                                                                         | Tensor singular value thresholding (TSVT).                                                           |
| [`impute_latc_tubal`](#pudl.analysis.timeseries_cleaning.impute_latc_tubal)(→ numpy.ndarray)                                                 | Impute tensor values with LATC-Tubal method by Chen, Chen and Sun (2020).                            |
| [`flag_null`](#pudl.analysis.timeseries_cleaning.flag_null)(→ FlaggedTimeseries)                                                             | Flag null values (MISSING_VALUE).                                                                    |
| [`flag_negative_or_zero`](#pudl.analysis.timeseries_cleaning.flag_negative_or_zero)(→ FlaggedTimeseries)                                     | Flag negative or zero values (NEGATIVE_OR_ZERO).                                                     |
| [`flag_identical_run`](#pudl.analysis.timeseries_cleaning.flag_identical_run)(→ FlaggedTimeseries)                                           | Flag the last values in identical runs (IDENTICAL_RUN).                                              |
| [`flag_global_outlier`](#pudl.analysis.timeseries_cleaning.flag_global_outlier)(→ FlaggedTimeseries)                                         | Flag values greater or less than n times the global median (GLOBAL_OUTLIER).                         |
| [`flag_global_outlier_neighbor`](#pudl.analysis.timeseries_cleaning.flag_global_outlier_neighbor)(→ FlaggedTimeseries)                       | Flag values neighboring global outliers (GLOBAL_OUTLIER_NEIGHBOR).                                   |
| [`rolling_median`](#pudl.analysis.timeseries_cleaning.rolling_median)(→ numpy.ndarray)                                                       | Rolling median of values.                                                                            |
| [`rolling_median_offset`](#pudl.analysis.timeseries_cleaning.rolling_median_offset)(→ numpy.ndarray)                                         | Values minus the rolling median.                                                                     |
| [`median_of_rolling_median_offset`](#pudl.analysis.timeseries_cleaning.median_of_rolling_median_offset)() → numpy.ndarray)                   | Median of the offset from the rolling median.                                                        |
| [`rolling_iqr_of_rolling_median_offset`](#pudl.analysis.timeseries_cleaning.rolling_iqr_of_rolling_median_offset)(→ numpy.ndarray)           | Rolling interquartile range (IQR) of rolling median offset.                                          |
| [`median_prediction`](#pudl.analysis.timeseries_cleaning.median_prediction)(, long_window)                                                   | Values predicted from local and regional rolling medians.                                            |
| [`flag_local_outlier`](#pudl.analysis.timeseries_cleaning.flag_local_outlier)(, long_window, iqr_window, ...)                                | Flag local outliers (LOCAL_OUTLIER_HIGH, LOCAL_OUTLIER_LOW).                                         |
| [`diff`](#pudl.analysis.timeseries_cleaning.diff)(→ numpy.ndarray)                                                                           | Values minus the value of their neighbor.                                                            |
| [`rolling_iqr_of_diff`](#pudl.analysis.timeseries_cleaning.rolling_iqr_of_diff)(→ numpy.ndarray)                                             | Rolling interquartile range (IQR) of difference between neighboring values.                          |
| [`flag_double_delta`](#pudl.analysis.timeseries_cleaning.flag_double_delta)(→ FlaggedTimeseries)                                             | Flag values very different from neighbors on either side (DOUBLE_DELTA).                             |
| [`relative_median_prediction`](#pudl.analysis.timeseries_cleaning.relative_median_prediction)(→ numpy.ndarray)                               | Values divided by their value predicted from medians.                                                |
| [`iqr_of_diff_of_relative_median_prediction`](#pudl.analysis.timeseries_cleaning.iqr_of_diff_of_relative_median_prediction)(→ numpy.ndarray) | Interquartile range of running difference of relative median prediction.                             |
| [`_find_single_delta`](#pudl.analysis.timeseries_cleaning._find_single_delta)(→ numpy.ndarray)                                               |                                                                                                      |
| [`flag_single_delta`](#pudl.analysis.timeseries_cleaning.flag_single_delta)(, long_window, iqr_window, ...)                                  | Flag values very different from the nearest unflagged value (SINGLE_DELTA).                          |
| [`flag_anomalous_region`](#pudl.analysis.timeseries_cleaning.flag_anomalous_region)(→ FlaggedTimeseries)                                     | Flag values surrounded by flagged values (ANOMALOUS_REGION).                                         |
| [`flag_bad_years`](#pudl.analysis.timeseries_cleaning.flag_bad_years)(→ FlaggedTimeseries)                                                   | Flag entire years, which are missing a large portion of values (BAD_YEAR).                           |
| [`flag_ruggles`](#pudl.analysis.timeseries_cleaning.flag_ruggles)(...)                                                                       | Flag values following the method of Ruggles and others (2020).                                       |
| [`summarize_flags`](#pudl.analysis.timeseries_cleaning.summarize_flags)(→ pandas.DataFrame)                                                  | Summarize flagged values by flag, count and median.                                                  |
| [`simulate_nulls`](#pudl.analysis.timeseries_cleaning.simulate_nulls)(→ numpy.ndarray)                                                       | Find non-null values to null to match a run-length distribution.                                     |
| [`fold_tensor`](#pudl.analysis.timeseries_cleaning.fold_tensor)(→ numpy.ndarray)                                                             | Fold into a 3-dimensional tensor representation.                                                     |
| [`unfold_tensor`](#pudl.analysis.timeseries_cleaning.unfold_tensor)(→ numpy.ndarray)                                                         | Unfold a 3-dimensional tensor representation.                                                        |
| [`impute`](#pudl.analysis.timeseries_cleaning.impute)(→ pandera.typing.DataFrame[TimeseriesMatrix])                                          | Impute null values.                                                                                  |
| [`summarize_imputed`](#pudl.analysis.timeseries_cleaning.summarize_imputed)(→ pandas.DataFrame)                                              | Summarize the fit of imputed values to actual values.                                                |
| [`impute_flagged_values`](#pudl.analysis.timeseries_cleaning.impute_flagged_values)(...)                                                     | Impute null values in input timeseries matrix.                                                       |
| [`_merge_imputed`](#pudl.analysis.timeseries_cleaning._merge_imputed)(→ pandas.DataFrame)                                                    | Helper function to melt imputed timeseries matrix and merge back on input asset.                     |
| [`_add_simulated_flag_col`](#pudl.analysis.timeseries_cleaning._add_simulated_flag_col)(...)                                                 | Return a modified `imputed_df` with a column indicating which rows should be flagged for simulation. |
| [`get_simulated_flag_mask`](#pudl.analysis.timeseries_cleaning.get_simulated_flag_mask)(...)                                                 | Return a flag mask to flag values for simulated imputation.                                          |
| [`impute_timeseries_asset_factory`](#pudl.analysis.timeseries_cleaning.impute_timeseries_asset_factory)() → pandas.DataFrame)                | Produces assets to impute values for a given timeseries table/column.                                |

## Module Contents

### pudl.analysis.timeseries_cleaning.logger

### pudl.analysis.timeseries_cleaning.STANDARD_UTC_OFFSETS *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

Hour offset from Coordinated Universal Time (UTC) by time zone.

Time zones are canonical names (e.g. ‘America/Denver’) from tzdata (
[https://www.iana.org/time-zones](https://www.iana.org/time-zones))
mapped to their standard-time UTC offset.

### *class* pudl.analysis.timeseries_cleaning.UTCTimeseriesDataFrame

Bases: `pandera.pandas.DataFrameModel`

Define schema of input tables for timeseries cleaning.

This model defines the expected structure of an input dataframe to the timeseries
imputation process. It will be be immediately converted to a
[`AlignedTimeseriesDataFrame`](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame), then pivoted to a [`TimeseriesMatrix`](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix).

#### id_col *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[Any]*

Entity ID column(s). Used to group timeseries by entity.

#### datetime_utc *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[pandera.pandas.dtypes.DateTime]*

Datetimes in UTC timezone.

#### timezone *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)*

Local timezone of entity.

#### value_col *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[[pandas.Float64Dtype](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Float64Dtype.html#pandas.Float64Dtype)]*

Column containing actual values to impute.

### *class* pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame

Bases: `pandera.pandas.DataFrameModel`

Define schema of input tables for timeseries cleaning.

This model is nearly identical to a `UTCTimeseriesDataFrame`, but the
`datetime_utc` values are aligned to “local” `datetime`’s using a fixed UTC
offset.

#### id_col *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[Any]*

Entity ID column(s). Used to group timeseries by entity.

#### datetime *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[pandera.pandas.dtypes.DateTime]*

Datetimes shifted by UTC offset to align all timeseries’.

#### value_col *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[[pandas.Float64Dtype](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Float64Dtype.html#pandas.Float64Dtype)]*

Column containing actual values to impute.

#### flags *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None)*

Column indicating why value was flagged for imputation.

### *class* pudl.analysis.timeseries_cleaning.TimeseriesMatrix

Bases: `pandera.pandas.DataFrameModel`

Define schema for timeseries matrix used during imputation.

TimeseriesMatrix is the main type used during imputation. It is a dataframe with a
datetime row index (e.g. ‘2006-01-01 00:00:00’, …, ‘2019-12-31 23:00:00’) in
local time ignoring daylight-savings, and a id_col column index (e.g. 101, …,
329). Since the columns are dynamically generated by pivoting a
`AlignedTimeseriesDataFrame`, this model only explicitly defines the `datetime`
index. The primary purpose of this type is to annotate methods in this module, so
the expected inputs and outputs are immediately clear.

#### datetime *: [pandera.typing.Index](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Index.html#pandera.typing.Index)[pandera.pandas.dtypes.DateTime]*

Index timeseries matrix by datetime.

### pudl.analysis.timeseries_cleaning.\_shift_utc(utc: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), utc_offset: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Shift `utc` by UTC offset.

* **Parameters:**
  * **utc** – UTC times (tz-naive `datetime64[ns]` or `datetime64[ns, UTC]`).
  * **utc_offset** – For each datetime in `utc` a corresponding offset in hours.
* **Returns:**
  Shifted datetimes (tz-naive `datetime64[ns]`).

### Examples

```pycon
>>> s = pd.Series([pd.Timestamp(2020, 1, 1), pd.Timestamp(2020, 1, 1)])
>>> _shift_utc(s, [-7, -6])
0   2019-12-31 17:00:00
1   2019-12-31 18:00:00
dtype: datetime64[ns]
```

### pudl.analysis.timeseries_cleaning.utc_dataframe_to_aligned(input_df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[UTCTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.UTCTimeseriesDataFrame)]) → [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)]

Return DataFrame with `datetime_utc` shifted by offset to align timeseries’.

### pudl.analysis.timeseries_cleaning.pivot_aligned_timeseries_dataframe(aligned_df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)], value_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'value_col') → [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)]

Pivot aligned timeseries dataframe into timeseries matrix and pad if needed.

Padding finds the complete list of hours from the start of the first day
present in the timeseries to the end of the last, and then fills any missing hours
with NULLs.

### pudl.analysis.timeseries_cleaning.melt_imputed_timeseries_matrix(imputed_matrix: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], flag_matrix: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)]) → [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)]

Melt imputed timeseries matrix and flag matrix to time-aligned dataframe.

### *class* pudl.analysis.timeseries_cleaning.FlaggedTimeseries

Container class used to flag values in a timeseries matrix for imputation.

#### x *: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)*

#### columns *: [pandas.Index](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.html#pandas.Index)*

#### index *: [pandas.Index](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.html#pandas.Index)*

#### flags *: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)*

#### uuid *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### \_\_hash_\_()

Implement hash for lru_cache.

#### *classmethod* from_timeseries_matrix(matrix: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), flags: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Create a timeseries object from a dataframe.

#### to_dataframes() → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Convert back to a dataframe.

#### flag(mask: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), flag: [pudl.metadata.dfs.ImputationReasonCodes](../../metadata/dfs/index.md#pudl.metadata.dfs.ImputationReasonCodes)) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag values.

Flags values (if not already flagged) and nulls flagged values.

* **Parameters:**
  * **mask** – Boolean mask of the values to flag.
  * **flag** – Flag name.

### pudl.analysis.timeseries_cleaning.slice_axis(x: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), start: [int](https://docs.python.org/3/library/functions.html#int) = None, end: [int](https://docs.python.org/3/library/functions.html#int) = None, step: [int](https://docs.python.org/3/library/functions.html#int) = None, axis: [int](https://docs.python.org/3/library/functions.html#int) = 0) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[slice](https://docs.python.org/3/library/functions.html#slice), Ellipsis]

Return an index that slices an array along an axis.

* **Parameters:**
  * **x** – Array to slice.
  * **start** – Start index of slice.
  * **end** – End index of slice.
  * **step** – Step size of slice.
  * **axis** – Axis along which to slice.
* **Returns:**
  Tuple of [`slice`](https://docs.python.org/3/library/functions.html#slice) that slices array x along axis axis
  (x[…, start:stop:step]).

### Examples

```pycon
>>> x = np.random.random((3, 4, 5))
>>> np.all(x[1:] == x[slice_axis(x, start=1, axis=0)])
np.True_
>>> np.all(x[:, 1:] == x[slice_axis(x, start=1, axis=1)])
np.True_
>>> np.all(x[:, :, 1:] == x[slice_axis(x, start=1, axis=2)])
np.True_
```

### pudl.analysis.timeseries_cleaning.array_diff(x: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), periods: [int](https://docs.python.org/3/library/functions.html#int) = 1, axis: [int](https://docs.python.org/3/library/functions.html#int) = 0, fill: Any = np.nan) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

First discrete difference of array elements.

This is a fast numpy implementation of `pd.DataFrame.diff()`.

* **Parameters:**
  * **periods** – Periods to shift for calculating difference, accepts negative values.
  * **axis** – Array axis along which to calculate the difference.
  * **fill** – Value to use at the margins where a difference cannot be calculated.
* **Returns:**
  Array of same shape and type as x with discrete element differences.

### Examples

```pycon
>>> x = np.random.random((4, 2))
>>> np.all(array_diff(x, 1)[1:] == pd.DataFrame(x).diff(1).to_numpy()[1:])
np.True_
>>> np.all(array_diff(x, 2)[2:] == pd.DataFrame(x).diff(2).to_numpy()[2:])
np.True_
>>> np.all(array_diff(x, -1)[:-1] == pd.DataFrame(x).diff(-1).to_numpy()[:-1])
np.True_
```

### pudl.analysis.timeseries_cleaning.encode_run_length(x: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence) | [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)]

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

### pudl.analysis.timeseries_cleaning.insert_run_length(x: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence) | [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), values: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence) | [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), lengths: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)], mask: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[bool](https://docs.python.org/3/library/functions.html#bool)] = None, padding: [int](https://docs.python.org/3/library/functions.html#int) = 0, intersect: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

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

### pudl.analysis.timeseries_cleaning.\_mat2ten(matrix: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), shape: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), mode: [int](https://docs.python.org/3/library/functions.html#int)) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Fold matrix into a tensor.

### pudl.analysis.timeseries_cleaning.\_ten2mat(tensor: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), mode: [int](https://docs.python.org/3/library/functions.html#int)) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Unfold tensor into a matrix.

### pudl.analysis.timeseries_cleaning.\_svt_tnn(matrix: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), tau: [float](https://docs.python.org/3/library/functions.html#float), theta: [int](https://docs.python.org/3/library/functions.html#int)) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Singular value thresholding (SVT) truncated nuclear norm (TNN) minimization.

### pudl.analysis.timeseries_cleaning.impute_latc_tnn(tensor: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), lags: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = [1], alpha: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[float](https://docs.python.org/3/library/functions.html#float)] = [1 / 3, 1 / 3, 1 / 3], rho0: [float](https://docs.python.org/3/library/functions.html#float) = 1e-07, lambda0: [float](https://docs.python.org/3/library/functions.html#float) = 2e-07, theta: [int](https://docs.python.org/3/library/functions.html#int) = 20, epsilon: [float](https://docs.python.org/3/library/functions.html#float) = 1e-07, maxiter: [int](https://docs.python.org/3/library/functions.html#int) = 300) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Impute tensor values with LATC-TNN method by Chen and Sun (2020).

Uses low-rank autoregressive tensor completion (LATC) with
truncated nuclear norm (TNN) minimization.

* description: [https://arxiv.org/abs/2006.10436](https://arxiv.org/abs/2006.10436)
* code: [https://github.com/xinychen/tensor-learning/blob/master/mats](https://github.com/xinychen/tensor-learning/blob/master/mats)

* **Parameters:**
  * **tensor** – Observational series in the form (series, groups, periods).
    Null values are replaced with zeros, so any zeros will be treated as null.
  * **lags**
  * **alpha**
  * **rho0**
  * **lambda0**
  * **theta**
  * **epsilon** – Convergence criterion. A smaller number will result in more iterations.
  * **maxiter** – Maximum number of iterations.
* **Returns:**
  Tensor with missing values in tensor replaced by imputed values.

### pudl.analysis.timeseries_cleaning.\_tsvt(tensor: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), phi: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), tau: [float](https://docs.python.org/3/library/functions.html#float)) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Tensor singular value thresholding (TSVT).

### pudl.analysis.timeseries_cleaning.impute_latc_tubal(tensor: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), lags: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = [1], rho0: [float](https://docs.python.org/3/library/functions.html#float) = 1e-07, lambda0: [float](https://docs.python.org/3/library/functions.html#float) = 2e-07, epsilon: [float](https://docs.python.org/3/library/functions.html#float) = 1e-07, maxiter: [int](https://docs.python.org/3/library/functions.html#int) = 300) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Impute tensor values with LATC-Tubal method by Chen, Chen and Sun (2020).

Uses low-tubal-rank autoregressive tensor completion (LATC-Tubal).
It is much faster than [`impute_latc_tnn()`](#pudl.analysis.timeseries_cleaning.impute_latc_tnn) for very large datasets,
with comparable accuracy.

* description: [https://arxiv.org/abs/2008.03194](https://arxiv.org/abs/2008.03194)
* code: [https://github.com/xinychen/tensor-learning/blob/master/mats](https://github.com/xinychen/tensor-learning/blob/master/mats)

* **Parameters:**
  * **tensor** – Observational series in the form (series, groups, periods).
    Null values are replaced with zeros, so any zeros will be treated as null.
  * **lags**
  * **rho0**
  * **lambda0**
  * **epsilon** – Convergence criterion. A smaller number will result in more iterations.
  * **maxiter** – Maximum number of iterations.
* **Returns:**
  Tensor with missing values in tensor replaced by imputed values.

### pudl.analysis.timeseries_cleaning.flag_null(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag null values (MISSING_VALUE).

### pudl.analysis.timeseries_cleaning.flag_negative_or_zero(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag negative or zero values (NEGATIVE_OR_ZERO).

### pudl.analysis.timeseries_cleaning.flag_identical_run(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), length: [int](https://docs.python.org/3/library/functions.html#int) = 3) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag the last values in identical runs (IDENTICAL_RUN).

* **Parameters:**
  **length** – Run length to flag.
  If 3, the third (and subsequent) identical values are flagged.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Run length must be 2 or greater.

### pudl.analysis.timeseries_cleaning.flag_global_outlier(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), medians: [float](https://docs.python.org/3/library/functions.html#float) = 9) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag values greater or less than n times the global median (GLOBAL_OUTLIER).

* **Parameters:**
  **medians** – Number of times the median the value must exceed the median.

### pudl.analysis.timeseries_cleaning.flag_global_outlier_neighbor(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), neighbors: [int](https://docs.python.org/3/library/functions.html#int) = 1) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag values neighboring global outliers (GLOBAL_OUTLIER_NEIGHBOR).

* **Parameters:**
  **neighbors** – Number of neighbors to flag on either side of each outlier.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Global outliers must be flagged first.

### pudl.analysis.timeseries_cleaning.rolling_median(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Rolling median of values.

* **Parameters:**
  **window** – Number of values in the moving window.

### pudl.analysis.timeseries_cleaning.rolling_median_offset(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Values minus the rolling median.

Estimates the local cycle in cyclical data by removing longterm trends.

* **Parameters:**
  **window** – Number of values in the moving window.

### pudl.analysis.timeseries_cleaning.median_of_rolling_median_offset(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48, shifts: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = range(-240, 241, 24)) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Median of the offset from the rolling median.

Calculated by shifting the rolling median offset ([`rolling_median_offset()`](#pudl.analysis.timeseries_cleaning.rolling_median_offset))
by different numbers of values, then taking the median at each position.
Estimates the typical local cycle in cyclical data.

* **Parameters:**
  * **window** – Number of values in the moving window for the rolling median.
  * **shifts** – Number of values to shift the rolling median offset by.

### pudl.analysis.timeseries_cleaning.rolling_iqr_of_rolling_median_offset(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48, iqr_window: [int](https://docs.python.org/3/library/functions.html#int) = 240) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Rolling interquartile range (IQR) of rolling median offset.

Estimates the spread of the local cycles in cyclical data.

* **Parameters:**
  * **window** – Number of values in the moving window for the rolling median.
  * **iqr_window** – Number of values in the moving window for the rolling IQR.

### pudl.analysis.timeseries_cleaning.median_prediction(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48, shifts: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = range(-240, 241, 24), long_window: [int](https://docs.python.org/3/library/functions.html#int) = 480) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Values predicted from local and regional rolling medians.

Calculated as { local median } +
{ median of local median offset } \* { local median } / { regional median }.

* **Parameters:**
  * **window** – Number of values in the moving window for the local rolling median.
  * **shifts** – Positions to shift the local rolling median offset by,
    for computing its median.
  * **long_window** – Number of values in the moving window
    for the regional (long) rolling median.

### pudl.analysis.timeseries_cleaning.flag_local_outlier(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48, shifts: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = range(-240, 241, 24), long_window: [int](https://docs.python.org/3/library/functions.html#int) = 480, iqr_window: [int](https://docs.python.org/3/library/functions.html#int) = 240, multiplier: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[float](https://docs.python.org/3/library/functions.html#float), [float](https://docs.python.org/3/library/functions.html#float)] = (3.5, 2.5)) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag local outliers (LOCAL_OUTLIER_HIGH, LOCAL_OUTLIER_LOW).

Flags values which are above or below the [`median_prediction()`](#pudl.analysis.timeseries_cleaning.median_prediction) by more than
a multiplier times the [`rolling_iqr_of_rolling_median_offset()`](#pudl.analysis.timeseries_cleaning.rolling_iqr_of_rolling_median_offset).

* **Parameters:**
  * **window** – Number of values in the moving window for the local rolling median.
  * **shifts** – Positions to shift the local rolling median offset by,
    for computing its median.
  * **long_window** – Number of values in the moving window
    for the regional (long) rolling median.
  * **iqr_window** – Number of values in the moving window
    for the rolling interquartile range (IQR).
  * **multiplier** – Number of times the [`rolling_iqr_of_rolling_median_offset()`](#pudl.analysis.timeseries_cleaning.rolling_iqr_of_rolling_median_offset)
    the value must be above (HIGH) and below (LOW)
    the [`median_prediction()`](#pudl.analysis.timeseries_cleaning.median_prediction) to be flagged.

### pudl.analysis.timeseries_cleaning.diff(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), shift: [int](https://docs.python.org/3/library/functions.html#int) = 1) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Values minus the value of their neighbor.

* **Parameters:**
  **shift** – Positions to shift for calculating the difference.
  Positive values select a preceding (left) neighbor.

### pudl.analysis.timeseries_cleaning.rolling_iqr_of_diff(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), shift: [int](https://docs.python.org/3/library/functions.html#int) = 1, window: [int](https://docs.python.org/3/library/functions.html#int) = 240) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Rolling interquartile range (IQR) of difference between neighboring values.

* **Parameters:**
  * **shift** – Positions to shift for calculating the difference.
  * **window** – Number of values in the moving window for the rolling IQR.

### pudl.analysis.timeseries_cleaning.flag_double_delta(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), iqr_window: [int](https://docs.python.org/3/library/functions.html#int) = 240, multiplier: [float](https://docs.python.org/3/library/functions.html#float) = 2) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag values very different from neighbors on either side (DOUBLE_DELTA).

Flags values whose differences to both neighbors on either side exceeds a
multiplier times the rolling interquartile range (IQR) of neighbor difference.

* **Parameters:**
  * **iqr_window** – Number of values in the moving window for the rolling IQR
    of neighbor difference.
  * **multiplier** – Number of times the rolling IQR of neighbor difference
    the value’s difference to its neighbors must exceed
    for the value to be flagged.

### pudl.analysis.timeseries_cleaning.relative_median_prediction(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), \*\*kwargs: Any) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Values divided by their value predicted from medians.

* **Parameters:**
  **kwargs** – Arguments to [`median_prediction()`](#pudl.analysis.timeseries_cleaning.median_prediction).

### pudl.analysis.timeseries_cleaning.iqr_of_diff_of_relative_median_prediction(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), shift: [int](https://docs.python.org/3/library/functions.html#int) = 1, \*\*kwargs: Any) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Interquartile range of running difference of relative median prediction.

* **Parameters:**
  * **shift** – Positions to shift for calculating the difference.
    Positive values select a preceding (left) neighbor.
  * **kwargs** – Arguments to [`relative_median_prediction()`](#pudl.analysis.timeseries_cleaning.relative_median_prediction).

### pudl.analysis.timeseries_cleaning.\_find_single_delta(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), relative_median_prediction: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), relative_median_prediction_long: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), rolling_iqr_of_diff: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), iqr_of_diff_of_relative_median_prediction: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), reverse: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

### pudl.analysis.timeseries_cleaning.flag_single_delta(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48, shifts: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = range(-240, 241, 24), long_window: [int](https://docs.python.org/3/library/functions.html#int) = 480, iqr_window: [int](https://docs.python.org/3/library/functions.html#int) = 240, multiplier: [float](https://docs.python.org/3/library/functions.html#float) = 5, rel_multiplier: [float](https://docs.python.org/3/library/functions.html#float) = 15) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag values very different from the nearest unflagged value (SINGLE_DELTA).

Flags values whose difference to the nearest unflagged value,
with respect to value and relative median prediction,
differ by less than a multiplier times the rolling interquartile range (IQR)
of the difference -
multiplier times [`rolling_iqr_of_diff()`](#pudl.analysis.timeseries_cleaning.rolling_iqr_of_diff) and
rel_multiplier times `iqr_of_diff_of_relative_mean_prediction()`,
respectively.

* **Parameters:**
  * **window** – Number of values in the moving window for the rolling median
    (for the relative median prediction).
  * **shifts** – Positions to shift the local rolling median offset by,
    for computing its median (for the relative median prediction).
  * **long_window** – Number of values in the moving window for the long rolling
    median (for the relative median prediction).
  * **iqr_window** – Number of values in the moving window for the rolling IQR
    of neighbor difference.
  * **multiplier** – Number of times the rolling IQR of neighbor difference
    the value’s difference to its neighbor must exceed
    for the value to be flagged.
  * **rel_multiplier** – Number of times the rolling IQR of relative median
    prediction the value’s prediction difference to its neighbor must exceed
    for the value to be flagged.

### pudl.analysis.timeseries_cleaning.flag_anomalous_region(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), window: [int](https://docs.python.org/3/library/functions.html#int) = 48, threshold: [float](https://docs.python.org/3/library/functions.html#float) = 0.15) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag values surrounded by flagged values (ANOMALOUS_REGION).

Original null values are not considered flagged values.

* **Parameters:**
  * **window** – Width of regions.
  * **threshold** – Fraction of flagged values required for a region to be flagged.

### pudl.analysis.timeseries_cleaning.flag_bad_years(ts: [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries), min_data: [int](https://docs.python.org/3/library/functions.html#int) = 100, min_data_fraction: [float](https://docs.python.org/3/library/functions.html#float) = 0.9) → [FlaggedTimeseries](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries)

Flag entire years, which are missing a large portion of values (BAD_YEAR).

This method checks two separate thresholds to determine whether a year is “bad”.
First, it finds the range from the first non-null hour to the last non-null hour
for each respondent-year. If that total range is less than `min_data`, then the
year is dropped. Next, it checks if the ratio of values within that range which are
non-null is greater than `min_data_fraction`. If not, then the year will also be
dropped. This ensures that if there is a section of the year that is mostly complete,
even if the rest of the year is NULL, then it will still be included for imputation.

* **Parameters:**
  * **ts** – Timeseries matrix as described in [`FlaggedTimeseries`](#pudl.analysis.timeseries_cleaning.FlaggedTimeseries).
  * **min_data** – Minimum number of non-null hours in a year.
  * **min_data_fraction** – Minimum fraction of non-null hours between the first and last
    non-null hour in a year.

### pudl.analysis.timeseries_cleaning.flag_ruggles(timeseries_matrix: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], min_data: [int](https://docs.python.org/3/library/functions.html#int) = 100, min_data_fraction: [float](https://docs.python.org/3/library/functions.html#float) = 0.9) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)]]

Flag values following the method of Ruggles and others (2020).

Assumes values are hourly electricity demand.

* description: [https://doi.org/10.1038/s41597-020-0483-x](https://doi.org/10.1038/s41597-020-0483-x)
* code: [https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code](https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code)

* **Parameters:**
  * **ts** – Aligned timeseries matrix for imputation.
  * **min_data** – Minimum number of non-null hours in a year.
  * **min_data_fraction** – Minimum fraction of non-null hours between the first and last
* **Returns:**
  Two `TimeseriesMatrix` dataframes with the same shape. The first contains
  the input timeseries with flagged values Nulled out in preparation for
  imputation. The second contains the actual flags for reference.

### pudl.analysis.timeseries_cleaning.summarize_flags(imputed_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), id_col: [str](https://docs.python.org/3/library/stdtypes.html#str), value_col: [str](https://docs.python.org/3/library/stdtypes.html#str), flag_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Summarize flagged values by flag, count and median.

* **Parameters:**
  **imputed_df** – DataFrame

### pudl.analysis.timeseries_cleaning.simulate_nulls(x: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), lengths: [collections.abc.Sequence](https://docs.python.org/3/library/collections.abc.html#collections.abc.Sequence)[[int](https://docs.python.org/3/library/functions.html#int)] = None, padding: [int](https://docs.python.org/3/library/functions.html#int) = 1, intersect: [bool](https://docs.python.org/3/library/functions.html#bool) = False, overlap: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Find non-null values to null to match a run-length distribution.

* **Parameters:**
  * **x** – Timeseries matrix as described in `_prepare_timeseries_matrix()`
    defined within [`impute_timeseries_asset_factory()`](#pudl.analysis.timeseries_cleaning.impute_timeseries_asset_factory).
  * **length** – Length of null runs to simulate for each series.
    By default, uses the run lengths of null values in each series.
  * **padding** – Minimum number of non-null values between simulated null runs
    and between simulated and existing null runs.
  * **intersect** – Whether simulated null runs can intersect each other.
  * **overlap** – Whether simulated null runs can overlap existing null runs. If
    `True`, `padding` is ignored.
* **Returns:**
  Boolean mask of current non-null values to set to null.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Could not find space for run of length {length}.

### Examples

```pycon
>>> x = np.column_stack([[1, 2, np.nan, 4, 5, 6, 7, np.nan, np.nan]])
>>> simulate_nulls(x).ravel()
array([ True, False, False, False, True, True, False, False, False])
>>> simulate_nulls(x, lengths=[4], padding=0).ravel()
array([False, False, False, True, True, True, True, False, False])
```

### pudl.analysis.timeseries_cleaning.fold_tensor(x: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), periods: [int](https://docs.python.org/3/library/functions.html#int) = 24) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Fold into a 3-dimensional tensor representation.

Folds the series x (number of observations, number of series)
into a 3-d tensor (number of series, number of groups, number of periods),
splitting observations into groups of length periods.
For example, each group may represent a day and each period the hour of the day.

* **Parameters:**
  * **x** – Series array to fold. Uses `x` by default.
  * **periods** – Number of consecutive values in each series to fold into a group.
* **Returns:**
  ```pycon
  >>> x = np.column_stack([[1, 2, 3, 4, 5, 6], [10, 20, 30, 40, 50, 60]])
  >>> tensor = fold_tensor(x, periods=3)
  >>> tensor[0]
  array([[1, 2, 3],
         [4, 5, 6]])
  >>> np.all(x == unfold_tensor(tensor, x.shape))
  np.True_
  ```

### pudl.analysis.timeseries_cleaning.unfold_tensor(tensor: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray), shape) → [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)

Unfold a 3-dimensional tensor representation.

Performs the reverse of [`fold_tensor()`](#pudl.analysis.timeseries_cleaning.fold_tensor).

### pudl.analysis.timeseries_cleaning.impute(df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], mask: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray) = None, periods: [int](https://docs.python.org/3/library/functions.html#int) = 24, blocks: [int](https://docs.python.org/3/library/functions.html#int) = 1, method: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'tubal', \*\*kwargs: Any) → [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)]

Impute null values.

#### NOTE
The imputation method requires that nulls be replaced by zeros,
so the series cannot already contain zeros.

* **Parameters:**
  * **mask** – Boolean mask of values to impute in addition to
    any null values in `x`.
  * **periods** – Number of consecutive values in each series to fold into a group.
    See [`fold_tensor()`](#pudl.analysis.timeseries_cleaning.fold_tensor). Default of 24 is meant for hourly data with a
    diurnal periodicity.
  * **blocks** – Number of blocks into which to split the series for imputation.
    This has been found to reduce processing time for method=’tnn’.
  * **method** – Imputation method to use
    (‘tubal’: [`impute_latc_tubal()`](#pudl.analysis.timeseries_cleaning.impute_latc_tubal), ‘tnn’: [`impute_latc_tnn()`](#pudl.analysis.timeseries_cleaning.impute_latc_tnn)).
  * **kwargs** – Optional arguments to method.
* **Returns:**
  Array of same shape as `x` with all null values
  (and those selected by mask) replaced with imputed values.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – Zero values present. Replace with very small value.

### pudl.analysis.timeseries_cleaning.summarize_imputed(matrix: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], imputed_matrix: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], mask: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Summarize the fit of imputed values to actual values.

Summarizes the agreement between actual and imputed values with the
following statistics:

* mpe: Mean percent error, (actual - imputed) / actual.
* mape: Mean absolute percent error, abs(mpe).

* **Parameters:**
  * **imputed** – Series of same shape as `x` with imputed values.
    See [`impute()`](#pudl.analysis.timeseries_cleaning.impute).
  * **mask** – Boolean mask of imputed values that were not null in `x`.
    See [`simulate_nulls()`](#pudl.analysis.timeseries_cleaning.simulate_nulls).
* **Returns:**
  Table of imputed value statistics for each series.

### pudl.analysis.timeseries_cleaning.impute_flagged_values(df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], years: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)], method: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), Literal['tubal', 'tnn']], periods: [int](https://docs.python.org/3/library/functions.html#int) = 24, blocks: [int](https://docs.python.org/3/library/functions.html#int) = 1) → [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)]

Impute null values in input timeseries matrix.

Imputation is performed separately for each year, with only the respondents
reporting data in that year.

#### NOTE
The imputation is parallelized internally, and by default will use all available
CPU cores. If you want to limit the number of cores used, you can set the
`OMP_NUM_THREADS` environment variable to the desired number of threads.

* **Parameters:**
  * **df** – Timeseries matrix as described in `_prepare_timeseries_matrix()`
    defined within [`impute_timeseries_asset_factory()`](#pudl.analysis.timeseries_cleaning.impute_timeseries_asset_factory).
  * **years** – list of years to input
  * **periods** – Number of consecutive values in each series to fold into a group. See
    [`fold_tensor()`](#pudl.analysis.timeseries_cleaning.fold_tensor).
  * **blocks** – Number of blocks into which to split the series for imputation.
    This has been found to reduce processing time for the tnn method.
  * **method** – Maps each year to the appropriate imputation method. “tubal” uses
    [`impute_latc_tubal()`](#pudl.analysis.timeseries_cleaning.impute_latc_tubal) and  “tnn” uses [`impute_latc_tnn()`](#pudl.analysis.timeseries_cleaning.impute_latc_tnn).
* **Returns:**
  Copy of `df` with imputed values.

### *class* pudl.analysis.timeseries_cleaning.SimulateFlagsSettings

Define settings used to simulate flagged values for scoring imputation.

#### num_months *: [int](https://docs.python.org/3/library/functions.html#int)* *= 30*

The number of months of data to simulate.

#### min_flag_rate *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.1*

Min ratio of bad points in a section of data to be used for reference.

#### max_flag_rate *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.5*

Max ratio of bad points in a section of data to be used for reference.

#### output_io_manager_key *: [str](https://docs.python.org/3/library/stdtypes.html#str)* *= 'io_manager'*

Specify io-manager for final simulated asset.

In some cases we use the parquet IO-manager so we can build notebooks/visualizations
on simulated data.

#### mape_threshold *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.05*

Maximum allowable mean absolute percent error computed on simulated values. Will be checked in an asset check.

### *class* pudl.analysis.timeseries_cleaning.SimulationDataFrame

Bases: `pandera.pandas.DataFrameModel`

Collection of months of data which will be used to simulate flagged values.

Each row in this dataframe identifies a pairing of two entity IDs and two months
that can be used to evaluate the performance of the imputation. The “reference”
is a month in which a high proportion of reported values were flagged for
imputation, and the “simulation” is a month in which there were no values flagged
for imputation. The pattern of flagged (null) values in the reference month will be
used to mask the reported values found in the simulation month so they can be
imputed, and then the imputed values will be compared to the originally reported
data to evaluate the imputation’s performance.

#### reference_id_col *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[Any]*

#### reference_month *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[pandera.pandas.dtypes.DateTime]*

#### simulation_id_col *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[Any]*

#### simulation_month *: [pandera.typing.Series](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.Series.html#pandera.typing.Series)[pandera.pandas.dtypes.DateTime]*

### pudl.analysis.timeseries_cleaning.\_merge_imputed(aligned_df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)], matrix: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], flags: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Helper function to melt imputed timeseries matrix and merge back on input asset.

### pudl.analysis.timeseries_cleaning.\_add_simulated_flag_col(imputed_df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)], simulation_df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[SimulationDataFrame](#pudl.analysis.timeseries_cleaning.SimulationDataFrame)]) → [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)]

Return a modified `imputed_df` with a column indicating which rows should be flagged for simulation.

This will find all flagged values from a reference month and apply the flag
pattern to a simulation month. The flag pattern is determined by calculating the
hour of the month for each flagged (how many hours is this after the start of the
month), and flagging the corresponding hour in the simulation month. Reference
months are chosen by finding months with a relatively high rate of imputation,
while simulation months have no values which were flagged for imputation.

* **Parameters:**
  * **imputed_df** – Production DataFrame with imputed values, which is used to find
    sections with high rates of imputation.
  * **simulation_df** – DataFrame with reference and simulation months.
* **Returns:**
  DataFrame which contains all ID/datetime pairs that should be flagged for
  simulated imputation.

### pudl.analysis.timeseries_cleaning.get_simulated_flag_mask(settings: [SimulateFlagsSettings](#pudl.analysis.timeseries_cleaning.SimulateFlagsSettings), imputed_df: [pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[AlignedTimeseriesDataFrame](#pudl.analysis.timeseries_cleaning.AlignedTimeseriesDataFrame)], simulation_group: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pandera.typing.DataFrame](https://pandera.readthedocs.io/en/stable/reference/generated/pandera.typing.DataFrame.html#pandera.typing.DataFrame)[[TimeseriesMatrix](#pudl.analysis.timeseries_cleaning.TimeseriesMatrix)], [set](https://docs.python.org/3/library/stdtypes.html#set)[[int](https://docs.python.org/3/library/functions.html#int)]]

Return a flag mask to flag values for simulated imputation.

Find months of data with high rate of flagged values, and use these sections as a
reference to flag values in otherwise good sections of data. This allows us to
impute data in a realistic scenario where we have good reported data, which we can
compare to in order to compute quantitative metrics to validate the quality of our
imputation.

* **Parameters:**
  * **settings** – Settings object, which contains all configurable settings for
    simulation.
  * **imputed_df** – Production DataFrame with imputed values, which is used to find
    sections with high rates of imputation.
  * **simulation_group** – Allows testing imputation performance on different groups of
    data like BA/subregion demand, which can be combined into a single
    imputation.
* **Returns:**
  Tuple of `timeseries_matrix`, and `flag_matrix` modified with simulation
  data.

### *class* pudl.analysis.timeseries_cleaning.ImputeTimeseriesSettings

Define settings used for timeseries imputation.

#### min_data_fraction *: [float](https://docs.python.org/3/library/functions.html#float)* *= 0.7*

Fraction of values in a year which must be non-null to do imputation on year.

#### min_data *: [int](https://docs.python.org/3/library/functions.html#int)* *= 100*

Minimum number of values which must be non-null to do imputation on year.

#### periods *: [int](https://docs.python.org/3/library/functions.html#int)* *= 24*

Number of consecutive values in each series to fold into a group.

See [`fold_tensor()`](#pudl.analysis.timeseries_cleaning.fold_tensor). The default of 24 is meant for hourly data with a diurnal
periodicity.

#### blocks *: [int](https://docs.python.org/3/library/functions.html#int)* *= 1*

Split timeseries matrix into equal sized blocks before running imputation.

#### method *: Literal['tubal', 'tnn']* *= 'tubal'*

Imputation method to use.

* tubal indicates [`impute_latc_tubal()`](#pudl.analysis.timeseries_cleaning.impute_latc_tubal)
* tnn indicates [`impute_latc_tnn()`](#pudl.analysis.timeseries_cleaning.impute_latc_tnn)

#### method_overrides *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), Literal['tubal', 'tnn']]*

Override stated imputation method for specific years.

#### simulate_flags_settings *: [SimulateFlagsSettings](#pudl.analysis.timeseries_cleaning.SimulateFlagsSettings) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

Settings to simulate flagged values and score imputation.

Defaults to None which will not do any simulation/scoring.

### pudl.analysis.timeseries_cleaning.impute_timeseries_asset_factory(input_asset_name: [str](https://docs.python.org/3/library/stdtypes.html#str), output_asset_name: [str](https://docs.python.org/3/library/stdtypes.html#str), years_from_context: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable), id_col: [str](https://docs.python.org/3/library/stdtypes.html#str), value_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'demand_mwh', imputed_value_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'demand_imputed_mwh', reported_value_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'demand_reported_mwh', simulation_group_col: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, output_io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'parquet_io_manager', op_tags: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, settings: [ImputeTimeseriesSettings](#pudl.analysis.timeseries_cleaning.ImputeTimeseriesSettings) = ImputeTimeseriesSettings()) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produces assets to impute values for a given timeseries table/column.

This factory function produces a set of assets which perform timeseries imputation
on one column in a specified table. This process is split into a series of assets
to reduce peak memory usage by offloading intermediate products onto disk. The
assets also correspond with the three steps that make up the the timeseries
imputation process:

1. Convert datetime UTC to local datetimes and pivot dataframe to timeseries matrix
2. Flag anomalous and missing values in timeseries
3. Perform imputation and melt back to expected output table structure

This factory also has the ability to produce a set of simulation assets. These
assets mirror the production assets, but they will impute a selection of values
which were not actually flagged for imputation. This means we can impute data
where the reported data is actually deemed “good”, allowing us to compare the
imputed values to the reported. We then compute Mean Absolute Percentage Error
to score the imputation. We can produce these simulated assets during our nightly
builds for ongoing monitoring of the imputation, or just as one off way to validate
or compare imputation methods.

* **Parameters:**
  * **input_asset_name** – Name of upstream asset to perform imputation on.
  * **output_asset_name** – Name of final output asset with imputed column.
  * **years_from_context** – Function to generate the list of years on which to perform
    imputation on.
  * **id_col** – Name of column identifying entities to group timeseries by.
  * **value_col** – Column imputation will be performed on.
  * **imputed_value_col** – Name of column in output asset with imputed values.
  * **reported_value_col** – Name of column in output asset with original reported
    values.
  * **output_io_manager_key** – IO-manager to use for final output asset.
  * **simulation_group_col** – In cases where we are combining multiple datasets into
    a single imputation run (like BA/subregion demand), this column is used
    to compute simulation results for each set independently. This should
    point to a categorical column which defines which group a row belongs to.
  * **op_tags** – Tags applied to every op produced by the factory. Use
    `{"dagster/priority": N}` to raise scheduling priority for assets on
    the critical execution path.
  * **settings** – Configurable options for imputation
    (see [`ImputeTimeseriesSettings`](#pudl.analysis.timeseries_cleaning.ImputeTimeseriesSettings)).
