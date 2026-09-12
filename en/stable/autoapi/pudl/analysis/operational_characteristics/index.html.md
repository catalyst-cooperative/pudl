# pudl.analysis.operational_characteristics

Use EPA CEMS and EIA data to estimate generator operational characteristics.

Starting from hourly EPA CEMS gross load and fuel heat content, this module estimates,
for every plant-unit, a single trailing-window snapshot of: minimum stable load,
minimum up/down time, heat rate at maximum and minimum stable load, and ramp-up/-down
rate. These are derived by combining several independent per-unit calculations –
load-factor binning, run-length detection, and ramp-rate summarization – into one
output row per unit via [`estimate_operational_characteristics_by_unit()`](#pudl.analysis.operational_characteristics.estimate_operational_characteristics_by_unit).

See [Generator Operational Characteristics](../../../../methodology/operational_characteristics.html.md) for a longer prose explanation.

## Attributes

| [`logger`](#pudl.analysis.operational_characteristics.logger)                       |                                                                     |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------|
| [`EARLIEST_USABLE_YEAR_QUARTER`](#pudl.analysis.operational_characteristics.EARLIEST_USABLE_YEAR_QUARTER) | Earliest EPA CEMS year-quarter treated as usable for this analysis. |

## Functions

| [`_get_heat_rate_analysis_config`](#pudl.analysis.operational_characteristics._get_heat_rate_analysis_config)(→ dict[str, int])         | Extract heat rate analysis settings from Dagster asset config.                |
|-----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| [`_year_quarter_to_ordinal`](#pudl.analysis.operational_characteristics._year_quarter_to_ordinal)(→ int)                          | Convert a `YYYYqN` string into a zero-based quarter ordinal.                  |
| [`_ordinal_to_quarter_start`](#pudl.analysis.operational_characteristics._ordinal_to_quarter_start)(→ pandas.Timestamp)            | Convert a zero-based quarter ordinal into its first UTC timestamp.            |
| [`_ordinal_to_year_quarter`](#pudl.analysis.operational_characteristics._ordinal_to_year_quarter)(→ str)                          | Convert a zero-based quarter ordinal into its `YYYYqN` string.                |
| [`_missing_required_quarters`](#pudl.analysis.operational_characteristics._missing_required_quarters)(→ list[str])                  | List quarters in the trailing window ending at `target_year_quarter`.         |
| [`_select_target_year_quarters`](#pudl.analysis.operational_characteristics._select_target_year_quarters)(→ list[str])                | Choose the EPA CEMS year-quarters that each end a full analysis window.       |
| [`filter_cems_for_heat_rate_analysis`](#pudl.analysis.operational_characteristics.filter_cems_for_heat_rate_analysis)(→ polars.LazyFrame)   | Filter hourly EPA CEMS records to the configured analysis window.             |
| [`_add_run_id_expr`](#pudl.analysis.operational_characteristics._add_run_id_expr)(→ polars.Expr)                          | Build an expression assigning run IDs to consecutive hourly observations.     |
| [`assign_groupwise_load_factor_bins`](#pudl.analysis.operational_characteristics.assign_groupwise_load_factor_bins)(→ polars.DataFrame)    | Fully vectorized, per-unit equal-width load-factor binning.                   |
| [`summarize_ramp_rates`](#pudl.analysis.operational_characteristics.summarize_ramp_rates)(→ polars.DataFrame)                 | Summarize per-unit ramp rates using the steepest 5% of observed ramp-up/down. |
| [`handle_adjustment_in_cems`](#pudl.analysis.operational_characteristics.handle_adjustment_in_cems)(→ tuple[polars.LazyFrame, ...) | Filter CEMS data, computing derived columns if not adjusted.                  |
| [`prep_output_df`](#pudl.analysis.operational_characteristics.prep_output_df)(→ polars.DataFrame)                       | Set up aggregated output dataframe with empty calculated columns.             |
| [`compute_minimum_stable_bin`](#pudl.analysis.operational_characteristics.compute_minimum_stable_bin)(→ polars.DataFrame)           | Given a certain consecutive hour threshold, find runs with stable behavior.   |
| [`compute_heat_rate_at_max_load`](#pudl.analysis.operational_characteristics.compute_heat_rate_at_max_load)(→ polars.DataFrame)        | Compute the heat rate at the maximum load (by bin).                           |
| [`compute_min_stable_heat_rates`](#pudl.analysis.operational_characteristics.compute_min_stable_heat_rates)(→ polars.DataFrame)        | Compute the heat rate for the minimum stable run.                             |
| [`filter_for_min_stable_bin`](#pudl.analysis.operational_characteristics.filter_for_min_stable_bin)(→ polars.DataFrame)            | Filter out records below the minimum stable bin.                              |
| [`calculate_min_up_or_down_times`](#pudl.analysis.operational_characteristics.calculate_min_up_or_down_times)(→ polars.DataFrame)       | Calculate minimum up or down times.                                           |
| [`estimate_operational_characteristics_by_unit`](#pudl.analysis.operational_characteristics.estimate_operational_characteristics_by_unit)(...)        | Estimate operational characteristics for every EPA CEMS plant-unit pair.      |
| [`out_epacems__yearly_operational_characteristics`](#pudl.analysis.operational_characteristics.out_epacems__yearly_operational_characteristics)(...)     | Estimate EPA CEMS unit operational characteristics for every unit and year.   |

## Module Contents

### pudl.analysis.operational_characteristics.logger

### pudl.analysis.operational_characteristics.EARLIEST_USABLE_YEAR_QUARTER *= '1998q1'*

Earliest EPA CEMS year-quarter treated as usable for this analysis.

EPA CEMS’s first few years of reporting (1995-1997) are known to have poor and
inconsistent unit coverage, making them unsuitable for estimating these
operational characteristics. Quarters before this are treated as unavailable
for this analysis regardless of what’s actually present in the EPA CEMS data
config, so the earliest feasible `report_year` in production (a 3-year /
12-quarter trailing window) is 2000, not 1997.

### pudl.analysis.operational_characteristics.\_get_heat_rate_analysis_config(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]

Extract heat rate analysis settings from Dagster asset config.

### pudl.analysis.operational_characteristics.\_year_quarter_to_ordinal(year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [int](https://docs.python.org/3/library/functions.html#int)

Convert a `YYYYqN` string into a zero-based quarter ordinal.

### pudl.analysis.operational_characteristics.\_ordinal_to_quarter_start(ordinal: [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html#pandas.Timestamp)

Convert a zero-based quarter ordinal into its first UTC timestamp.

`operating_datetime_utc` is stored as a timezone-naive timestamp (already in
UTC), so this deliberately returns a naive `Timestamp` to compare against it.

### pudl.analysis.operational_characteristics.\_ordinal_to_year_quarter(ordinal: [int](https://docs.python.org/3/library/functions.html#int)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Convert a zero-based quarter ordinal into its `YYYYqN` string.

### pudl.analysis.operational_characteristics.\_missing_required_quarters(year_quarters: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], target_year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), num_quarters: [int](https://docs.python.org/3/library/functions.html#int)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

List quarters in the trailing window ending at `target_year_quarter`.

Specifically, the quarters in that window that are absent from
`year_quarters`. `EpaCemsDataConfig` only validates that each
configured year-quarter is a real partition and that there are no
duplicates – it does not require the list to be contiguous, so a
trailing window can have gaps even in a config that otherwise looks
reasonable.

### pudl.analysis.operational_characteristics.\_select_target_year_quarters(year_quarters: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], num_quarters: [int](https://docs.python.org/3/library/functions.html#int)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Choose the EPA CEMS year-quarters that each end a full analysis window.

The analysis produces one set of operational characteristics per calendar
year, computed from a trailing window of `num_quarters` quarters ending
in that year’s Q4. This returns the end-of-window year-quarter for every
calendar year that both:

- has its Q4 present in `year_quarters` (considering only quarters at or
  after [`EARLIEST_USABLE_YEAR_QUARTER`](#pudl.analysis.operational_characteristics.EARLIEST_USABLE_YEAR_QUARTER) as available at all), and
- has all `num_quarters` quarters of its trailing window available.

In production, with a 12-quarter window and EPA CEMS data reaching back to
the late 1990s, this yields one `YYYYq4` value per year from 2000 through
the most recent complete year.

The fast ETL and pytest configs load only a single EPA CEMS quarter and
set `num_quarters` to 1, so no Q4-ending window exists. In that case the
single latest usable year-quarter (e.g. `"2022q1"`) is returned as its
own one-quarter window, and the caller treats its calendar year as the
report year. This fallback quarter is returned as-is rather than coerced
to Q4, because the loaded data only covers that specific quarter.

* **Parameters:**
  * **year_quarters** – Every EPA CEMS `YYYYqN` partition available to the
    run, as configured in the Dagster settings. Need not be sorted or
    contiguous.
  * **num_quarters** – Length of the trailing window, in quarters, that each
    returned year-quarter must have fully available.
* **Returns:**
  Sorted list of `YYYYqN` year-quarters, one per analyzable calendar
  year, each usable as the `final_year_quarter` of a
  `num_quarters`-long window. Normally every entry ends in `q4`; in
  the single-quarter fast-ETL case the one entry is the loaded quarter.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – if no configured year-quarter is at or after
  [`EARLIEST_USABLE_YEAR_QUARTER`](#pudl.analysis.operational_characteristics.EARLIEST_USABLE_YEAR_QUARTER), or if no candidate
  year-quarter has its full trailing window available – e.g.
  `num_quarters` is larger than the usable EPA CEMS history.

### pudl.analysis.operational_characteristics.filter_cems_for_heat_rate_analysis(core_epacems_\_hourly_emissions: polars.LazyFrame, final_year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), num_quarters: [int](https://docs.python.org/3/library/functions.html#int), states: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → polars.LazyFrame

Filter hourly EPA CEMS records to the configured analysis window.

* **Parameters:**
  * **core_epacems_\_hourly_emissions** – Hourly CEMS emissions and gross load data.
  * **final_year_quarter** – Final EPA CEMS year-quarter (e.g. `"2024q1"`) to
    include in the analysis.
  * **num_quarters** – Number of historical quarters to include, counting backward
    from `final_year_quarter`.
  * **states** – Optional list of two-letter state abbreviations to include.
    Default is None, which will grab all states.
* **Returns:**
  Hourly EPA CEMS records filtered to the requested quarters and states.

### pudl.analysis.operational_characteristics.\_add_run_id_expr(unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], state_col: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → polars.Expr

Build an expression assigning run IDs to consecutive hourly observations.

Assumes the frame is already sorted by `unit_cols` (and, implicitly,
`operating_datetime_utc`), since it relies on `.shift()` to compare each row
to its immediate predecessor.

### pudl.analysis.operational_characteristics.assign_groupwise_load_factor_bins(cems_working: polars.LazyFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], load_factor_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.DataFrame

Fully vectorized, per-unit equal-width load-factor binning.

This uses polars but is replicating the `pandas.cut` methodology. This results
in a `load_factor_bin` `column with 10 unique values with a two dimensional
structure as a datatype. The left value of the structure is the lower bound of the
load factors within that bin and the right value it the higher bound. Using the
``load_factor_bin` we also assign `load_factor_bin_rank` which is the lower
bound of the lowest `load_factor_bin`.

This function uses polars but is attempting to directly reproduce
`pandas.cut(bins=10, right=True, include_lowest=False)` within each unit group.
The pandas methodology computes 10 equal-width bins spanning that unit’s own
observed min/max `load_factor_col` (`width = (max - min) / 10`), except that
only the *lowest* bin’s left edge is padded by 0.1% of the range (or by 0.001 when
the range is zero) so that the unit’s minimum observation falls inside the
first (right-closed) bin rather than outside every bin – matching pandas’
`_bins_to_cuts` behavior of shifting only `bins[0]`, not redistributing
the padding across all ten bins.

### pudl.analysis.operational_characteristics.summarize_ramp_rates(cems_with_stable_bins: polars.DataFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], generation_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.DataFrame

Summarize per-unit ramp rates using the steepest 5% of observed ramp-up/down.

This bins on `ramp_rate` (change in `load_factor`), not `load_factor` itself
and uses 20 equal-count (quantile) bins. Only the bottom and top bins (the steepest
5% of downward and upward ramps, respectively) are actually used, via
`head`/`tail` on the sorted values rather than an explicit bin column.

### pudl.analysis.operational_characteristics.handle_adjustment_in_cems(cems: polars.LazyFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], adjusted: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[polars.LazyFrame, [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]

Filter CEMS data, computing derived columns if not adjusted.

This enables us to adjust the load factor based using net generation
instead of gross generation. Adjusting the load factor is not yet
implemented fully. A draft is below add_adjusted_net_generation_to_cems.

This returns a lazframe and a dictionary with keys of the column references
and values of the column names to use.

TODO: Consider simplification or use of a dataclass or other lightweight data
structure. Implement changes when we implement
add_adjusted_net_generation_to_cems below.

### pudl.analysis.operational_characteristics.prep_output_df(cems: polars.DataFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], max_load_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.DataFrame

Set up aggregated output dataframe with empty calculated columns.

Every unit gets a row here, even ones that don’t have enough distinct load
factors to bin (e.g. constant-load units) – those come back all-null except for
identifying columns and max load. Downstream steps merge their real values on top
of this shell, so every unit is guaranteed to appear in the final output.

### pudl.analysis.operational_characteristics.compute_minimum_stable_bin(binned_cems: polars.DataFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], min_stable_consecutive_hours: [int](https://docs.python.org/3/library/functions.html#int)) → polars.DataFrame

Given a certain consecutive hour threshold, find runs with stable behavior.

This function determines the minimum stable load load factor bin, which means the
lowest load factor which we see instances of consecutive running.

For every record above the first load_factor_bin (aka when a unit is effectively
off), first calculate how long any given “run” is. A “run” here is defined as
a set of consecutive hours that are within the same `load_factor_bin` within a
given unit.

Once we know how long all the runs are, we find all of the runs that are longer
than `min_stable_consecutive_hours` and we find the `load_factor_bin` which
corresponds to the lowest `load_factor_bin` to get the minimum stable bin.

### pudl.analysis.operational_characteristics.compute_heat_rate_at_max_load(heat_rate_input: polars.DataFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], heat_rate_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.DataFrame

Compute the heat rate at the maximum load (by bin).

### pudl.analysis.operational_characteristics.compute_min_stable_heat_rates(heat_rate_input: polars.DataFrame, min_stable_bins: polars.DataFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], heat_rate_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.DataFrame

Compute the heat rate for the minimum stable run.

### pudl.analysis.operational_characteristics.filter_for_min_stable_bin(df: polars.DataFrame) → polars.DataFrame

Filter out records below the minimum stable bin.

### pudl.analysis.operational_characteristics.calculate_min_up_or_down_times(output: polars.DataFrame, cems_with_stable_bins: polars.DataFrame, unit_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], up_or_down: Literal['up', 'down']) → polars.DataFrame

Calculate minimum up or down times.

Hourly data points are considered “up” when the `load_factor_bin` is greater than
the `min_stable_bin` (calculated in [`compute_minimum_stable_bin()`](#pudl.analysis.operational_characteristics.compute_minimum_stable_bin)). Runs are
considered “down” when there is no load_factor_bin (which is equivalent to having no
load during that hour).

### pudl.analysis.operational_characteristics.estimate_operational_characteristics_by_unit(cems: polars.LazyFrame, min_stable_consecutive_hours: [int](https://docs.python.org/3/library/functions.html#int), adjusted: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → polars.DataFrame

Estimate operational characteristics for every EPA CEMS plant-unit pair.

Everything through the initial load-factor binning step is lazily evaluated
(see [`assign_groupwise_load_factor_bins()`](#pudl.analysis.operational_characteristics.assign_groupwise_load_factor_bins)). Everything after that operates
on the resulting eager `DataFrame`, in a fully vectorized manner across every
unit at once – there’s no per-unit or per-batch Python looping, and no
`pandas` fallback.

### pudl.analysis.operational_characteristics.out_epacems_\_yearly_operational_characteristics(context: [dagster.AssetExecutionContext](https://docs.dagster.io/api/dagster/execution/#dagster.AssetExecutionContext), core_epacems_\_hourly_emissions: polars.LazyFrame) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Estimate EPA CEMS unit operational characteristics for every unit and year.

Computes one set of characteristics per state per calendar year that has a
full trailing window of EPA CEMS data available (see
[`_select_target_year_quarters()`](#pudl.analysis.operational_characteristics._select_target_year_quarters)), looping over states within each year.
Each `(year, state)` pair’s hourly CEMS data is filtered, reduced down to a
handful of per-unit summary rows, and immediately discarded, so peak memory
stays bounded by a single state’s window (observed ~16 GB for CA or TX) no
matter how many years get processed. All of the resulting per-unit summary
rows – on the order of 100,000 total across every year and state, versus
billions of hourly input records – are concatenated once at the very end.
