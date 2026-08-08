# pudl.analysis.operational_characteristics

Use EPA CEMS and EIA data to estimate generator operational characteristics.

Starting from hourly EPA CEMS gross load and fuel heat content, this module estimates,
for every plant-unit, a single trailing-window snapshot of: minimum stable load,
minimum up/down time, heat rate at maximum and minimum stable load, and ramp-up/-down
rate. These are derived by combining several independent per-unit calculations –
load-factor binning, run-length detection, and ramp-rate summarization – into one
output row per unit via [`estimate_operational_characteristics_by_unit()`](#pudl.analysis.operational_characteristics.estimate_operational_characteristics_by_unit).

See [Generator Operational Characteristics](../../../../methodology/operational_characteristics.md) for a longer prose explanation.

## Attributes

| [`logger`](#pudl.analysis.operational_characteristics.logger)   |    |
|-----------------------------------------------------------------|----|

## Functions

| [`_get_heat_rate_analysis_config`](#pudl.analysis.operational_characteristics._get_heat_rate_analysis_config)(→ dict[str, int])                      | Extract heat rate analysis settings from Dagster asset config.                |
|------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| [`_year_quarter_to_ordinal`](#pudl.analysis.operational_characteristics._year_quarter_to_ordinal)(→ int)                                             | Convert a `YYYYqN` string into a zero-based quarter ordinal.                  |
| [`_ordinal_to_quarter_start`](#pudl.analysis.operational_characteristics._ordinal_to_quarter_start)(→ pandas.Timestamp)                              | Convert a zero-based quarter ordinal into its first UTC timestamp.            |
| [`_ordinal_to_year_quarter`](#pudl.analysis.operational_characteristics._ordinal_to_year_quarter)(→ str)                                             | Convert a zero-based quarter ordinal into its `YYYYqN` string.                |
| [`_assert_required_quarters_available`](#pudl.analysis.operational_characteristics._assert_required_quarters_available)(→ None)                      | Raise if the configured EPA CEMS quarters don't cover the trailing window.    |
| [`_select_target_year_quarter`](#pudl.analysis.operational_characteristics._select_target_year_quarter)(→ str)                                       | Pick the year-quarter to treat as the end of the analysis window.             |
| [`filter_cems_for_heat_rate_analysis`](#pudl.analysis.operational_characteristics.filter_cems_for_heat_rate_analysis)(→ polars.LazyFrame)            | Filter hourly EPA CEMS records to the configured analysis window.             |
| [`_add_run_id_expr`](#pudl.analysis.operational_characteristics._add_run_id_expr)(→ polars.Expr)                                                     | Build an expression assigning run IDs to consecutive hourly observations.     |
| [`assign_groupwise_load_factor_bins`](#pudl.analysis.operational_characteristics.assign_groupwise_load_factor_bins)(→ polars.DataFrame)              | Fully vectorized, per-unit equal-width load-factor binning.                   |
| [`summarize_ramp_rates`](#pudl.analysis.operational_characteristics.summarize_ramp_rates)(→ polars.DataFrame)                                        | Summarize per-unit ramp rates using the steepest 5% of observed ramp-up/down. |
| [`handle_adjustment_in_cems`](#pudl.analysis.operational_characteristics.handle_adjustment_in_cems)(→ tuple[polars.LazyFrame, ...)                   | Filter CEMS data, computing derived columns if not adjusted.                  |
| [`prep_output_df`](#pudl.analysis.operational_characteristics.prep_output_df)(→ polars.DataFrame)                                                    | Set up aggregated output dataframe with empty calculated columns.             |
| [`compute_minimum_stable_bin`](#pudl.analysis.operational_characteristics.compute_minimum_stable_bin)(→ polars.DataFrame)                            | Given a certain consecutive hour threshold, find runs with stable behavior.   |
| [`compute_heat_rate_at_max_load`](#pudl.analysis.operational_characteristics.compute_heat_rate_at_max_load)(→ polars.DataFrame)                      | Compute the heat rate at the maximum load (by bin).                           |
| [`compute_min_stable_heat_rates`](#pudl.analysis.operational_characteristics.compute_min_stable_heat_rates)(→ polars.DataFrame)                      | Compute the heat rate for the minimum stable run.                             |
| [`filter_for_min_stable_bin`](#pudl.analysis.operational_characteristics.filter_for_min_stable_bin)(→ polars.DataFrame)                              | Filter out records below the minimum stable bin.                              |
| [`calculate_min_up_or_down_times`](#pudl.analysis.operational_characteristics.calculate_min_up_or_down_times)(→ polars.DataFrame)                    | Calculate minimum up or down times.                                           |
| [`estimate_operational_characteristics_by_unit`](#pudl.analysis.operational_characteristics.estimate_operational_characteristics_by_unit)(...)       | Estimate operational characteristics for every EPA CEMS plant-unit pair.      |
| [`out_epacems__yearly_operational_characteristics`](#pudl.analysis.operational_characteristics.out_epacems__yearly_operational_characteristics)(...) | Estimate EPA CEMS unit operational characteristics for every unit.            |

## Module Contents

### pudl.analysis.operational_characteristics.logger

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

### pudl.analysis.operational_characteristics.\_assert_required_quarters_available(year_quarters: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], target_year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str), num_quarters: [int](https://docs.python.org/3/library/functions.html#int)) → [None](https://docs.python.org/3/library/constants.html#None)

Raise if the configured EPA CEMS quarters don’t cover the trailing window.

`EpaCemsDataConfig` only validates that each configured year-quarter is a
real partition and that there are no duplicates – it does not require the
list to be contiguous. Nothing downstream would otherwise notice a gap:
[`_select_target_year_quarter()`](#pudl.analysis.operational_characteristics._select_target_year_quarter) only needs a max(), and
[`filter_cems_for_heat_rate_analysis()`](#pudl.analysis.operational_characteristics.filter_cems_for_heat_rate_analysis) filters purely by timestamp
range, with no visibility into which quarters were actually requested. This
check makes a missing or discontinuous configuration fail loudly instead of
silently producing estimates from a partial window.

### pudl.analysis.operational_characteristics.\_select_target_year_quarter(year_quarters: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Pick the year-quarter to treat as the end of the analysis window.

Prefers the most recent year-quarter ending in Q4 (i.e. the most recent
*complete* calendar year), which reproduces the historical whole-year
behavior of this analysis in production. Falls back to the single most
recent year-quarter of any kind when no Q4 is present – e.g. in the fast
ETL / CI, which only has a single quarter of EPA CEMS data and can’t
produce a Q4-ending window at all.

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

Estimate EPA CEMS unit operational characteristics for every unit.
