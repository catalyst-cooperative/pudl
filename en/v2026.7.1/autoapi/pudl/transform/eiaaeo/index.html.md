# pudl.transform.eiaaeo

Transform raw AEO tables into normalized assets.

Raw AEO tables often contain many different types of data which are split out
along different dimensions. For example, one table may contain generation
split out by fuel type as well as prices split out by service category.

As a result, we need to split these large tables into smaller tables that have
more uniform data, which we do by filtering the large table to its relevant
subsets, and then transforming some human-readable string fields into useful
metadata fields.

## Attributes

| [`BASE_AEO_CATEGORIES`](#pudl.transform.eiaaeo.BASE_AEO_CATEGORIES)   |    |
|-----------------------------------------------------------------------|----|
| [`check_specs`](#pudl.transform.eiaaeo.check_specs)                   |    |
| [`_checks`](#pudl.transform.eiaaeo._checks)                           |    |

## Classes

| [`AeoCheckSpec`](#pudl.transform.eiaaeo.AeoCheckSpec)   | Define some simple checks that can run on any AEO asset.   |
|---------------------------------------------------------|------------------------------------------------------------|

## Functions

| [`__sanitize_string`](#pudl.transform.eiaaeo.__sanitize_string)(→ pandas.Series)                                                                                                     |                                                                                                           |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| [`get_series_info`](#pudl.transform.eiaaeo.get_series_info)(→ pandas.DataFrame)                                                                                                      | Break human-readable series name into machine-readable fields.                                            |
| [`get_category_info`](#pudl.transform.eiaaeo.get_category_info)(→ pandas.Series)                                                                                                     | Break human-readable category name into machine-readable fields.                                          |
| [`subtotals_match_reported_totals_ratio`](#pudl.transform.eiaaeo.subtotals_match_reported_totals_ratio)(→ float)                                                                     | When subtotals and totals are reported in the same column, check their sums.                              |
| [`series_sum_ratio`](#pudl.transform.eiaaeo.series_sum_ratio)(→ float)                                                                                                               | Find how well multiple columns sum to another column.                                                     |
| [`filter_enrich_sanitize`](#pudl.transform.eiaaeo.filter_enrich_sanitize)(→ pandas.DataFrame)                                                                                        | Basic cleaning steps common to all AEO tables.                                                            |
| [`_collect_totals`](#pudl.transform.eiaaeo._collect_totals)(→ pandas.DataFrame)                                                                                                      | Various columns have different names for their "total" fact.                                              |
| [`unstack`](#pudl.transform.eiaaeo.unstack)(df, eventual_pk)                                                                                                                         | Unstack the values by the various variable names provided.                                                |
| [`core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology`](#pudl.transform.eiaaeo.core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology)(...) | Projected net summer generation capacity and additions/retirements.                                       |
| [`core_eiaaeo__yearly_projected_electric_sales`](#pudl.transform.eiaaeo.core_eiaaeo__yearly_projected_electric_sales)(...)                                                           | Projected electricity sales by customer class.                                                            |
| [`core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type`](#pudl.transform.eiaaeo.core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type)(...)   | Projected generation capacity + gross generation in end-use sectors.                                      |
| [`core_eiaaeo__yearly_projected_energy_use_by_sector_and_type`](#pudl.transform.eiaaeo.core_eiaaeo__yearly_projected_energy_use_by_sector_and_type)(...)                             | Projected energy use for commercial, electric power, industrial, residential, and transportation sectors. |
| [`core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type`](#pudl.transform.eiaaeo.core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type)(...)               | Projected fuel cost for the electric power sector.                                                        |
| [`make_check`](#pudl.transform.eiaaeo.make_check)(→ dagster.AssetChecksDefinition)                                                                                                   | Turn the AeoCheckSpec into an actual Dagster asset check.                                                 |

## Module Contents

### pudl.transform.eiaaeo.\_\_sanitize_string(series: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

### pudl.transform.eiaaeo.get_series_info(series_name: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Break human-readable series name into machine-readable fields.

The series name contains several comma-separated fields: the variable,
the region, the case, and the report year.

The variable then contains its own colon-separated fields: a general topic,
a less general subtopic, and specific variable name. It may also contain a
fourth field for a specific dimension such as fuel type.

### pudl.transform.eiaaeo.get_category_info(category_name: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Break human-readable category name into machine-readable fields.

Fortunately the only field we’re pulling out of the category so far is the
region, which is the last of two comma-separated fields.

### pudl.transform.eiaaeo.subtotals_match_reported_totals_ratio(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pk: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], fact_columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], dimension_column: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [float](https://docs.python.org/3/library/functions.html#float)

When subtotals and totals are reported in the same column, check their sums.

Group by some key, then check that within each group the non-`"total"`
values sum up to the corresponding `"total"` value.

Checks the list of fact columns to in aggregate, but if you want to check
that *each* column sums up correctly, individually, you can call this
function once per column.

TODO 2024-05-06: it may make sense to pass the threshold into this
function, which would clean up the call sites.

* **Parameters:**
  * **df** – the dataframe to investigate
  * **pk** – the key to group facts by
  * **fact_columns** – the columns containing facts you’d like to sum
  * **dimension_column** – the column which tells you if a fact is a sub-total
    or a total.
* **Returns:**
  The ratio of reported totals that are np.isclose() to the sum of their
  component parts.

### pudl.transform.eiaaeo.series_sum_ratio(summands: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), total: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [float](https://docs.python.org/3/library/functions.html#float)

Find how well multiple columns sum to another column.

* **Parameters:**
  * **summands** – the columns that should sum to total
  * **total** – the target total column
* **Returns:**
  the ratio of values in `total` that are np.isclose() to the sum of
  `summands`.

### pudl.transform.eiaaeo.filter_enrich_sanitize(raw_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), relevant_series_names: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Basic cleaning steps common to all AEO tables.

1. Filter the AEO rows based on the series name
2. Break the series name and category names into useful fields
3. Sanitize strings & turn data values into a numeric field
4. Make some defensive checks about data from multiple sources that
   *should* agree.

### pudl.transform.eiaaeo.\_collect_totals(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), total_colname='dimension') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Various columns have different names for their “total” fact.

This combines them into one “total” dimension.

### pudl.transform.eiaaeo.unstack(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), eventual_pk: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)])

Unstack the values by the various variable names provided.

### pudl.transform.eiaaeo.core_eiaaeo_\_yearly_projected_generation_in_electric_sector_by_technology(raw_eiaaeo_\_electric_power_projections_regional)

Projected net summer generation capacity and additions/retirements.

### pudl.transform.eiaaeo.core_eiaaeo_\_yearly_projected_electric_sales(raw_eiaaeo_\_electric_power_projections_regional)

Projected electricity sales by customer class.

### pudl.transform.eiaaeo.core_eiaaeo_\_yearly_projected_generation_in_end_use_sectors_by_fuel_type(raw_eiaaeo_\_electric_power_projections_regional)

Projected generation capacity + gross generation in end-use sectors.

This includes data that’s reported by fuel type and ignores data that’s
only reported at the system-wide level, such as total generation, sales to
grid, and generation for own use. Those three facts are reported in
core_eiaaeo_\_yearly_projected_generation_in_end_use_sectors instead.

### pudl.transform.eiaaeo.core_eiaaeo_\_yearly_projected_energy_use_by_sector_and_type(raw_eiaaeo_\_energy_consumption_by_sector_and_source)

Projected energy use for commercial, electric power, industrial, residential, and transportation sectors.

The “Energy Use” series in Table 2 which track figures by sector do not always
define each type of usage the same way across sectors. There is detailed
information about what is included or excluded in each usage type for each sector
in the footnotes of the EIA’s online AEO data browser:

> [https://www.eia.gov/outlooks/aeo/data/browser/#/](https://www.eia.gov/outlooks/aeo/data/browser/#/)?id=2-AEO2023

The data browser also gives some visibility into the tricky system of subtotals
within the Energy Use series. To identify and map subtotal usage types, look for
the following features in the data browser display: subtotal series are displayed
indented, and include all lines above them which are one level out, up to the
next indented line. Delivered Energy and Total are special cases which include
those plus all subtotals above. In this way, “Delivered Energy” includes
purchased electricity, renewable energy, and an array of fuels based on sector,
and explicitly excludes electricity-related losses.

AEO Energy Use figures are variously referred to as delivered energy, energy
consumption, energy use, and energy demand, depending on which usage types are
being discussed, and which org and which document is describing them. In PUDL we
say energy use or energy consumption.

### pudl.transform.eiaaeo.core_eiaaeo_\_yearly_projected_fuel_cost_in_electric_sector_by_type(raw_eiaaeo_\_electric_power_projections_regional)

Projected fuel cost for the electric power sector.

Includes 2022, 2024, 2025, and nominal US dollars per million BTU.

In future report years, the base year for the real cost will change, so we
store that base year as well.

### *class* pudl.transform.eiaaeo.AeoCheckSpec

Define some simple checks that can run on any AEO asset.

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### asset *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

#### category_counts *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]*

### pudl.transform.eiaaeo.BASE_AEO_CATEGORIES

### pudl.transform.eiaaeo.check_specs

### pudl.transform.eiaaeo.make_check(spec: [AeoCheckSpec](#pudl.transform.eiaaeo.AeoCheckSpec)) → [dagster.AssetChecksDefinition](https://docs.dagster.io/api/dagster/asset-checks/#dagster.AssetChecksDefinition)

Turn the AeoCheckSpec into an actual Dagster asset check.

### pudl.transform.eiaaeo.\_checks
