# pudl.output.eia930

Functions for compiling derived aspects of the EIA 930 data.

For a narrative overview of the timeseries imputation process, see the documentation
at [Timeseries Imputation](../../../../methodology/timeseries_imputation.md)

## Attributes

| [`imputed_combined_demand_assets`](#pudl.output.eia930.imputed_combined_demand_assets)   |    |
|------------------------------------------------------------------------------------------|----|

## Functions

| [`_add_timezone`](#pudl.output.eia930._add_timezone)(→ pandas.DataFrame)                                               |                                                                                        |
|------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|
| [`_out_eia930__hourly_operations`](#pudl.output.eia930._out_eia930__hourly_operations)(→ pandas.DataFrame)             | Adds timezone column and combined ID with BA/subregion used for imputation.            |
| [`_out_eia930__hourly_subregion_demand`](#pudl.output.eia930._out_eia930__hourly_subregion_demand)(→ pandas.DataFrame) | Adds timezone column and combined ID with BA/subregion used for imputation.            |
| [`_years_from_context`](#pudl.output.eia930._years_from_context)(→ list[int])                                          |                                                                                        |
| [`_out_eia930__combined_demand`](#pudl.output.eia930._out_eia930__combined_demand)(→ pandas.DataFrame)                 | Combine subregion and BA demand into a single DataFrame to perform imputation.         |
| [`split_ba_subregion_demand`](#pudl.output.eia930.split_ba_subregion_demand)(...)                                      | Split combined imputed demand into separate BA/subregion tables.                       |
| [`out_eia930__hourly_aggregated_demand`](#pudl.output.eia930.out_eia930__hourly_aggregated_demand)(→ pandas.DataFrame) | Aggregate imputed demand from the BA level to region, interconnect, and contiguous US. |

## Module Contents

### pudl.output.eia930.\_add_timezone(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_codes_balancing_authorities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.output.eia930.\_out_eia930_\_hourly_operations(core_eia930_\_hourly_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_codes_balancing_authorities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Adds timezone column and combined ID with BA/subregion used for imputation.

### pudl.output.eia930.\_out_eia930_\_hourly_subregion_demand(core_eia930_\_hourly_subregion_demand: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_codes_balancing_authorities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Adds timezone column and combined ID with BA/subregion used for imputation.

### pudl.output.eia930.\_years_from_context(context) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]

### pudl.output.eia930.\_out_eia930_\_combined_demand(\_out_eia930_\_hourly_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_out_eia930_\_hourly_subregion_demand: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Combine subregion and BA demand into a single DataFrame to perform imputation.

### pudl.output.eia930.imputed_combined_demand_assets

### pudl.output.eia930.split_ba_subregion_demand(\_out_eia930_\_combined_imputed_demand: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia930_\_hourly_subregion_demand: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia930_\_hourly_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Split combined imputed demand into separate BA/subregion tables.

### pudl.output.eia930.out_eia930_\_hourly_aggregated_demand(out_eia930_\_hourly_operations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), core_eia_\_codes_balancing_authorities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Aggregate imputed demand from the BA level to region, interconnect, and contiguous US.
