# pudl.transform.gridpathratoolkit

Transformations of the GridPath RA Toolkit renewable generation profiles.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure. The generator aggregation group
association tables for various technology types are also concatenated together.

## Functions

| [`_transform_capacity_factors`](#pudl.transform.gridpathratoolkit._transform_capacity_factors)(→ pandas.DataFrame)                                            | Basic transformations that can be applied to many profiles.           |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`out_gridpathratoolkit__hourly_available_capacity_factor`](#pudl.transform.gridpathratoolkit.out_gridpathratoolkit__hourly_available_capacity_factor)(...)   | Transform raw GridPath RA Toolkit renewable generation profiles.      |
| [`_transform_aggs`](#pudl.transform.gridpathratoolkit._transform_aggs)(→ pandas.DataFrame)                                                                    | Transform raw GridPath RA Toolkit generator aggregations.             |
| [`core_gridpathratoolkit__assn_generator_aggregation_group`](#pudl.transform.gridpathratoolkit.core_gridpathratoolkit__assn_generator_aggregation_group)(...) | Transform and combine raw GridPath RA Toolkit generator aggregations. |

## Module Contents

### pudl.transform.gridpathratoolkit.\_transform_capacity_factors(capacity_factors: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), utc_offset: [pandas.Timedelta](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timedelta.html#pandas.Timedelta)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Basic transformations that can be applied to many profiles.

- Construct a datetime column and adjust it to be in UTC.
- Reshape the table from wide to tidy format.
- Name columns appropriately.

### pudl.transform.gridpathratoolkit.out_gridpathratoolkit_\_hourly_available_capacity_factor(raw_gridpathratoolkit_\_aggregated_extended_solar_capacity: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_gridpathratoolkit_\_aggregated_extended_wind_capacity: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform raw GridPath RA Toolkit renewable generation profiles.

Concatenates the solar and wind capacity factors into a single table and turns the
aggregation key into a categorical column to save space.

Note that this transform is a bit unusual, in that it is producing a highly
processed output table. That’s because we’re working backwards from an archived
finished product to be able to provide a minimum viable product. Our intent is to
integrate or reimplement the steps required to produce this output table from
less processed original inputs in the future.

### pudl.transform.gridpathratoolkit.\_transform_aggs(raw_agg: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform raw GridPath RA Toolkit generator aggregations.

- split EIA_UniqueID into plant + generator IDs
- rename columns to use PUDL conventions
- verify that split-out plant IDs always match reported plant IDs
- Set column dtypes

### pudl.transform.gridpathratoolkit.core_gridpathratoolkit_\_assn_generator_aggregation_group(raw_gridpathratoolkit_\_wind_capacity_aggregations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_gridpathratoolkit_\_solar_capacity_aggregations: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform and combine raw GridPath RA Toolkit generator aggregations.
