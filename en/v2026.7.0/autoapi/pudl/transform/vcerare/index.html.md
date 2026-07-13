# pudl.transform.vcerare

Transformations of the Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.

## Attributes

| [`logger`](#pudl.transform.vcerare.logger)   |    |
|----------------------------------------------|----|

## Functions

| [`_prep_lat_long_fips_df`](#pudl.transform.vcerare._prep_lat_long_fips_df)(→ pandas.DataFrame)                                    | Prep the lat_long_fips table to merge into the capacity factor tables.                      |
|-----------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| [`_add_time_cols`](#pudl.transform.vcerare._add_time_cols)(→ pandas.DataFrame)                                                    | Add datetime and hour_of_year columns.                                                      |
| [`_drop_city_cols`](#pudl.transform.vcerare._drop_city_cols)(→ pandas.DataFrame)                                                  | Drop city columns from the capacity factor tables before stacking.                          |
| [`_stack_cap_fac_df`](#pudl.transform.vcerare._stack_cap_fac_df)(→ pandas.DataFrame)                                              | Function to transform each capacity factor table individually to save memory.               |
| [`_make_cap_fac_frac`](#pudl.transform.vcerare._make_cap_fac_frac)(→ pandas.DataFrame)                                            | Make the capacity factor column a fraction instead of a percentage.                         |
| [`_check_for_valid_counties`](#pudl.transform.vcerare._check_for_valid_counties)(→ pandas.DataFrame)                              | Make sure the state_county values show up in the FIPS table.                                |
| [`_standardize_census_names`](#pudl.transform.vcerare._standardize_census_names)(vce_fips_df, census_pep_data)                    | Make sure that the VCE place names correspond to the latest census vintage.                 |
| [`_clip_unexpected_2016_pv_capacity`](#pudl.transform.vcerare._clip_unexpected_2016_pv_capacity)(df, df_name, year)               | Handle unexpectedly large PV capacity values in 2016.                                       |
| [`_spot_fix_great_lakes_values`](#pudl.transform.vcerare._spot_fix_great_lakes_values)(→ pandas.Series)                           | Normalize spelling of great lakes in cell values.                                           |
| [`_spot_fix_great_lakes_columns`](#pudl.transform.vcerare._spot_fix_great_lakes_columns)(→ pandas.DataFrame)                      | Normalize spelling of great lakes in column names.                                          |
| [`one_year_hourly_available_capacity_factor`](#pudl.transform.vcerare.one_year_hourly_available_capacity_factor)(→ dict[str, ...) | Transform raw Vibrant Clean Energy renewable generation profiles.                           |
| [`merge_all_vce_tables`](#pudl.transform.vcerare.merge_all_vce_tables)(→ pudl.helpers.ParquetData)                                | Merge cleaned VCE tables to create final `out` table and write as partitioned parquet file. |
| [`out_vcerare__hourly_available_capacity_factor`](#pudl.transform.vcerare.out_vcerare__hourly_available_capacity_factor)(...)     | Transform raw Vibrant Clean Energy renewable generation profiles.                           |

## Module Contents

### pudl.transform.vcerare.logger

### pudl.transform.vcerare.\_prep_lat_long_fips_df(raw_vcerare_\_lat_lon_fips: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Prep the lat_long_fips table to merge into the capacity factor tables.

Prep entails making sure the formatting and column names match those in the
capacity factor tables, adding 0s to the beginning of FIPS codes with 4 values,
and making separate county/subregion and state columns. Instead of pulling state
from the county_state column, we use the first two digits of the county FIPS ID
to pull in state code values from the census data stored in POLITICAL_SUBDIVISIONS.

The county portion of the county_state column does not map directly to FIPS ID.
Some of the county names are actually subregions like cities or lakes. For this
reason we’ve named the column place_name and it should be considered
part of the primary key. There are several instances of multiple subregions that
map to a single county_id_fips value.

### pudl.transform.vcerare.\_add_time_cols(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), df_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add datetime and hour_of_year columns.

This function adds a datetime column and a hour_of_year column.
The datetime column is important for merging the data with other
data, and the hour_of_year 1-8760 column is important for modeling
purposes. The report_year column is also helpful for filtering,
so we keep all three!

For leap years (2020), December 31st is excluded.

### pudl.transform.vcerare.\_drop_city_cols(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), df_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Drop city columns from the capacity factor tables before stacking.

We do this early since the columns can be dropped by name here, and we don’t have to
search through all of the stacked rows to find matching records.

### pudl.transform.vcerare.\_stack_cap_fac_df(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), df_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Function to transform each capacity factor table individually to save memory.

The main transforms are turning county/subregion columns into county/subregion rows
and renaming columns to be more human-readable and compatible with the FIPS df
that will get merged in.

This function is intended to save memory by being applied to each individual
capacity factor table rather than the giant combined one.

### pudl.transform.vcerare.\_make_cap_fac_frac(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), df_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Make the capacity factor column a fraction instead of a percentage.

This step happens before the table gets stacked to save memory.

### pudl.transform.vcerare.\_check_for_valid_counties(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), clean_fips_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), df_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Make sure the state_county values show up in the FIPS table.

This step happens before the table gets stacked to save memory.

### pudl.transform.vcerare.\_standardize_census_names(vce_fips_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), census_pep_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Make sure that the VCE place names correspond to the latest census vintage.

This function solves a problem of slight inconsistencies between Census PEP data and
the county names provided by VCE RARE. We join the latest version of the Census PEP
data onto the VCE RARE lat lon FIPS dataframe by FIPS ID, and then we take the
Census PEP version of the county name wherever these values differ.

Because the county_state_name column corresponds to the column names of each
spreadsheet, we avoid altering it and only update the place_name column.
In the final dataframe, we join all the dataframes on the original county_state_name
value and drop this column, leaving only an updated place_name value in the final
output.

The function returns the cleaned VCE FIPS dataframe with updated place_name,
as compared to the original VCE RARE values. Lakes and city names are not updated,
as lakes don’t have comparable values in the Census PEP data and we drop the city values.

### pudl.transform.vcerare.\_clip_unexpected_2016_pv_capacity(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), df_name: [str](https://docs.python.org/3/library/stdtypes.html#str), year: [int](https://docs.python.org/3/library/functions.html#int))

Handle unexpectedly large PV capacity values in 2016.

In 2016, there are a few values for PV capacity factors that exceed the maximum
allowed values noted in the read-me (110%).
should be zeroed out, according to correspondence with the data provider.
This function narrowly zeroes out these nulls, expecting that the rest of the
data should conform to the expectation of no-null values.

### pudl.transform.vcerare.\_spot_fix_great_lakes_values(sr: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Normalize spelling of great lakes in cell values.

### pudl.transform.vcerare.\_spot_fix_great_lakes_columns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Normalize spelling of great lakes in column names.

### pudl.transform.vcerare.one_year_hourly_available_capacity_factor(year: [int](https://docs.python.org/3/library/functions.html#int), fips_df_census: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_vcerare_\_fixed_solar_pv_lat_upv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_vcerare_\_offshore_wind_power_140m: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_vcerare_\_onshore_wind_power_100m: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)]

Transform raw Vibrant Clean Energy renewable generation profiles.

Concatenates the solar and wind capacity factors into a single table and turns
the columns for each county or subregion into a single place_name column.

### pudl.transform.vcerare.merge_all_vce_tables(transformed_tables: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)], vce_fips_table: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), year: [int](https://docs.python.org/3/library/functions.html#int)) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Merge cleaned VCE tables to create final `out` table and write as partitioned parquet file.

### pudl.transform.vcerare.out_vcerare_\_hourly_available_capacity_factor(context, raw_vcerare_\_fixed_solar_pv_lat_upv: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)], raw_vcerare_\_offshore_wind_power_140m: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)], raw_vcerare_\_onshore_wind_power_100m: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)], raw_vcerare_\_lat_lon_fips: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → polars.LazyFrame

Transform raw Vibrant Clean Energy renewable generation profiles.

Concatenates the solar and wind capacity factors into a single table and turns
the columns for each county or subregion into a single place_name column.
Asset will process 1 year of data at a time to limit peak memory usage.
