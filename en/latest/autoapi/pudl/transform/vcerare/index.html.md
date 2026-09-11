# pudl.transform.vcerare

Transformations of the Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset.

Wind and solar profiles are extracted separately, but concatenated into a single table
in this module, as they have exactly the same structure.

## Attributes

| [`logger`](#pudl.transform.vcerare.logger)         |    |
|-----------------------------------------------------------------|----|
| [`HOURLY_ID_COLS`](#pudl.transform.vcerare.HOURLY_ID_COLS) |    |

## Functions

| [`_prep_lat_long_fips_df`](#pudl.transform.vcerare._prep_lat_long_fips_df)(→ pandas.DataFrame)                 | Prep the lat_long_fips table to merge into the capacity factor tables.      |
|-------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`_stack_cap_fac_df`](#pudl.transform.vcerare._stack_cap_fac_df)(→ polars.LazyFrame)                      | Reshape one wide capacity factor table from county columns to county rows.  |
| [`_add_time_cols`](#pudl.transform.vcerare._add_time_cols)(→ polars.LazyFrame)                         | Add aligned `datetime_utc` and integer `hour_of_year` columns.              |
| [`_drop_city_cols`](#pudl.transform.vcerare._drop_city_cols)(→ polars.LazyFrame)                        | Drop the two Virginia independent-city rows from a capacity factor table.   |
| [`_make_cap_fac_frac`](#pudl.transform.vcerare._make_cap_fac_frac)(→ polars.LazyFrame)                     | Convert the capacity factor column from a percentage to a fraction.         |
| [`_check_for_valid_counties`](#pudl.transform.vcerare._check_for_valid_counties)(→ polars.LazyFrame)              | Check that every place name in the data appears in the FIPS mapping table.  |
| [`_standardize_census_names`](#pudl.transform.vcerare._standardize_census_names)(→ pandas.DataFrame)              | Make sure that the VCE place names correspond to the latest census vintage. |
| [`_clip_unexpected_2016_pv_capacity`](#pudl.transform.vcerare._clip_unexpected_2016_pv_capacity)(→ polars.LazyFrame)      | Clip a known set of out-of-bounds 2016 solar PV capacity factors.           |
| [`_spot_fix_great_lakes_fips`](#pudl.transform.vcerare._spot_fix_great_lakes_fips)(→ pandas.Series)                | Normalize the misspelled Lake Huron place name in the lat/lon/FIPS table.   |
| [`_spot_fix_great_lakes_capacity_factor`](#pudl.transform.vcerare._spot_fix_great_lakes_capacity_factor)(→ polars.LazyFrame)  | Normalize the misspelled Lake Huron place name in a capacity factor table.  |
| [`one_year_hourly_available_capacity_factor`](#pudl.transform.vcerare.one_year_hourly_available_capacity_factor)(→ dict[str, ...) | Transform one year of raw VCE RARE capacity factor tables to parquet.       |
| [`merge_all_vce_tables`](#pudl.transform.vcerare.merge_all_vce_tables)(→ pudl.helpers.ParquetData)           | Merge the cleaned VCE capacity factor tables into the final `out` table.    |
| [`out_vcerare__hourly_available_capacity_factor`](#pudl.transform.vcerare.out_vcerare__hourly_available_capacity_factor)(...)         | Transform raw Vibrant Clean Energy renewable generation profiles.           |

## Module Contents

### pudl.transform.vcerare.logger

### pudl.transform.vcerare.HOURLY_ID_COLS *= ['hour_of_year', 'report_year']*

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

* **Parameters:**
  **raw_vcerare_\_lat_lon_fips** – The raw VCE RARE lat/lon/FIPS mapping table.
* **Returns:**
  The prepped table with `county_state_names`, `county_id_fips` (nulled
  for lakes), `place_name`, `latitude`, `longitude`, and `state`
  columns.

### pudl.transform.vcerare.\_stack_cap_fac_df(lf: polars.LazyFrame, lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.LazyFrame

Reshape one wide capacity factor table from county columns to county rows.

Each raw capacity factor table has one column per county/subregion. This
unpivots those into a single `county_state_names` column (cast to
Categorical) plus a `capacity_factor_<lf_name>` value column, keeping
`hour_of_year` and `report_year` as identifiers. Applying this per table
rather than to the concatenated table keeps peak memory down.

* **Parameters:**
  * **lf** – One raw capacity factor table, wide, with `hour_of_year` and
    `report_year` columns plus one column per county/subregion.
  * **lf_name** – Short table label (e.g. `"solar_pv"`); names the value column
    and is used for logging.
* **Returns:**
  The table in long form with columns `hour_of_year`, `report_year`,
  `county_state_names`, and `capacity_factor_<lf_name>`.

### pudl.transform.vcerare.\_add_time_cols(lf: polars.LazyFrame, lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str), year: [int](https://docs.python.org/3/library/functions.html#int)) → polars.LazyFrame

Add aligned `datetime_utc` and integer `hour_of_year` columns.

The `datetime_utc` column matters for merging with other data; the integer
`hour_of_year` (1-8760) matters for modeling; `report_year` is handy for
filtering, so all three are kept.

Older vintages publish an integer `hour_of_year` and we reconstruct
`datetime_utc` from it. Vintages from 2024 on publish an hourly datetime column
(arriving as `hour_of_year`) and we derive the integer `hour_of_year` from that
instead.

On leap years December 31st is excluded so that every year has exactly 8760 hours.
Older vintages already omit it; newer ones may include it, so it is clipped here
based on whether `year` is a leap year.

* **Parameters:**
  * **lf** – One capacity factor table, already stacked, with `report_year` and
    `hour_of_year` columns.
  * **lf_name** – Short table label, used for logging.
  * **year** – The report year of this table.
* **Returns:**
  The table with `datetime_utc` (datetime) and `hour_of_year` (Int32,
  1-8760) columns, clipped to 8760 hours on leap years.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – For `year >= 2024`, if the incoming `hour_of_year` column
  is not a datetime column as expected.

### pudl.transform.vcerare.\_drop_city_cols(lf: polars.LazyFrame, lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.LazyFrame

Drop the two Virginia independent-city rows from a capacity factor table.

`bedford_city_virginia` and `clifton_forge_city_virginia` are reported by VCE
RARE but are not counties, and are excluded from the output.

* **Parameters:**
  * **lf** – A stacked capacity factor table with a `county_state_names` column.
  * **lf_name** – Short table label, used for logging.
* **Returns:**
  The table with the two independent-city rows removed.

### pudl.transform.vcerare.\_make_cap_fac_frac(lf: polars.LazyFrame, lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.LazyFrame

Convert the capacity factor column from a percentage to a fraction.

* **Parameters:**
  * **lf** – A stacked capacity factor table with a `capacity_factor_<lf_name>`
    column expressed as a percentage (0-100).
  * **lf_name** – Short table label; selects the value column and is used for logging.
* **Returns:**
  The table with `capacity_factor_<lf_name>` divided by 100.

### pudl.transform.vcerare.\_check_for_valid_counties(lf: polars.LazyFrame, clean_fips_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.LazyFrame

Check that every place name in the data appears in the FIPS mapping table.

Runs on the wide raw table so the place names can be read straight from the
schema (they are the non-identifier column names), avoiding a data scan.

* **Parameters:**
  * **lf** – A wide raw capacity factor table with one column per place name plus
    the `HOURLY_ID_COLS` identifier columns.
  * **clean_fips_df** – The cleaned lat/lon/FIPS table, whose `county_state_names`
    column is the set of expected place names.
  * **lf_name** – Short table label, used for logging and the error message.
* **Returns:**
  `lf` unchanged; this is a validation pass-through.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If the data contains any place name that is not present in
  `clean_fips_df`.

### pudl.transform.vcerare.\_standardize_census_names(vce_fips_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), census_pep_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

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

* **Parameters:**
  * **vce_fips_df** – The prepped VCE RARE lat/lon/FIPS table from
    `_prep_lat_long_fips_df`.
  * **census_pep_data** – The latest-vintage state-county-level rows of
    `_core_censuspep__yearly_geocodes`.
* **Returns:**
  `vce_fips_df` with `place_name` replaced by the Census PEP county name
  wherever the two differ, and the original VCE `place_name` column dropped.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If more than 74 place names would be replaced, suggesting
  the Census data or VCE names have shifted and need manual review.

### pudl.transform.vcerare.\_clip_unexpected_2016_pv_capacity(lf: polars.LazyFrame, lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str), year: [int](https://docs.python.org/3/library/functions.html#int)) → polars.LazyFrame

Clip a known set of out-of-bounds 2016 solar PV capacity factors.

The 2016 solar PV data contains exactly 365 hourly capacity factor values that
exceed the 110% maximum documented in the data’s read-me. Per correspondence with
the data provider these should be capped at 110%. This only touches the
`solar_pv` table for `year == 2016`; every other table/year combination passes
through unchanged.

* **Parameters:**
  * **lf** – A stacked capacity factor table.
  * **lf_name** – Short table label; the clip only applies when this is `"solar_pv"`.
  * **year** – Report year; the clip only applies when this is 2016.
* **Returns:**
  The table with `capacity_factor_solar_pv` values above 1.10 clipped to 1.10
  for 2016 solar PV; otherwise `lf` unchanged.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If the number of 2016 solar PV values exceeding 1.10 is not
  exactly 365, which would mean the data has changed and needs review.

### pudl.transform.vcerare.\_spot_fix_great_lakes_fips(sr: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Normalize the misspelled Lake Huron place name in the lat/lon/FIPS table.

VCE RARE spells Lake Huron as `lake_hurron_michigan` in the raw
`county_state_names` values of the lat/lon/FIPS mapping table. This must be
fixed so those values line up with the (separately fixed) capacity factor
tables when they are joined on `county_state_names` in
`merge_all_vce_tables`. See `_spot_fix_great_lakes_capacity_factor` for
the equivalent fix applied to the capacity factor data.

* **Parameters:**
  **sr** – The `county_state_names` Series from the raw lat/lon/FIPS table.
* **Returns:**
  The Series with `lake_hurron_michigan` replaced by `lake_huron_michigan`.

### pudl.transform.vcerare.\_spot_fix_great_lakes_capacity_factor(lf: polars.LazyFrame, lf_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.LazyFrame

Normalize the misspelled Lake Huron place name in a capacity factor table.

VCE RARE spells Lake Huron as `lake_hurron_michigan` in the raw capacity
factor data, where each place name is a column. This is the same fix
`_spot_fix_great_lakes_fips` applies to the lat/lon/FIPS table; both are
needed because the misspelling appears in both raw sources and the two tables
are joined on `county_state_names` in `merge_all_vce_tables`. It runs
before `_check_for_valid_counties` so the corrected name matches the FIPS
table.

* **Parameters:**
  * **lf** – A wide raw capacity factor table with one column per place name.
  * **lf_name** – Label for the table, used only for logging by the caller.
* **Returns:**
  The LazyFrame with any `lake_hurron_michigan` column renamed to
  `lake_huron_michigan`.

### pudl.transform.vcerare.one_year_hourly_available_capacity_factor(year: [int](https://docs.python.org/3/library/functions.html#int), fips_df_census: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_vcerare_\_fixed_solar_pv_lat_upv: polars.LazyFrame, raw_vcerare_\_offshore_wind_power_140m: polars.LazyFrame, raw_vcerare_\_onshore_wind_power_100m: polars.LazyFrame) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)]

Transform one year of raw VCE RARE capacity factor tables to parquet.

Runs the same stack-and-clean pipeline over each of the three raw capacity
factor tables (solar PV, offshore wind, onshore wind) separately rather than
on a single concatenated table, to keep peak memory down, and writes each
result to a partitioned `_core_vcerare__*` parquet file.

* **Parameters:**
  * **year** – The report year being processed.
  * **fips_df_census** – The cleaned lat/lon/FIPS table, used to validate place
    names.
  * **raw_vcerare_\_fixed_solar_pv_lat_upv** – Raw solar PV capacity factors for
    `year`.
  * **raw_vcerare_\_offshore_wind_power_140m** – Raw offshore wind capacity factors
    for `year`.
  * **raw_vcerare_\_onshore_wind_power_100m** – Raw onshore wind capacity factors
    for `year`.
* **Returns:**
  A dict mapping each `_core_vcerare__<table>` name to the `ParquetData`
  describing its written partition for `year`.

### pudl.transform.vcerare.merge_all_vce_tables(transformed_tables: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)], vce_fips_table: [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), year: [int](https://docs.python.org/3/library/functions.html#int)) → [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)

Merge the cleaned VCE capacity factor tables into the final `out` table.

Full-joins the three `_core_vcerare__*` tables on the hourly key, fills the
solar PV capacity factor with 0 where wind-only rows introduced nulls, joins the
lat/lon/FIPS metadata on `county_state_names`, sorts, and writes the result to a
partitioned parquet file for `year`.

* **Parameters:**
  * **transformed_tables** – The `_core_vcerare__*` -> `ParquetData` mapping
    returned by `one_year_hourly_available_capacity_factor`.
  * **vce_fips_table** – `ParquetData` for the cleaned lat/lon/FIPS table. Must
    carry an `index` column (see the note at the `.drop` call below).
  * **table_name** – Name of the partitioned output table to write.
  * **year** – The report year being merged.
* **Returns:**
  The `ParquetData` describing the written `table_name` partition for
  `year`.

### pudl.transform.vcerare.out_vcerare_\_hourly_available_capacity_factor(context, raw_vcerare_\_fixed_solar_pv_lat_upv: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)], raw_vcerare_\_offshore_wind_power_140m: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)], raw_vcerare_\_onshore_wind_power_100m: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [pudl.helpers.ParquetData](../../helpers/index.html.md#pudl.helpers.ParquetData)], raw_vcerare_\_lat_lon_fips: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → polars.LazyFrame

Transform raw Vibrant Clean Energy renewable generation profiles.

Concatenates the solar and wind capacity factors into a single table and turns
the columns for each county or subregion into a single place_name column. One
year of data is processed at a time to limit peak memory usage, with each year
written to its own partition of the output parquet file.

* **Parameters:**
  * **context** – The Dagster asset execution context.
  * **raw_vcerare_\_fixed_solar_pv_lat_upv** – Year -> `ParquetData` mapping of raw
    solar PV capacity factors.
  * **raw_vcerare_\_offshore_wind_power_140m** – Year -> `ParquetData` mapping of raw
    offshore wind capacity factors.
  * **raw_vcerare_\_onshore_wind_power_100m** – Year -> `ParquetData` mapping of raw
    onshore wind capacity factors.
  * **raw_vcerare_\_lat_lon_fips** – The raw VCE RARE lat/lon/FIPS mapping table.
  * **\_core_censuspep_\_yearly_geocodes** – Census PEP geocodes, used to standardize
    county place names to the latest vintage.
* **Returns:**
  A LazyFrame scanning every written year partition of the merged
  `out_vcerare__hourly_available_capacity_factor` table.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If the latest available Census PEP vintage predates the
  most recent VCE RARE report year.
