# pudl.transform.ferc714

Transformation of the FERC Form 714 data.

FERC Form 714 has two separate raw data sources - CSV and XBRL. For both sources
there is usually some specific processing that needs to happen before the two
data sources get concatenated together to create the full timeseries. We are
currently processing three tables from 714. Each one is processed using a similar
pattern: we’ve defined a class with a run classmethod as a coordinating method,
any table-specific transforms are defined as staticmethod’s within the table
class and any generic 714 transforms are defined as internal module functions.
The table assets are created through a small function that calls the run method.
Any of the methods or functions that only apply to either of the raw data sources
should include a raw datasource suffix.

## Attributes

| [`logger`](#pudl.transform.ferc714.logger)                                                         |                                                                                 |
|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`TIMEZONE_OFFSET_CODE_FIXES`](#pudl.transform.ferc714.TIMEZONE_OFFSET_CODE_FIXES)                 |                                                                                 |
| [`TIMEZONE_OFFSET_CODE_FIXES_BY_YEAR`](#pudl.transform.ferc714.TIMEZONE_OFFSET_CODE_FIXES_BY_YEAR) |                                                                                 |
| [`DISCONTINUOUS_DATES`](#pudl.transform.ferc714.DISCONTINUOUS_DATES)                               | Identified gaps in hourly timeseries. The vast majority of these are around     |
| [`DUPLICATED_DATETIMES`](#pudl.transform.ferc714.DUPLICATED_DATETIMES)                             | Identified duplicated UTC datetimes resulting from changes to a planning area's |
| [`BAD_RESPONDENTS`](#pudl.transform.ferc714.BAD_RESPONDENTS)                                       | Fake respondent IDs for database test entities.                                 |
| [`TIMEZONE_OFFSET_CODES`](#pudl.transform.ferc714.TIMEZONE_OFFSET_CODES)                           | A mapping of timezone offset codes to Timedelta offsets from UTC.               |
| [`TIMEZONE_CODES`](#pudl.transform.ferc714.TIMEZONE_CODES)                                         | Mapping between standardized time offset codes and canonical timezones.         |
| [`EIA_CODE_FIXES`](#pudl.transform.ferc714.EIA_CODE_FIXES)                                         | Overrides of FERC 714 respondent IDs with wrong or missing EIA Codes.           |
| [`RENAME_COLS`](#pudl.transform.ferc714.RENAME_COLS)                                               |                                                                                 |

## Classes

| [`RespondentId`](#pudl.transform.ferc714.RespondentId)                                         | Class for building the [core_ferc714_\_respondent_id](../../../../data_dictionaries/pudl_db.md#core-ferc714-respondent-id) asset.                                               |
|------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`HourlyPlanningAreaDemand`](#pudl.transform.ferc714.HourlyPlanningAreaDemand)                 | Class for building the [core_ferc714_\_hourly_planning_area_demand](../../../../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand) asset.                   |
| [`YearlyPlanningAreaDemandForecast`](#pudl.transform.ferc714.YearlyPlanningAreaDemandForecast) | Class for building the [core_ferc714_\_yearly_planning_area_demand_forecast](../../../../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast) asset. |

## Functions

| [`_pre_process_csv`](#pudl.transform.ferc714._pre_process_csv)(→ pandas.DataFrame)                                                      | A simple transform function for processing the CSV raw data.                                                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`_assign_respondent_id_ferc714`](#pudl.transform.ferc714._assign_respondent_id_ferc714)(→ pandas.DataFrame)                            | Assign the PUDL-assigned respondent_id_ferc714 based on the native respondent ID.                                                                            |
| [`_filter_for_freshest_data_xbrl`](#pudl.transform.ferc714._filter_for_freshest_data_xbrl)(raw_xbrl, table_name, ...)                   | Wrapper around filter_for_freshest_data_xbrl.                                                                                                                |
| [`_fillna_respondent_id_ferc714_source`](#pudl.transform.ferc714._fillna_respondent_id_ferc714_source)(→ pandas.DataFrame)              | Fill missing CSV or XBRL respondent id.                                                                                                                      |
| [`assign_report_day`](#pudl.transform.ferc714.assign_report_day)(→ pandas.DataFrame)                                                    | Add a report_day column.                                                                                                                                     |
| [`core_ferc714__respondent_id`](#pudl.transform.ferc714.core_ferc714__respondent_id)(→ pandas.DataFrame)                                | Transform the FERC 714 respondent IDs, names, and EIA utility IDs.                                                                                           |
| [`core_ferc714__hourly_planning_area_demand`](#pudl.transform.ferc714.core_ferc714__hourly_planning_area_demand)(...)                   | Build the [core_ferc714_\_hourly_planning_area_demand](../../../../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand).                   |
| [`core_ferc714__yearly_planning_area_demand_forecast`](#pudl.transform.ferc714.core_ferc714__yearly_planning_area_demand_forecast)(...) | Build the [core_ferc714_\_yearly_planning_area_demand_forecast](../../../../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast). |

## Module Contents

### pudl.transform.ferc714.logger

### pudl.transform.ferc714.TIMEZONE_OFFSET_CODE_FIXES

### pudl.transform.ferc714.TIMEZONE_OFFSET_CODE_FIXES_BY_YEAR

### pudl.transform.ferc714.DISCONTINUOUS_DATES

Identified gaps in hourly timeseries. The vast majority of these are around
daylight saving time switchover dates, though there are a couple exceptions. We
expect to add to this list each year.

### pudl.transform.ferc714.DUPLICATED_DATETIMES

Identified duplicated UTC datetimes resulting from changes to a planning area’s
reporting timezone.

### pudl.transform.ferc714.BAD_RESPONDENTS *= [2, 319, 99991, 99992, 99993, 99994, 99995]*

Fake respondent IDs for database test entities.

### pudl.transform.ferc714.TIMEZONE_OFFSET_CODES

A mapping of timezone offset codes to Timedelta offsets from UTC.

Note that the FERC 714 instructions state that all hourly demand is to be reported
in STANDARD time for whatever timezone is being used. Even though many respondents
use daylight savings / standard time abbreviations, a large majority do appear to
conform to using a single UTC offset throughout the year. There are 6 instances in
which the timezone associated with reporting changed dropped.

### pudl.transform.ferc714.TIMEZONE_CODES

Mapping between standardized time offset codes and canonical timezones.

### pudl.transform.ferc714.EIA_CODE_FIXES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Literal['combined', 'csv', 'xbrl'], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int) | [str](https://docs.python.org/3/library/stdtypes.html#str), [int](https://docs.python.org/3/library/functions.html#int)]]*

Overrides of FERC 714 respondent IDs with wrong or missing EIA Codes.

This is used in [`RespondentId.spot_fix_eia_codes()`](#pudl.transform.ferc714.RespondentId.spot_fix_eia_codes). The dictionary
is organized by “source” keys (“combined”, “csv”, or “xbrl”). Each source’s
value is a secondary dictionary which contains source respondent ID’s as keys
and fixes for EIA codes as values.

We separated these fixes by either coming directly from the CSV data, the XBRL
data, or the combined data. We use the corresponding source or PUDL-derived
respondent ID to identify the EIA code to overwrite. We could have combined
these fixes all into one set of combined fixes identified by the PUDL-derived
`respondent_id_ferc714`, but this way we can do more targeted source-based
cleaning and test each source’s EIA codes before the sources are concatenated
together.

### pudl.transform.ferc714.RENAME_COLS

### pudl.transform.ferc714.\_pre_process_csv(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

A simple transform function for processing the CSV raw data.

* Removes footnotes columns ending with \_f
* Drops report_prd, spplmnt_num, and row_num columns
* Excludes records which pertain to bad (test) respondents.

### pudl.transform.ferc714.\_assign_respondent_id_ferc714(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), source: Literal['csv', 'xbrl']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Assign the PUDL-assigned respondent_id_ferc714 based on the native respondent ID.

We need to replace the natively reported respondent ID from each of the two FERC714
sources with a PUDL-assigned respondent ID. The mapping between the native ID’s and
these PUDL-assigned ID’s can be accessed in the database tables
`respondents_csv_ferc714` and `respondents_xbrl_ferc714`.

* **Parameters:**
  * **df** – the input table with the native respondent ID column.
  * **source** – the lower-case string name of the source of the FERC714 data. Either csv
  * **xbrl.** (*or*)
* **Returns:**
  an augmented version of the input `df` with a new column that replaces
  the natively reported respondent ID with the PUDL-assigned respondent ID.

### pudl.transform.ferc714.\_filter_for_freshest_data_xbrl(raw_xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), instant_or_duration: Literal['instant', 'duration'], pudl_paths: [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths) | [None](https://docs.python.org/3/library/constants.html#None) = None)

Wrapper around filter_for_freshest_data_xbrl.

Most of the specific stuff here is in just converting the core table name
into the raw instant or duration XBRL table name.

### pudl.transform.ferc714.\_fillna_respondent_id_ferc714_source(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), source: Literal['csv', 'xbrl']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill missing CSV or XBRL respondent id.

The source (CSV or XBRL) tables get assigned a PUDL-derived
`respondent_id_ferc714` ID column (via [`_assign_respondent_id_ferc714()`](#pudl.transform.ferc714._assign_respondent_id_ferc714)).
After we concatenate the source tables, we sometimes backfill and
forward-fill the source IDs (`respondent_id_ferc714_csv` and
`respondent_id_ferc714_xbrl`). This way the older records from the CSV years
will also have the XBRL ID’s and vice versa. This will enable users to find
the full timeseries of a respondent that given either source ID (instead of
using the source ID to find the PUDL-derived ID and then finding the records).

### pudl.transform.ferc714.assign_report_day(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), date_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add a report_day column.

### *class* pudl.transform.ferc714.RespondentId

Class for building the [core_ferc714_\_respondent_id](../../../../data_dictionaries/pudl_db.md#core-ferc714-respondent-id) asset.

Most of the methods in this class as staticmethods. The purpose of using a class
in this instance is mostly for organizing the table specific transforms under the
same name-space.

#### *classmethod* run(raw_csv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_duration: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pudl_paths: [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build the table for the [core_ferc714_\_respondent_id](../../../../data_dictionaries/pudl_db.md#core-ferc714-respondent-id) asset.

Process and combine the CSV and XBRL based data.

There are two main threads of transforms happening here:

* Table compatibility: The CSV raw table is static (does not even report years)
  while the xbrl table is reported annually. A lot of the downstream analysis
  expects this table to be static. So the first step was to check whether or not
  the columns that we have in the CSV years had consistent data over the few XBRL
  years that we have. There are a small number of eia_code’s we needed to clean
  up, but besides that it was static. We then convert the XBRL data into a static
  table, then we concat-ed the tables and checked the static-ness again via
  [`ensure_eia_code_uniqueness()`](#pudl.transform.ferc714.RespondentId.ensure_eia_code_uniqueness).
* eia_code cleaning: Clean up FERC-714 respondent names and manually assign EIA
  utility IDs to a few FERC Form 714 respondents that report planning area demand,
  but which don’t have their corresponding EIA utility IDs provided by FERC for
  some reason (including PacifiCorp). Done all via [`spot_fix_eia_codes()`](#pudl.transform.ferc714.RespondentId.spot_fix_eia_codes) &
  EIA_CODE_FIXES.

#### *static* spot_fix_eia_codes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), source: Literal['csv', 'xbrl', 'combined']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Spot fix the eia_codes.

Using the manually compiled fixes to the `eia_code` column stored in
[`EIA_CODE_FIXES`](#pudl.transform.ferc714.EIA_CODE_FIXES), replace the reported values by respondent.

#### *static* ensure_eia_code_uniqueness(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), source: Literal['csv', 'xbrl', 'combined']) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Ensure there is only one unique eia_code for each respondent.

#### *static* clean_eia_codes_xbrl(xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Make eia_code’s cleaner coming from the XBRL data.

Desired outcomes here include all respondents have only one non-null
eia_code and all eia_codes that are actually the respondent_id_ferc714_xbrl
are nulled.

#### *static* convert_into_static_table_xbrl(xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert this annually reported table into a skinnier, static table.

The CSV table is entirely static - it doesn’t have any reported
changes that vary over time. The XBRL table does have start and end
dates in it. In order to merge these two sources, we are checking
whether or not the shared variables change over time and then
converting this table into a non-time-varying table.

#### *static* condense_into_one_source_table(df)

Condense the CSV and XBRL records together into one record.

We have two records coming from each of the two sources in this table.
This method simply drops duplicates based on the PKs of the table.
We know that the names are different in the CSV vs the XBRL source.
We are going to grab the XBRL names because they are more recent.

NOTE: We could have merged the data in [`run()`](#pudl.transform.ferc714.RespondentId.run) instead of concatenating
along the index. We would have had to develop different methods for
[`ensure_eia_code_uniqueness()`](#pudl.transform.ferc714.RespondentId.ensure_eia_code_uniqueness).

#### *static* fill_missing_eia_codes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill missing eia_code values with unique non-null value per respondent.

### pudl.transform.ferc714.core_ferc714_\_respondent_id(context, raw_csv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_duration: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the FERC 714 respondent IDs, names, and EIA utility IDs.

This is a light wrapper around [`RespondentId`](#pudl.transform.ferc714.RespondentId) because you need to
build an asset from a function - not a staticmethod of a class.

* **Parameters:**
  * **raw_csv** – Raw table describing the FERC 714 Respondents from the CSV years.
  * **raw_xbrl_duration** – Raw table describing the FERC 714 Respondents from the
    XBRL years.
* **Returns:**
  A clean(er) version of the FERC-714 respondents table.

### *class* pudl.transform.ferc714.HourlyPlanningAreaDemand

Class for building the [core_ferc714_\_hourly_planning_area_demand](../../../../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand) asset.

The [core_ferc714_\_hourly_planning_area_demand](../../../../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand) table is an hourly time
series of demand by Planning Area.

Most of the methods in this class as staticmethods. The purpose of using a class
in this instance is mostly for organizing the table specific transforms under the
same name-space.

#### *classmethod* run(raw_csv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_duration: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_instant: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pudl_paths: [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build the [core_ferc714_\_hourly_planning_area_demand](../../../../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand) asset.

To transform this table we have to process the instant and duration xbrl
tables so we can merge them together and process the XBRL data. We also
have to process the CSV data so we can concatenate it with the XBLR data.
Then we can process all of the data together.

For both the CSV and XBRL data, the main transforms that are happening
have to do with cleaning the timestamps in the data, resulting in
timestamps that are in a datetime format and are nearly continuous
for every respondent.

Once the CSV and XBRL data is merged together, the transforms are mostly
focused on cleaning the timezone codes reported to FERC
and then using those timezone codes to convert all of timestamps into
UTC datetime.

The outcome here is nearly continuous and non-duplicative time series.

#### *static* melt_hourx_columns_csv(df)

Melt hourX columns into hours.

There are some instances of the CSVs with a 25th hour. We drop
those entirely because almost all of them are unusable (0.0 or
daily totals), and they shouldn’t really exist at all based on
FERC instructions.

#### *static* parse_date_strings_csv(csv)

Convert report_date into pandas Datetime types.

Make the report_date column from the daily string `report_date` and
the integer `hour` column.

#### *static* remove_yearly_records_duration_xbrl(duration_xbrl)

Convert a table with mostly daily records with some annuals into fully daily.

Almost all of the records have a start_date that == the end_date
which I’m assuming means the record spans the duration of one day
there are a small handful of records which seem to span a full year.

#### *static* merge_instant_and_duration_tables_xbrl(instant_xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), duration_xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge XBRL instant and duration tables, reshaping instant as needed.

FERC714 XBRL instant period signifies that it is true as of the reported date,
while a duration fact pertains to the specified time period. The `date` column
for an instant fact corresponds to the `end_date` column of a duration fact.

* **Parameters:**
  * **instant_xbrl** – table representing XBRL instant facts.
  * **raw_xbrl_duration** – table representing XBRL duration facts.
* **Returns:**
  A unified table combining the XBRL duration and instant facts, if both types
  of facts were present. If either input dataframe is empty, the other
  dataframe is returned unchanged, except that several unused columns are
  dropped. If both input dataframes are empty, an empty dataframe is returned.

#### *static* convert_dates_to_zero_offset_hours_xbrl(xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert all hours to: Hour (24-hour clock) as a zero-padded decimal number.

The FERC 714 form includes columns for the hours of each day. Those columns are
labeled with 1-24 to indicate the hours of the day. The XBRL filings themselves
have time-like string associated with each of the facts. They include both a the
year-month-day portion (formatted as %Y-%m-%d) as well as an hour-minute-second
component (semi-formatted as T%H:%M:%S). Attempting to simply convert this
timestamp information to a datetime using the format `"%Y-%m-%dT%H:%M:%S"`
fails because about a third of the records include hour 24 - which is not an
accepted hour in standard datetime formats.

The respondents that report hour 24 do not report hour 00. We have done some spot
checking of values reported to FERC and have determined that hour 24 seems to
correspond with hour 00 (of the next day). We have not gotten complete
confirmation from FERC staff that this is always the case, but it seems like a
decent assumption.

So, this step converts all of the hour 24 records to be hour 00 of the next day.

#### *static* convert_dates_to_zero_seconds_xbrl(xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert the last second of the day records to the first (0) second of the next day.

There are a small amount of records which report the last “hour” of the day
as last second of the day, as opposed to T24 cleaned in
[`convert_dates_to_zero_offset_hours_xbrl()`](#pudl.transform.ferc714.HourlyPlanningAreaDemand.convert_dates_to_zero_offset_hours_xbrl) or T00 which is standard for a
datetime. This function finds these records and adds one second to them and
then ensures all of the records has 0’s for seconds.

#### *static* spot_fix_records_xbrl(xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Spot fix some specific XBRL records.

#### *static* ensure_dates_are_continuous(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), source: Literal['csv', 'xbrl'])

Assert that almost all respondents have continuous timestamps.

The xbrl data frequently includes gaps around daylight savings switchover
dates. These are catalogued in DISCONTINUOUS_DATES. The csv data has 10 gaps.
Pretty good all in all!

#### *static* standardize_offset_codes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), offset_fixes) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Convert to standardized UTC offset abbreviations.

This function ensures that all of the 3-4 letter abbreviations used to indicate a
timestamp’s localized offset from UTC are standardized, so that they can be used to
make the timestamps timezone aware. The standard abbreviations we’re using are:

“HST”: Hawaii Standard Time
“AKST”: Alaska Standard Time
“AKDT”: Alaska Daylight Time
“PST”: Pacific Standard Time
“PDT”: Pacific Daylight Time
“MST”: Mountain Standard Time
“MDT”: Mountain Daylight Time
“CST”: Central Standard Time
“CDT”: Central Daylight Time
“EST”: Eastern Standard Time
“EDT”: Eastern Daylight Time

In some cases different respondents use the same non-standard abbreviations to
indicate different offsets, and so the fixes are applied on a per-respondent basis,
as defined by offset_fixes.

* **Parameters:**
  * **df** – DataFrame containing a utc_offset_code column that needs to be standardized.
  * **offset_fixes** – A dictionary with respondent_id_ferc714 values as the keys, and a
    dictionary mapping non-standard UTC offset codes to the standardized UTC
    offset codes as the value.
* **Returns:**
  Standardized UTC offset codes.

#### *static* clean_utc_code_offsets_and_set_timezone(df)

Clean UTC Codes and set timezone.

#### *static* drop_missing_utc_offset(df)

Drop records with missing UTC offsets and zero demand.

#### *static* construct_utc_datetime(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Construct datetime_utc column.

#### *static* ensure_non_duplicated_datetimes(df)

Report and drop duplicated UTC datetimes.

#### *static* spot_fix_values(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Spot fix values.

### pudl.transform.ferc714.core_ferc714_\_hourly_planning_area_demand(context, raw_csv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_duration: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_instant: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build the [core_ferc714_\_hourly_planning_area_demand](../../../../data_dictionaries/pudl_db.md#core-ferc714-hourly-planning-area-demand).

This is a light wrapper around [`HourlyPlanningAreaDemand`](#pudl.transform.ferc714.HourlyPlanningAreaDemand) because
it seems you need to build an asset from a function - not a staticmethod of
a class.

### *class* pudl.transform.ferc714.YearlyPlanningAreaDemandForecast

Class for building the [core_ferc714_\_yearly_planning_area_demand_forecast](../../../../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast) asset.

The [core_ferc714_\_yearly_planning_area_demand_forecast](../../../../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast) table is an annual, forecasted
time series of demand by Planning Area.

Most of the methods in this class as staticmethods. The purpose of using a class
in this instance is mostly for organizing the table specific transforms under the
same name-space.

#### *classmethod* run(raw_csv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_duration: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), pudl_paths: [pudl.workspace.setup.PudlPaths](../../workspace/setup/index.md#pudl.workspace.setup.PudlPaths)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build the [core_ferc714_\_yearly_planning_area_demand_forecast](../../../../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast) asset.

To transform this table we have to process the CSV data and the XBRL duration data
(this data has no instant table), merge together the XBRL and CSV data, and
process the combined datasets.

The main transforms include spot-fixing forecast years with
[`spot_fix_forecast_years_xbrl()`](#pudl.transform.ferc714.YearlyPlanningAreaDemandForecast.spot_fix_forecast_years_xbrl) and averaging out duplicate forecast values
for duplicate primary key rows in the CSV table.

#### *static* spot_fix_forecast_years_xbrl(df)

Spot fix forecast year errors.

This function fixes the following errors:

- There’s one record with an NA forecast_year value. This row
  also has no demand forecast values. Because forecast_year is a primary key
  we can’t have any NA values. Because there are no substantive forecasts
  in this row, we can safely remove this row.
- respondent_id_ferc714 number 107 reported their forecast_year
  as YY instead of YYYY values.
- There’s also at least one forecast year value reported as 3033 that should
  be 2033.

This function also checks that the values for forecast year are within an
expected range.

#### *static* average_duplicate_pks_csv(df)

Average forecast values for duplicate primary keys.

The XBRL data had duplicate primary keys, but it was easy to parse
them by keeping rows with the most recent publication_time value.
The CSVs have no such distinguishing column, despite having some
duplicate primary keys.

This function takes the average of the forecast values for rows
with duplicate primary keys. There are only 6 respondent/report_year/
forecast year rows where the forecast values differ. One of those is a
pair where one forecast value is 0. We’ll take the non-zero value here
and average out the rest.

### pudl.transform.ferc714.core_ferc714_\_yearly_planning_area_demand_forecast(context, raw_csv: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_xbrl_duration: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build the [core_ferc714_\_yearly_planning_area_demand_forecast](../../../../data_dictionaries/pudl_db.md#core-ferc714-yearly-planning-area-demand-forecast).

This is a light wrapper around [`YearlyPlanningAreaDemandForecast`](#pudl.transform.ferc714.YearlyPlanningAreaDemandForecast) because
it seems you need to build an asset from a function - not a staticmethod of
a class.
