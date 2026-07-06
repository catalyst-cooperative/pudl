# pudl.transform.eia

Code for transforming EIA data that pertains to more than one EIA Form.

This module helps normalize EIA datasets and infers additional connections
between EIA entities (i.e. utilities, plants, units, generators…). This
includes:

- compiling a master list of plant, utility, boiler, and generator IDs that
  appear in any of the EIA 860 or 923 tables.
- inferring more complete boiler-generator associations.
- differentiating between static and time varying attributes associated with
  the EIA entities, storing the static fields with the entity table, and the
  variable fields in an annual table.

The boiler generator association inference (bga) takes the associations
provided by the EIA 860, and expands on it using several methods which can be
found in `pudl.transform.eia._boiler_generator_assn()`.

## Attributes

| [`logger`](#pudl.transform.eia.logger)                           |    |
|------------------------------------------------------------------|----|
| [`HARVESTABLE_ASSETS`](#pudl.transform.eia.HARVESTABLE_ASSETS)   |    |
| [`harvested_entities`](#pudl.transform.eia.harvested_entities)   |    |
| [`finished_eia_assets`](#pudl.transform.eia.finished_eia_assets) |    |

## Classes

| [`EiaEntity`](#pudl.transform.eia.EiaEntity)   | Enum for the different types of EIA entities.   |
|------------------------------------------------|-------------------------------------------------|

## Functions

| [`find_timezone`](#pudl.transform.eia.find_timezone)(\*[, lng, lat, state, strict, tz_finder])                             | Find the timezone associated with the a specified input location.                |
|----------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`occurrence_consistency`](#pudl.transform.eia.occurrence_consistency)(→ pandas.DataFrame)                                 | Find the occurrence of entities & the consistency of records.                    |
| [`_lat_long`](#pudl.transform.eia._lat_long)(→ pandas.DataFrame)                                                           | Harvests more complete lat/long in special cases.                                |
| [`_last_operating_date`](#pudl.transform.eia._last_operating_date)(→ pandas.DataFrame)                                     | When there's no consistent generator operating date, take the last reported one. |
| [`_add_timezone`](#pudl.transform.eia._add_timezone)(→ pandas.DataFrame)                                                   | Add plant IANA timezone based on lat/lon or state if lat/lon is unavailable.     |
| [`_add_additional_epacems_plants`](#pudl.transform.eia._add_additional_epacems_plants)(→ pandas.DataFrame)                 | Adds the info for plants that have IDs in the CEMS data but not EIA data.        |
| [`_compile_all_entity_records`](#pudl.transform.eia._compile_all_entity_records)(→ pandas.DataFrame)                       | Compile all of the entity records from each table they appear in.                |
| [`_manage_strictness`](#pudl.transform.eia._manage_strictness)(→ float)                                                    | Manage the strictness level for each column.                                     |
| [`harvest_entity_tables`](#pudl.transform.eia.harvest_entity_tables)(→ tuple)                                              | Compile consistent records for various entities.                                 |
| [`core_eia860__assn_boiler_generator`](#pudl.transform.eia.core_eia860__assn_boiler_generator)(→ pandas.DataFrame)         | Creates a set of more complete boiler generator associations.                    |
| [`_restrict_years`](#pudl.transform.eia._restrict_years)(→ pandas.DataFrame)                                               | Restricts eia years for boiler generator association.                            |
| [`map_balancing_authority_names_to_codes`](#pudl.transform.eia.map_balancing_authority_names_to_codes)(→ pandas.DataFrame) | Build a map of the BA names to their most frequently associated BA codes.        |
| [`fillna_balancing_authority_codes_via_names`](#pudl.transform.eia.fillna_balancing_authority_codes_via_names)(...)        | Fill null balancing authority (BA) codes via a map of the BA names to codes.     |
| [`fix_balancing_authority_codes_with_state`](#pudl.transform.eia.fix_balancing_authority_codes_with_state)(...)            | Fix selective balancing_authority_code_eia's based on states.                    |
| [`harvested_entity_asset_factory`](#pudl.transform.eia.harvested_entity_asset_factory)(→ dagster.AssetsDefinition)         | Create an asset definition for the harvested entity tables.                      |
| [`finished_eia_asset_factory`](#pudl.transform.eia.finished_eia_asset_factory)(→ dagster.AssetsDefinition)                 | An asset factory for finished EIA tables.                                        |

## Module Contents

### pudl.transform.eia.logger

### pudl.transform.eia.HARVESTABLE_ASSETS *= ('_core_eia860_\_boiler_cooling', '_core_eia860_\_boiler_emissions_control_equipment_assn',...*

### *class* pudl.transform.eia.EiaEntity

Bases: [`enum.StrEnum`](https://docs.python.org/3/library/enum.html#enum.StrEnum)

Enum for the different types of EIA entities.

#### PLANTS

#### UTILITIES

#### BOILERS

#### GENERATORS

### pudl.transform.eia.find_timezone(, lng=None, lat=None, state=None, strict=True, tz_finder=None)

Find the timezone associated with the a specified input location.

Note that this function requires named arguments. The names are lng, lat,
and state.  lng and lat must be provided, but they may be NA. state isn’t
required, and isn’t used unless lng/lat are NA or timezonefinder can’t find
a corresponding timezone.

Timezones based on states are imprecise, so it’s far better to use lng/lat
if possible. If strict is True, state will not be used.
More on state-to-timezone conversion here:
[https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory](https://en.wikipedia.org/wiki/List_of_time_offsets_by_U.S._state_and_territory)

* **Parameters:**
  * **lng** ([*int*](https://docs.python.org/3/library/functions.html#int) *or* *float in* *[* *-180* *,**180* *]*) – Longitude, in decimal degrees
  * **lat** ([*int*](https://docs.python.org/3/library/functions.html#int) *or* *float in* *[* *-90* *,* *90* *]*) – Latitude, in decimal degrees
  * **state** ([*str*](https://docs.python.org/3/library/stdtypes.html#str)) – Abbreviation for US state or Canadian province
  * **strict** ([*bool*](https://docs.python.org/3/library/functions.html#bool)) – Raise an error if no timezone is found?
* **Returns:**
  The timezone (as an IANA string) for that location.
* **Return type:**
  [str](https://docs.python.org/3/library/stdtypes.html#str)

### pudl.transform.eia.occurrence_consistency(entity_idx: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], compiled_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), col: [str](https://docs.python.org/3/library/stdtypes.html#str), cols_to_consit: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], strictness: [float](https://docs.python.org/3/library/functions.html#float) = 0.7) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Find the occurrence of entities & the consistency of records.

We need to determine how consistent a reported value is in the records
across all of the years or tables that the value is being reported, so we
want to compile two key numbers: the number of occurrences of the entity and
the number of occurrences of each reported record for each entity. With that
information we can determine if the reported records are strict enough.

* **Parameters:**
  * **entity_idx** – a list of the id(s) for the entity. Ex: for a plant
    entity, the entity_idx is [‘plant_id_eia’]. For a generator entity,
    the entity_idx is [‘plant_id_eia’, ‘generator_id’].
  * **compiled_df** – a dataframe with every instance of the
    column we are trying to harvest.
  * **col** – the column name of the column we are trying to harvest.
  * **cols_to_consit** – a list of the columns to determine consistency.
    This either the [entity_id] or the [entity_id, ‘report_date’],
    depending on whether the entity is static or annual.
  * **strictness** – How consistent do you want the column records to
    be? The default setting is .7 (so 70% of the records need to be
    consistent in order to accept harvesting the record).
* **Returns:**
  A transformed version of compiled_df with NaNs removed and with new columns with
  information about the consistency of the reported values.

### pudl.transform.eia.\_lat_long(dirty_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), clean_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), entity_id_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), entity_idx: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], col: [str](https://docs.python.org/3/library/stdtypes.html#str), cols_to_consit: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], round_to: [int](https://docs.python.org/3/library/functions.html#int) = 2, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Harvests more complete lat/long in special cases.

For all of the entities were there is not a consistent enough reported record for
latitude and longitude, this function reduces the precision of the reported lat/long
by rounding down the reported records in order to get more complete set of
consistent records.

* **Parameters:**
  * **dirty_df** – dataframe with entity records having inconsistently reported lat/long.
  * **clean_df** – dataframe with entity records having consistently reported lat/long.
  * **entity_id_df** – a dataframe with a complete set of possible entity ids.
  * **entity_idx** – a list of the id(s) for the entity. Ex: for a plant entity, the
    entity_idx is [‘plant_id_eia’]. For a generator entity, the entity_idx is
    [‘plant_id_eia’, ‘generator_id’].
  * **col** – the column name of the column we are trying to harvest.
  * **cols_to_consit** – a list of the columns to determine consistency. This either the
    [entity_id] or the [entity_id, ‘report_date’], depending on whether the
    entity is static or annual.
  * **round_to** – The number of decimal places we want to preserve while rounding down.
* **Returns:**
  DataFrame with all of the entity ids. Some will have harvested records from the
  clean_df. Some will have harvested records that were found after rounding. Some
  will have NaNs if no consistently reported records were found.

### pudl.transform.eia.\_last_operating_date(dirty_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), clean_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), entity_id_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), entity_idx: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], col: [str](https://docs.python.org/3/library/stdtypes.html#str), cols_to_consit: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

When there’s no consistent generator operating date, take the last reported one.

For all of the entities where there is not a consistent enough reported
operating date, this function keeps the most recently reported date.

* **Parameters:**
  * **dirty_df** – a dataframe with entity records that have inconsistently reported
    operating dates.
  * **clean_df** – a dataframe with entity records that have consistently reported
    operating dates.
  * **entity_id_df** – a dataframe with a complete set of possible entity ids
  * **entity_idx** – a list of the id(s) for the entity. Ex: for a plant entity, the
    entity_idx is [‘plant_id_eia’]. For a generator entity, the entity_idx is
    [‘plant_id_eia’, ‘generator_id’].
  * **col** – the column name of the column we are trying to harvest.
  * **cols_to_consit** – a list of the columns to determine consistency.  This either the
    [entity_id] or the [entity_id, ‘report_date’], depending on whether the
    entity is static or annual.
* **Returns:**
  A dataframe with all of the entity ids. Some will have harvested records from
  the clean_df. Some will have NA values if no consistently reported records were
  found.

### pudl.transform.eia.\_add_timezone(plants_entity: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add plant IANA timezone based on lat/lon or state if lat/lon is unavailable.

* **Parameters:**
  **plants_entity** – Plant entity table, including columns named “latitude”,
  “longitude”, and optionally “state”
* **Returns:**
  A DataFrame containing the same table, with a “timezone” column added.
  Timezone may be missing if lat / lon is missing or invalid.

### pudl.transform.eia.\_add_additional_epacems_plants(plants_entity: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Adds the info for plants that have IDs in the CEMS data but not EIA data.

The columns loaded are plant_id_eia, plant_name, state, latitude, and
longitude. Note that a side effect will be resetting the index on
plants_entity, if onecexists. If that’s a problem, modify the code below.

Note that some of these plants disappear from the CEMS before the
earliest EIA data PUDL processes, so if PUDL eventually ingests older
data, these may be redundant.

The set of additional plants is every plant that appears in the hourly CEMS
data (1995-2017) that never appears in the EIA 923 or 860 data (2009-2017
for EIA 923, 2011-2017 for EIA 860).

* **Parameters:**
  **plants_entity** – The plant entity table to which we will append additional plants.
* **Returns:**
  The same plants_entity table, with the addition of some missing EPA CEMS plants.

### pudl.transform.eia.\_compile_all_entity_records(entity: [EiaEntity](#pudl.transform.eia.EiaEntity), clean_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile all of the entity records from each table they appear in.

Comb through each of the dataframes in clean_dfs to pull out every instance of the
entity id.

### pudl.transform.eia.\_manage_strictness(col: [str](https://docs.python.org/3/library/stdtypes.html#str), special_case_strictness: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [float](https://docs.python.org/3/library/functions.html#float)]) → [float](https://docs.python.org/3/library/functions.html#float)

Manage the strictness level for each column.

* **Parameters:**
  * **col** – name of column
  * **special_case_strictness** – dictionary of column names (keys) and strictness
    values. The default strictness for harvesting consistent records is
    0.7. If any column you want to have a different consistency requirement,
    add it into this special case dictionary.

### pudl.transform.eia.harvest_entity_tables(entity: [EiaEntity](#pudl.transform.eia.EiaEntity), clean_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], special_case_strictness: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [float](https://docs.python.org/3/library/functions.html#float)] = {}, debug: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)

Compile consistent records for various entities.

For each entity (plants, generators, boilers, utilities), this function
finds all the harvestable columns from any table that they show up
in. It then determines how consistent the records are and keeps the values
that are mostly consistent. It compiles those consistent records into
one normalized table.

There are a few things to note here. First being that we are not expecting
the outcome here to be perfect! We choose to pull the most consistent
record as reported across all the EIA tables and years, but we also
required a “strictness” level of 70% (this is currently a hard coded
argument for [`occurrence_consistency()`](#pudl.transform.eia.occurrence_consistency)). That means at least 70% of the
records must be the same for us to use that value. So if values for an
entity haven’t been reported 70% consistently, then it will show up as a
null value. We built in the ability to add special cases for columns where
we want to apply a different method to, but the only ones we added was for
latitude and longitude because they are by far the dirtiest.

We have determined which columns should be considered “static” or “annual”.
These can be found in constants in the entities dictionary. Static means
That is should not change over time. Annual means there is annual
variability. This distinction was made in part by testing the consistency
and in part by an understanding of how the entities and columns relate in
the real world.

* **Parameters:**
  * **entity** – One of: plants, generators, boilers, or utilities
  * **clean_dfs** – A dictionary of table names (keys) and clean dfs (values).
  * **special_case_strictness** – dictionary of column names (keys) and strictness
    values. The default strictness for harvesting consistent records is
    0.7. If any column you want to have a different consistency requirement,
    add it into this special case dictionary.
  * **debug** – if True, log when columns are inconsistent, but don’t raise an error.
* **Returns:**
  entity_df (the harvested entity table), annual_df (the annual entity table),
  col_dfs (a dictionary of dataframes, one per harvested column, with information)
  about their consistency and the values which were harvested)
* **Raises:**
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If the consistency of any record value is <90% (when
  * **debug=False****)** – 

### pudl.transform.eia.core_eia860_\_assn_boiler_generator(context, \*\*clean_dfs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Creates a set of more complete boiler generator associations.

Creates a unique `unit_id_pudl` for each collection of boilers and generators
within a plant that have ever been associated with each other, based on the boiler
generator associations reported in EIA860. Unfortunately, this information is not
complete for years before 2014, as the gas turbine portion of combined cycle power
plants in those earlier years were not reporting their fuel consumption, or
existence as part of the plants.

For years 2014 and on, EIA860 contains a `unit_id_eia` value, allowing the
combined cycle plant components to be associated with each other. For many plants not
listed in the reported boiler generator associations, it is nonetheless possible to
associate boilers and generators on a one-to-one basis, as they use identical
strings to describe the units.

In the end, between the reported BGA table, the string matching, and the
`unit_id_eia` values, it’s possible to create a nearly complete mapping of the
generation units, at least for 2014 and later.

* **Parameters:**
  **clean_dfs** – a dictionary of clean EIA dataframes that have passed through the
  early transform steps.
* **Returns:**
  A dataframe containing the boiler generator associations.
* **Raises:**
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If the boiler - generator association graphs are not bi-partite,
    meaning generators only connect to boilers, and boilers only connect to
    generators.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If all boilers do not end up with the same unit_id each year.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If all generators do not end up with the same unit_id each year.

### pudl.transform.eia.\_restrict_years(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), eia_data_config: [pudl.settings.EiaDataConfig](../../settings/index.md#pudl.settings.EiaDataConfig) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Restricts eia years for boiler generator association.

### pudl.transform.eia.map_balancing_authority_names_to_codes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Build a map of the BA names to their most frequently associated BA codes.

We know there are some inconsistent pairings of codes and names so we grab the most
consistently reported combo, making the assumption that the most consistent pairing
is most likely to be the correct.

* **Parameters:**
  **df** – a data table with columns `balancing_authority_code_eia` and
  `balancing_authority_name_eia`
* **Returns:**
  a table with a unique index of `balancing_authority_name_eia` and a column of
  `balancing_authority_code`.

### pudl.transform.eia.fillna_balancing_authority_codes_via_names(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill null balancing authority (BA) codes via a map of the BA names to codes.

There are a handful of missing `balancing_authority_code_eia`’s that are easy to
map given the balancing_authority_name_eia. This function fills in null BA codes
using the BA names. The map of the BA names to codes is generated via
[`map_balancing_authority_names_to_codes()`](#pudl.transform.eia.map_balancing_authority_names_to_codes).

* **Parameters:**
  **df** – a data table with columns `balancing_authority_code_eia` and
  `balancing_authority_name_eia`

### pudl.transform.eia.fix_balancing_authority_codes_with_state(plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), plants_entity: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fix selective balancing_authority_code_eia’s based on states.

There are some known errors in the `balancing_authority_code_eia` column that we
can identify and fix based on the state where the plant is located. Where we update
the `balancing_authority_code_eia` column, we also update the
`balancing_authority_name_eia` column using the name generated by
[`map_balancing_authority_names_to_codes()`](#pudl.transform.eia.map_balancing_authority_names_to_codes).

This function should only be applied post-[`harvest_entity_tables()`](#pudl.transform.eia.harvest_entity_tables). The
`state` column is a “static” entity column so the first step in this function is
merging the static and annually varying plants together. Then we fix known errors in
the BA codes:

* reported PACE, but state is OR or CA, code should be PACW
* reported PACW, but state is UT, code should be PACE

* **Parameters:**
  * **plants** – annually harvested plant table with columns: `plant_id_eia`,
    `report_date` and `balancing_authority_code_eia`.
  * **plants_entity** – static harvested plant table with columns: `plant_id_eia` and
    `state`.
* **Returns:**
  plants table that has the same set of columns and rows, with cleaned
  `balancing_authority_code_eia` column and an updated corresponding
  `balancing_authority_name_eia` column.

### pudl.transform.eia.harvested_entity_asset_factory(entity: [EiaEntity](#pudl.transform.eia.EiaEntity), io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Create an asset definition for the harvested entity tables.

### pudl.transform.eia.harvested_entities

### pudl.transform.eia.finished_eia_asset_factory(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), \_core_table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), io_manager_key: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

An asset factory for finished EIA tables.

* **Parameters:**
  * **table_name** – the name of the harvest table.
  * **\_core_table_name** – the name of the unharvested input table
  * **io_manager_key** – the name of the IO Manager of the final asset.
* **Returns:**
  A harvested EIA asset.

### pudl.transform.eia.finished_eia_assets
