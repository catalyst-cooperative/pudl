# pudl.transform.eia923

Module to perform data cleaning functions on EIA923 data tables.

## Attributes

| [`logger`](#pudl.transform.eia923.logger)                                 |                                                                                |
|---------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`COALMINE_COUNTRY_CODES`](#pudl.transform.eia923.COALMINE_COUNTRY_CODES) | A mapping of EIA foreign coal mine country codes to 3-letter ISO-3166-1 codes. |

## Functions

| [`_get_plant_nuclear_unit_id_map`](#pudl.transform.eia923._get_plant_nuclear_unit_id_map)(→ dict[int, str])                                | Get a plant_id -> nuclear_unit_id mapping for all plants with one nuclear unit.                                                                |
|--------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| [`_backfill_nuclear_unit_id`](#pudl.transform.eia923._backfill_nuclear_unit_id)(→ pandas.DataFrame)                                        | Backfill 2001 and 2002 nuclear_unit_id for plants with one nuclear unit.                                                                       |
| [`_get_plant_prime_mover_map`](#pudl.transform.eia923._get_plant_prime_mover_map)(→ dict[int, str])                                        | Get a plant_id -> prime_mover_code mapping for all plants with one prime mover.                                                                |
| [`_backfill_prime_mover_code`](#pudl.transform.eia923._backfill_prime_mover_code)(→ pandas.DataFrame)                                      | Backfill 2001 and 2002 prime_mover_code for plants with one prime mover.                                                                       |
| [`_get_most_frequent_energy_source_map`](#pudl.transform.eia923._get_most_frequent_energy_source_map)(→ dict[str, str])                    | Get the a mapping of the most common energy_source for each fuel_type_code_agg.                                                                |
| [`_clean_gen_fuel_energy_sources`](#pudl.transform.eia923._clean_gen_fuel_energy_sources)(→ pandas.DataFrame)                              | Clean the generator_fuel_eia923.energy_source_code field specifically.                                                                         |
| [`_aggregate_generation_fuel_duplicates`](#pudl.transform.eia923._aggregate_generation_fuel_duplicates)(→ pandas.DataFrame)                | Aggregate remaining duplicate generation fuels.                                                                                                |
| [`_yearly_to_monthly_records`](#pudl.transform.eia923._yearly_to_monthly_records)(→ pandas.DataFrame)                                      | Converts an EIA 923 record of 12 months of data into 12 monthly records.                                                                       |
| [`_coalmine_cleanup`](#pudl.transform.eia923._coalmine_cleanup)(→ pandas.DataFrame)                                                        | Clean up the core_eia923_\_entity_coalmine table.                                                                                              |
| [`plants_eia923`](#pudl.transform.eia923.plants_eia923)(→ dict[str, pandas.DataFrame])                                                     | Transforms the plants_eia923 table.                                                                                                            |
| [`gen_fuel_nuclear`](#pudl.transform.eia923.gen_fuel_nuclear)(→ pandas.DataFrame)                                                          | Transforms the core_eia923_\_monthly_generation_fuel_nuclear table.                                                                            |
| [`_core_eia923__pre_generation_fuel`](#pudl.transform.eia923._core_eia923__pre_generation_fuel)(...)                                       | Transforms the raw_eia923_\_generation_fuel table.                                                                                             |
| [`_map_prime_mover_sets`](#pudl.transform.eia923._map_prime_mover_sets)(→ str)                                                             | Map unique prime mover combinations to a single prime mover code.                                                                              |
| [`_aggregate_duplicate_boiler_fuel_keys`](#pudl.transform.eia923._aggregate_duplicate_boiler_fuel_keys)(→ pandas.DataFrame)                | Combine boiler_fuel rows with duplicate keys by aggregating them.                                                                              |
| [`_core_eia923__boiler_fuel`](#pudl.transform.eia923._core_eia923__boiler_fuel)(→ pandas.DataFrame)                                        | Transforms the core_eia923_\_monthly_boiler_fuel table.                                                                                        |
| [`remove_duplicate_pks_boiler_fuel_eia923`](#pudl.transform.eia923.remove_duplicate_pks_boiler_fuel_eia923)(→ pandas.DataFrame)            | Deduplicate on primary keys for [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel). |
| [`_core_eia923__generation`](#pudl.transform.eia923._core_eia923__generation)(→ pandas.DataFrame)                                          | Transforms the EIA 923 generation table.                                                                                                       |
| [`_drop_duplicates__core_eia923__generation`](#pudl.transform.eia923._drop_duplicates__core_eia923__generation)(...)                       |                                                                                                                                                |
| [`_core_eia923__coalmine`](#pudl.transform.eia923._core_eia923__coalmine)(→ pandas.DataFrame)                                              | Transforms the raw_eia923_\_fuel_receipts_costs table.                                                                                         |
| [`_core_eia923__fuel_receipts_costs`](#pudl.transform.eia923._core_eia923__fuel_receipts_costs)(→ pandas.DataFrame)                        | Transforms the eia923_\_fuel_receipts_costs dataframe.                                                                                         |
| [`_core_eia923__monthly_cooling_system_information`](#pudl.transform.eia923._core_eia923__monthly_cooling_system_information)(...)         | Transforms the eia923_\_cooling_system_information dataframe.                                                                                  |
| [`cooling_system_information_continuity`](#pudl.transform.eia923.cooling_system_information_continuity)(csi)                               | Check to see if columns vary as slowly as expected.                                                                                            |
| [`_build_emissions_control_dates`](#pudl.transform.eia923._build_emissions_control_dates)(→ pandas.Series)                                 | Validate date parts and build parsed timestamps.                                                                                               |
| [`_parse_emissions_control_date_subset`](#pudl.transform.eia923._parse_emissions_control_date_subset)(→ pandas.Series)                     | Parse one recognized emissions-control date format for a subset of rows.                                                                       |
| [`_clean_emissions_control_dates`](#pudl.transform.eia923._clean_emissions_control_dates)(→ pandas.Series)                                 | Parse raw EIA-923 emissions-control date strings into datetimes.                                                                               |
| [`_core_eia923__yearly_fgd_operation_maintenance`](#pudl.transform.eia923._core_eia923__yearly_fgd_operation_maintenance)(...)             | Transforms the \_core_eia923_\_yearly_fgd_operation_maintenance table.                                                                         |
| [`fgd_continuity_check`](#pudl.transform.eia923.fgd_continuity_check)(fgd)                                                                 | Check to see if columns vary as slowly as expected.                                                                                            |
| [`_core_eia923__energy_storage`](#pudl.transform.eia923._core_eia923__energy_storage)(→ pandas.DataFrame)                                  | Transforms the eia923_energy_storage table.                                                                                                    |
| [`_core_eia923__yearly_byproduct_disposition`](#pudl.transform.eia923._core_eia923__yearly_byproduct_disposition)(...)                     | Transforms the eia923_\_byproduct_disposition table.                                                                                           |
| [`disposition_continuity_check`](#pudl.transform.eia923.disposition_continuity_check)(bpd)                                                 | Check to see if columns vary as slowly as expected.                                                                                            |
| [`_core_eia923__yearly_byproduct_expenses_and_revenues`](#pudl.transform.eia923._core_eia923__yearly_byproduct_expenses_and_revenues)(...) | Transforms the eia923_\_byproduct_expenses_and_revenues table.                                                                                 |
| [`_core_eia923__yearly_emissions_control`](#pudl.transform.eia923._core_eia923__yearly_emissions_control)(→ pandas.DataFrame)              | Transforms the eia923_\_emissions_control table.                                                                                               |

## Module Contents

### pudl.transform.eia923.logger

### pudl.transform.eia923.COALMINE_COUNTRY_CODES *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]*

A mapping of EIA foreign coal mine country codes to 3-letter ISO-3166-1 codes.

The EIA-923 lists the US state of origin for coal deliveries using standard 2-letter US
state abbreviations. However, foreign countries are also included as “states” in this
category and because some of them have 2-letter abbreviation collisions with US states,
their coding is non-standard.

Instead of using the provided non-standard codes, we convert to the ISO-3166-1 three
letter country codes:
[https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3)

### pudl.transform.eia923.\_get_plant_nuclear_unit_id_map(nuc_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Get a plant_id -> nuclear_unit_id mapping for all plants with one nuclear unit.

* **Parameters:**
  **nuc_fuel** – dataframe of nuclear unit fuels.
* **Returns:**
  one to one mapping of plant_id_eia to nuclear_unit_id.
* **Return type:**
  plant_to_nuc_id

### pudl.transform.eia923.\_backfill_nuclear_unit_id(nuc_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Backfill 2001 and 2002 nuclear_unit_id for plants with one nuclear unit.

2001 and 2002 core_eia923_\_monthly_generation_fuel records do not include nuclear_unit_id which is
required for the primary key of nuclear_unit_fuel_eia923. We backfill this field for
plants with one nuclear unit. nuclear_unit_id is filled with ‘UNK’ if the
nuclear_unit_id can’t be recovered.

Params:
: nuc_fuel: nuclear fuels dataframe.

* **Returns:**
  nuclear fuels dataframe with backfilled nuclear_unit_id field.
* **Return type:**
  nuc_fuel

### pudl.transform.eia923.\_get_plant_prime_mover_map(gen_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[int](https://docs.python.org/3/library/functions.html#int), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Get a plant_id -> prime_mover_code mapping for all plants with one prime mover.

* **Parameters:**
  **gen_fuel** – dataframe of generation fuels.
* **Returns:**
  one to one mapping of plant_id_eia to prime_mover_codes.
* **Return type:**
  fuel_type_map

### pudl.transform.eia923.\_backfill_prime_mover_code(gen_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Backfill 2001 and 2002 prime_mover_code for plants with one prime mover.

2001 and 2002 core_eia923_\_monthly_generation_fuel records do not include prime_mover_code
which is required for the primary key. We backfill this field for plants
with one prime mover. prime_mover_code is set to ‘UNK’ if future plants
have multiple prime movers.

* **Parameters:**
  **gen_fuel** – generation fuels dataframe.
* **Returns:**
  generation fuels dataframe with backfilled prime_mover_code field.
* **Return type:**
  gen_fuel

### pudl.transform.eia923.\_get_most_frequent_energy_source_map(gen_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Get the a mapping of the most common energy_source for each fuel_type_code_agg.

* **Parameters:**
  **gen_fuel** – generation_fuel dataframe.
* **Returns:**
  mapping of fuel_type_code_agg to energy_source_code.
* **Return type:**
  energy_source_map

### pudl.transform.eia923.\_clean_gen_fuel_energy_sources(gen_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean the generator_fuel_eia923.energy_source_code field specifically.

Transformations include:

* Remap MSW to biogenic and non biogenic fuel types.
* Fill missing energy_source_code using most common code for each AER fuel codes.

* **Parameters:**
  **gen_fuel** – generation fuels dataframe.
* **Returns:**
  generation fuels dataframe with cleaned energy_source_code field.
* **Return type:**
  gen_fuel

### pudl.transform.eia923.\_aggregate_generation_fuel_duplicates(gen_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), nuclear: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Aggregate remaining duplicate generation fuels.

There are a handful of plants (< 100) whose prime_mover_code can’t be imputed
or duplicates exist in the raw table. We resolve these be aggregate the variable
fields.

* **Parameters:**
  * **gen_fuel** – generation fuels dataframe.
  * **nuclear** – adds nuclear_unit_id to list of natural key fields.
* **Returns:**
  generation fuels dataframe without duplicates in natural key fields.
* **Return type:**
  gen_fuel

### pudl.transform.eia923.\_yearly_to_monthly_records(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Converts an EIA 923 record of 12 months of data into 12 monthly records.

Much of the data reported in EIA 923 is monthly, but all 12 months worth of data is
reported in a single record, with one field for each of the 12 months.  This
function converts these annualized composite records into a set of 12 monthly
records containing the same information, by parsing the field names for months, and
adding a month field.  Non - time series data is retained in the same format.

* **Parameters:**
  **df** – A pandas DataFrame containing the annual data to be
  converted into monthly records.
* **Returns:**
  A dataframe containing the same data as was passed in via df,
  but with monthly records as rows instead of as columns.

### pudl.transform.eia923.\_coalmine_cleanup(cmi_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean up the core_eia923_\_entity_coalmine table.

This function does most of the core_eia923_\_entity_coalmine table transformation. It
is separate from the coalmine() transform function because of the peculiar way that
we are normalizing the [core_eia923_\_fuel_receipts_costs](../../../../data_dictionaries/pudl_db.md#core-eia923-fuel-receipts-costs) table.

All of the coalmine information is originally coming from the EIA
fuel_receipts_costs spreadsheet, but it really belongs in its own table. We strip it
out of FRC, and create that separate table, but then we need to refer to that table
through a foreign key. To do so, we actually merge the entire contents of the
coalmine table into FRC, including the surrogate key, and then drop the data fields.

For this to work, we need to have exactly the same coalmine data fields in both the
new coalmine table, and the FRC table. To ensure that’s true, we isolate the
transformations here in this function, and apply them to the coalmine columns in
both the FRC table and the coalmine table.

* **Parameters:**
  **cmi_df** – Coal mine information table (e.g. mine name, county, state)
* **Returns:**
  A cleaned DataFrame containing coalmine information.

### pudl.transform.eia923.plants_eia923(eia923_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], eia923_transformed_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Transforms the plants_eia923 table.

Much of the static plant information is reported repeatedly, and scattered across
several different pages of EIA 923. The data frame that this function uses is
assembled from those many different pages, and passed in via the same dictionary of
dataframes that all the other ingest functions use for uniformity.

Transformations include:

* Map full spelling onto code values.
* Convert Y/N columns to booleans.
* Remove excess white space around values.
* Drop duplicate rows.

* **Parameters:**
  * **eia923_dfs** – Each entry in this dictionary of DataFrame objects corresponds to a
    page from the EIA 923 form, as reported in the Excel spreadsheets they
    distribute.
  * **eia923_transformed_dfs** – A dictionary of DataFrame objects in which pages from
    EIA923 form (keys) correspond to normalized DataFrames of values from that
    page (values).
* **Returns:**
  A dictionary of DataFrame objects in which pages from EIA923 form (keys)
  correspond to normalized DataFrames of values from that page (values).

### pudl.transform.eia923.gen_fuel_nuclear(gen_fuel_nuke: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the core_eia923_\_monthly_generation_fuel_nuclear table.

Transformations include:

* Backfill nuclear_unit_ids for 2001 and 2002.
* Set all prime_mover_codes to ‘ST’.
* Aggregate remaining duplicate units.

* **Parameters:**
  **gen_fuel_nuke** – dataframe of nuclear unit fuels.
* **Returns:**
  Transformed nuclear generation fuel table.

### pudl.transform.eia923.\_core_eia923_\_pre_generation_fuel(raw_eia923_\_generation_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame))

Transforms the raw_eia923_\_generation_fuel table.

Transformations include:

* Remove fields implicated elsewhere.
* Replace . values with NA.
* Create a fuel_type_code_pudl field that organizes fuel types into
  clean, distinguishable categories.
* Combine year and month columns into a single date column.
* Clean and impute fuel_type field.
* Backfill missing prime_mover_codes
* Create a separate generation_fuel_nuclear table.
* Aggregate records with duplicate natural keys.
* Drop duplicate fields where only difference is energy_source_code and 0
  consumption data.

* **Parameters:**
  **raw_eia923_\_generation_fuel** – The raw `raw_eia923__generation_fuel` dataframe.
* **Returns:**
  Cleaned `eia923__generation_fuel` dataframe ready for harvesting.
  \_core_eia923_\_generation_fuel_nuclear: Cleaned `eia923__generation_fuel_nuclear` dataframe ready for harvesting.
* **Return type:**
  \_core_eia923_\_generation_fuel

### pudl.transform.eia923.\_map_prime_mover_sets(prime_mover_set: [numpy.ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html#numpy.ndarray)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Map unique prime mover combinations to a single prime mover code.

In 2001-2019 data, the .value_counts() of the combinations is:
(CA, CT)        750
(ST, CA)        101
(ST)             60
(CA)             17
(CS, ST, CT)      2
:param prime_mover_set: unique combinations of prime_mover_code
:type prime_mover_set: np.ndarray

* **Returns:**
  single prime mover code
* **Return type:**
  [str](https://docs.python.org/3/library/stdtypes.html#str)

### pudl.transform.eia923.\_aggregate_duplicate_boiler_fuel_keys(boiler_fuel_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Combine boiler_fuel rows with duplicate keys by aggregating them.

core_eia923_\_monthly_boiler_fuel contains a few records with duplicate keys, mostly caused by
CA and CT parts of combined cycle plants being mapped to the same boiler ID.
This is most likely a data entry error. See GitHub issue #852

One solution (implemented here) is to simply aggregate those records together.
This is cheap and easy compared to the more thorough solution of making
surrogate boiler IDs. Aggregation was preferred to purity due to the low volume of
affected records (4.5% of combined cycle plants).

* **Parameters:**
  **boiler_fuel_df** – the boiler_fuel dataframe
* **Returns:**
  A copy of boiler_fuel dataframe with duplicates removed and aggregates appended.

### pudl.transform.eia923.\_core_eia923_\_boiler_fuel(raw_eia923_\_boiler_fuel: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the core_eia923_\_monthly_boiler_fuel table.

Transformations include:

* Remove fields implicated elsewhere.
* Drop values with plant and boiler id values of NA.
* Replace . values with NA.
* Create a fuel_type_code_pudl field that organizes fuel types into clean,
  distinguishable categories.
* Combine year and month columns into a single date column.
* Drop duplicate rows with NA or 0 in all value columns.

Eventually we should truncate this table by the last year-month that was integrated.
Right now all months get integrated for a given year, regardless of whether there’s
data for them.

* **Parameters:**
  **raw_eia923_\_boiler_fuel** – The raw `raw_eia923__boiler_fuel` dataframe.
* **Returns:**
  Cleaned `core_eia923__monthly_boiler_fuel` dataframe ready for harvesting.

### pudl.transform.eia923.remove_duplicate_pks_boiler_fuel_eia923(bf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Deduplicate on primary keys for [core_eia923_\_monthly_boiler_fuel](../../../../data_dictionaries/pudl_db.md#core-eia923-monthly-boiler-fuel).

There are a relatively small number of records ~5% from the boiler fuel table that
have duplicate records based on what we believe is this table’s primary keys.
Fortunately, all of these duplicates have at least one records w/ only zeros and or
nulls. So this method drops only the records which have duplicate pks and only have
zeros or nulls in the non-primary key columns.

Note: There are 4 boilers in 2021 that are being dropped entirely during this
cleaning. They have BOTH duplicate pks and only have zeros or nulls in the
non-primary key columns. We could choose to preserve all instances of the pks even
after `drop_invalid_rows()` or only dropping one when there are two. We chose to
leave this be because it was minor and these boilers show up in other years.
See [comment](https://github.com/catalyst-cooperative/pudl/pull/2362#issuecomment-1470012538)
for more details.

### pudl.transform.eia923.\_core_eia923_\_generation(raw_eia923_\_generator: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the EIA 923 generation table.

Transformations include:

* Drop rows with NA for generator id.
* Remove fields implicated elsewhere.
* Replace . values with NA.
* Drop generator-date row duplicates (all have no data).

* **Parameters:**
  **raw_eia923_\_generator** – The raw `raw_eia923__generator` dataframe.
* **Returns:**
  Cleaned `_core_eia923__generation` dataframe ready for harvesting.

### pudl.transform.eia923.\_drop_duplicates_\_core_eia923_\_generation(gen_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), unit_test: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.transform.eia923.\_core_eia923_\_coalmine(raw_eia923_\_fuel_receipts_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the raw_eia923_\_fuel_receipts_costs table.

Transformations include:

* Remove fields implicated elsewhere.
* Drop duplicates with MSHA ID.

* **Parameters:**
  **raw_eia923_\_fuel_receipts_costs** – raw precursor to the
  [core_eia923_\_fuel_receipts_costs](../../../../data_dictionaries/pudl_db.md#core-eia923-fuel-receipts-costs) table.
* **Returns:**
  Cleaned `_core_eia923__coalmine` dataframe ready for harvesting.

### pudl.transform.eia923.\_core_eia923_\_fuel_receipts_costs(raw_eia923_\_fuel_receipts_costs: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_eia923_\_coalmine: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the eia923_\_fuel_receipts_costs dataframe.

Transformations include:

* Remove fields implicated elsewhere.
* Replace . values with NA.
* Standardize codes values.
* Fix dates.
* Replace invalid mercury content values with NA.

Fuel cost is reported in cents per mmbtu. Converts cents to dollars.

* **Parameters:**
  * **raw_eia923_\_fuel_receipts_costs** – The raw `raw_eia923__fuel_receipts_costs` dataframe.
  * **\_core_eia923_\_coalmine** – The cleaned pre-harvest EIA 923 coal mine dataframe.
* **Returns:**
  Cleaned `eia923__fuel_receipts_costs` dataframe ready for harvesting.

### pudl.transform.eia923.\_core_eia923_\_monthly_cooling_system_information(raw_eia923_\_cooling_system_information: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the eia923_\_cooling_system_information dataframe.

Applies typical NA conversion and date conversion.

As of 2024-02-28: 2008 and 2009 only have annual rates, but the
“_rate_gallons_per_minute” values are otherwise monthly; we leave them NA
for 2008-2009 to avoid confusion.

As of 2024-02-28: In 2008 and 2009 the rate columns are labeled as “0.1
cubic feet per second”; In 2013 they change it to gallons/min.

If taken to mean that the earlier unit is literally deci-cubic-feet per
second, we find that all these rates jump 10x when we hit the 2013 data;
so we interpret “0.1 cubic feet per second” to mean “cubic feet per
second, with precision of 0.1 cfs.”

### pudl.transform.eia923.cooling_system_information_continuity(csi)

Check to see if columns vary as slowly as expected.

### pudl.transform.eia923.\_build_emissions_control_dates(raw_value: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), year: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), month: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), day: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), min_valid_year: [int](https://docs.python.org/3/library/functions.html#int), max_valid_year: [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Validate date parts and build parsed timestamps.

This helper centralizes range checks and date construction so all recognized input
formats (full date, month/year, compact monthyear, year-only) use the same
validation rules:

* month `00` is coerced to January
* years must be in `[min_valid_year, max_valid_year]`
* invalid calendar dates (e.g. day 32) raise

* **Parameters:**
  * **raw_value** – Original raw date strings, used to report readable error messages.
  * **year** – Parsed year components.
  * **month** – Parsed month components.
  * **day** – Parsed day components.
  * **min_valid_year** – Lower accepted year bound.
  * **max_valid_year** – Upper accepted year bound.
* **Returns:**
  Parsed timestamps for valid date parts.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – If year/month/day parts violate expected constraints.

### pudl.transform.eia923.\_parse_emissions_control_date_subset(raw_subset: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), pattern: [str](https://docs.python.org/3/library/stdtypes.html#str), min_valid_year: [int](https://docs.python.org/3/library/functions.html#int), max_valid_year: [int](https://docs.python.org/3/library/functions.html#int), month_group: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), day_group: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None), default_month: [int](https://docs.python.org/3/library/functions.html#int) = 1, default_day: [int](https://docs.python.org/3/library/functions.html#int) = 1) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Parse one recognized emissions-control date format for a subset of rows.

* **Parameters:**
  * **raw_subset** – Raw date strings for rows matching one known format.
  * **pattern** – Regex pattern with a required `year` capture group and optional
    `month` / `day` groups.
  * **min_valid_year** – Lower accepted year bound.
  * **max_valid_year** – Upper accepted year bound.
  * **month_group** – Name of the extracted month group or `None` to use
    `default_month`.
  * **day_group** – Name of the extracted day group or `None` to use `default_day`.
  * **default_month** – Month value used when `month_group` is omitted.
  * **default_day** – Day value used when `day_group` is omitted.
* **Returns:**
  Parsed datetime series for the input subset, with `0000` year fragments
  converted to `NaT` before year normalization.

### pudl.transform.eia923.\_clean_emissions_control_dates(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), min_valid_year: [int](https://docs.python.org/3/library/functions.html#int), max_valid_year: [int](https://docs.python.org/3/library/functions.html#int), spot_fixes: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.Timestamp](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Timestamp.html#pandas.Timestamp)] | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Parse raw EIA-923 emissions-control date strings into datetimes.

Supported formats include:

* compact month-year: `012024`
* delimited month-year: `01-2024`, `1/20`
* full dates: `1/11/07`
* year-only values: `1990` (treated as January 1st)

Known non-date sentinel tokens (e.g. `na`, `CEM`) are converted to `NaT`.

Parsing is intentionally strict. Any value that is not explicitly recognized and
validated will raise `ValueError` so future upstream format changes surface
during ETL/testing rather than silently becoming bad timestamps.

* **Parameters:**
  * **col** – Raw date-like values to clean.
  * **spot_fixes** – Optional table-specific mapping of raw string values to corrected
    timestamps. If provided, these mappings are applied before pattern parsing.
* **Returns:**
  A `datetime64[ns]` Series containing parsed timestamps and `NaT` for known
  missing/sentinel values.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – If characters, structure, or date parts violate parsing
  assumptions.

### pudl.transform.eia923.\_core_eia923_\_yearly_fgd_operation_maintenance(raw_eia923_\_fgd_operation_maintenance: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the \_core_eia923_\_yearly_fgd_operation_maintenance table.

Transformations include:

* Drop values with plant and boiler id values of NA.
* Replace . values with NA.
* Convert thousands of dollars to dollars.
* Fix datetimes for SO2 test dates.
* Ensure a unique primary key and drop some duplicated rows.

* **Parameters:**
  **raw_eia923_\_fgd_operation_maintenance** – The raw
  `raw_eia923__fgd_operation_maintenance` dataframe.
* **Returns:**
  Cleaned `_core_eia923__yearly_fgd_operation_maintenance` dataframe ready for
  harvesting.

### pudl.transform.eia923.fgd_continuity_check(fgd)

Check to see if columns vary as slowly as expected.

### pudl.transform.eia923.\_core_eia923_\_energy_storage(raw_eia923_\_energy_storage: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the eia923_energy_storage table.

Transformations include:

* Replace . values with NA.
* Clean up `fuel_unit` strings.
* Make wide monthly columns into tall monthly columns.
* Convert date to month.
* Encode relevant columns.

Other cleaning that could be done:

* Come up with an encoder for `fuel_unit` (tricky because different between FERC
  and EIA).

### pudl.transform.eia923.\_core_eia923_\_yearly_byproduct_disposition(raw_eia923_\_byproduct_disposition: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the eia923_\_byproduct_disposition table.

Transformations include:

* Replace . values with NA
* Drop rows with NA byproduct_description. This also removes all duplicates based on
  : report_date, plant_id_eia, and byproduct_description
* Convert 1000 tons to tons (avoiding steam sales, which are reported in MMBtu)
* Create a byproducts_unit column based on the byproduct_disposition

* **Parameters:**
  * **raw_eia923_\_byproduct_disposition** – The raw `raw_eia923__byproduct_disposition`
  * **dataframe.**
* **Returns:**
  Cleaned `core_eia923__byproduct_disposition` dataframe ready for harvesting.

### pudl.transform.eia923.disposition_continuity_check(bpd)

Check to see if columns vary as slowly as expected.

### pudl.transform.eia923.\_core_eia923_\_yearly_byproduct_expenses_and_revenues(raw_eia923_\_byproduct_expenses_and_revenues: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the eia923_\_byproduct_expenses_and_revenues table.

Transformations include:

* Standardize NA values
* Convert integer year to datetime
* Convert 1000 dollars (opex and revenue columns) to dollars

* **Parameters:**
  **raw_eia923_\_byproduct_expenses_and_revenues** – The raw
  `raw_eia923__byproduct_expenses_and_revenues` dataframe.
* **Returns:**
  Cleaned `_core_eia923__yearly_byproduct_expenses_and_revenues` dataframe ready
  for harvesting.

### pudl.transform.eia923.\_core_eia923_\_yearly_emissions_control(raw_eia923_\_emissions_control: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the eia923_\_emissions_control table.

Transformations include:

* Standardize NA values
* Convert units from thousands of tons to tons
* Clean and standardize format of month-year date string columns

* **Parameters:**
  * **raw_eia923_\_emissions_control** – The raw `raw_eia923__emissions_control`
  * **dataframe.**
* **Returns:**
  Cleaned `_core_eia923__yearly_emissions_control` dataframe ready for
  harvesting.
