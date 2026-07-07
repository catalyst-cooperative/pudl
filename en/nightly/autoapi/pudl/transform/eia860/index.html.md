# pudl.transform.eia860

Module to perform data cleaning functions on EIA860 data tables.

## Attributes

| [`logger`](#pudl.transform.eia860.logger)   |    |
|---------------------------------------------|----|

## Functions

| [`_core_eia860__ownership`](#pudl.transform.eia860._core_eia860__ownership)(→ pandas.DataFrame)                                              | Pull and transform the ownership table.                            |
|----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| [`_core_eia860__generators`](#pudl.transform.eia860._core_eia860__generators)(→ pandas.DataFrame)                                            | Pull and transform the generators table.                           |
| [`_core_eia860__generators_solar`](#pudl.transform.eia860._core_eia860__generators_solar)(→ pandas.DataFrame)                                | Transform the solar-specific generators table.                     |
| [`_core_eia860__generators_energy_storage`](#pudl.transform.eia860._core_eia860__generators_energy_storage)(→ pandas.DataFrame)              | Transform the energy storage specific generators table.            |
| [`_core_eia860__generators_wind`](#pudl.transform.eia860._core_eia860__generators_wind)(→ pandas.DataFrame)                                  | Transform the wind-specific generators table.                      |
| [`_core_eia860__generators_multifuel`](#pudl.transform.eia860._core_eia860__generators_multifuel)(→ pandas.DataFrame)                        | Transform the multifuel generators table.                          |
| [`_core_eia860__plants`](#pudl.transform.eia860._core_eia860__plants)(→ pandas.DataFrame)                                                    | Pull and transform the plants table.                               |
| [`_core_eia860__boiler_generator_assn`](#pudl.transform.eia860._core_eia860__boiler_generator_assn)(→ pandas.DataFrame)                      | Pull and transform the boilder generator association table.        |
| [`_core_eia860__utilities`](#pudl.transform.eia860._core_eia860__utilities)(→ pandas.DataFrame)                                              | Pull and transform the utilities table.                            |
| [`_core_eia860__boilers`](#pudl.transform.eia860._core_eia860__boilers)(→ pandas.DataFrame)                                                  | Pull and transform the boilers table.                              |
| [`_core_eia860__emissions_control_equipment`](#pudl.transform.eia860._core_eia860__emissions_control_equipment)(...)                         | Pull and transform the emissions control equipment table.          |
| [`_core_eia860__boiler_emissions_control_equipment_assn`](#pudl.transform.eia860._core_eia860__boiler_emissions_control_equipment_assn)(...) | Pull and transform the emissions control <> boiler ID link tables. |
| [`_core_eia860__boiler_cooling`](#pudl.transform.eia860._core_eia860__boiler_cooling)(→ pandas.DataFrame)                                    | Pull and transform the EIA 860 boiler to cooler ID table.          |
| [`_core_eia860__boiler_stack_flue`](#pudl.transform.eia860._core_eia860__boiler_stack_flue)(→ pandas.DataFrame)                              | Pull and transform the EIA 860 boiler to stack flue ID table.      |
| [`_core_eia860__cooling_equipment`](#pudl.transform.eia860._core_eia860__cooling_equipment)(→ pandas.DataFrame)                              | Transform the EIA 860 cooling equipment table.                     |
| [`cooling_equipment_continuity`](#pudl.transform.eia860.cooling_equipment_continuity)(cooling_equipment)                                     | Check to see if columns vary as slowly as expected.                |
| [`_core_eia860__fgd_equipment`](#pudl.transform.eia860._core_eia860__fgd_equipment)(→ pandas.DataFrame)                                      | Transform the EIA 860 FGD equipment table.                         |
| [`fgd_equipment_continuity`](#pudl.transform.eia860.fgd_equipment_continuity)(fgd)                                                           | Check to see if columns vary as slowly as expected.                |

## Module Contents

### pudl.transform.eia860.logger

### pudl.transform.eia860.\_core_eia860_\_ownership(raw_eia860_\_ownership: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the ownership table.

Transformations include:

* Replace . values with NA.
* Convert pre-2012 ownership percentages to proportions to match post-2012
  reporting.

* **Parameters:**
  **raw_eia860_\_ownership** – The raw `raw_eia860__ownership` dataframe.
* **Returns:**
  Cleaned `_core_eia860__ownership` dataframe ready for harvesting.

### pudl.transform.eia860.\_core_eia860_\_generators(raw_eia860_\_generator_proposed: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the generators table.

There are three tabs that the generator records come from (proposed, existing,
retired). Pre 2009, the existing and retired data are lumped together under a single
generator file with one tab. We pull each tab into one dataframe and include an
`operational_status` to indicate which tab the record came from. We use
`operational_status` to parse the pre 2009 files as well.

Transformations include:

* Replace . values with NA.
* Update `operational_status_code` to reflect plant status as either proposed,
  existing or retired.
* Drop values with NA for plant and generator id.
* Replace 0 values with NA where appropriate.
* Convert Y/N/X values to boolean True/False.
* Convert U/Unknown values to NA.
* Map full spelling onto code values.
* Create a fuel_type_code_pudl field that organizes fuel types into
  clean, distinguishable categories.

* **Parameters:**
  * **raw_eia860_\_generator_proposed** – The raw `raw_eia860__generator_proposed` dataframe.
  * **raw_eia860_\_generator_existing** – The raw `raw_eia860__generator_existing` dataframe.
  * **raw_eia860_\_generator_retired** – The raw `raw_eia860__generator_retired` dataframe.
  * **raw_eia860_\_generator** – The raw `raw_eia860__generator` dataframe.
* **Returns:**
  Cleaned `_core_eia860__generators` dataframe ready for harvesting.

### pudl.transform.eia860.\_core_eia860_\_generators_solar(raw_eia860_\_generator_solar_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator_solar_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the solar-specific generators table.

Many of the same transforms to the core generators table are applied here.
Most of the unique solar columns are booleans.

Notes for possible future cleaning:

* Both the `tilt_angle` and `azimuth_angle` columns have a small number of
  negative values (both under 40 records). This seems off, but not impossible?
* A lot of the boolean columns in this table are mostly null. It is probably
  that a lot of the nulls should correspond to False’s, but there is no sure way
  to know, so nulls seem more appropriate.

### pudl.transform.eia860.\_core_eia860_\_generators_energy_storage(raw_eia860_\_generator_energy_storage_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator_energy_storage_proposed: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator_energy_storage_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the energy storage specific generators table.

### pudl.transform.eia860.\_core_eia860_\_generators_wind(raw_eia860_\_generator_wind_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_generator_wind_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the wind-specific generators table.

Many of the same transforms to the core generators table are applied here.

Some notes for possible cleaning later:

* technology_description: this field didn’t exist in 2013. We could try to backfill.
  this is an annual scd so it’ll get slurped up there and backfilling does happen
  in the output layer via [`pudl.output.eia.fill_generator_technology_description()`](../../output/eia/index.md#pudl.output.eia.fill_generator_technology_description)
* turbines_num: this field doesn’t show up in this table for 2013 and 2014, but it does
  exist in the 2001-2012 generators tab. This is an annual generator scd.

### pudl.transform.eia860.\_core_eia860_\_generators_multifuel(raw_eia860_\_multifuel_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_multifuel_proposed: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_multifuel_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the multifuel generators table.

### pudl.transform.eia860.\_core_eia860_\_plants(raw_eia860_\_plant: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the plants table.

Much of the static plant information is reported repeatedly, and scattered across
several different pages of EIA 923. The data frame which this function uses is
assembled from those many different pages, and passed in via the same dictionary of
dataframes that all the other ingest functions use for uniformity.

Transformations include:

* Replace . values with NA.
* Homogenize spelling of county names.
* Convert Y/N/X values to boolean True/False.

* **Parameters:**
  **raw_eia860_\_plant** – The raw `raw_eia860__plant` dataframe.
* **Returns:**
  Cleaned `_core_eia860__plants` dataframe ready for harvesting.

### pudl.transform.eia860.\_core_eia860_\_boiler_generator_assn(raw_eia860_\_boiler_generator_assn: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the boilder generator association table.

Transformations include:

* Drop non-data rows with EIA notes.
* Drop duplicate rows.

* **Parameters:**
  **raw_eia860_\_boiler_generator_assn** – Each entry in this dictionary of DataFrame
  objects corresponds to a page from the EIA860 form, as reported in the Excel
  spreadsheets they distribute.
* **Returns:**
  Cleaned `_core_eia860__boiler_generator_assn` dataframe ready for harvesting.

### pudl.transform.eia860.\_core_eia860_\_utilities(raw_eia860_\_utility: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the utilities table.

Transformations include:

* Replace . values with NA.
* Fix typos in state abbreviations, convert to uppercase.
* Drop address_3 field (all NA).
* Combine phone number columns into one field and set values that don’t mimic real
  US phone numbers to NA.
* Convert Y/N/X values to boolean True/False.
* Map full spelling onto code values.

* **Parameters:**
  **raw_eia860_\_utility** – The raw `raw_eia860__utility` dataframe.
* **Returns:**
  Cleaned `_core_eia860__utilities` dataframe ready for harvesting.

### pudl.transform.eia860.\_core_eia860_\_boilers(raw_eia860_\_emission_control_strategies: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_boiler_info: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the boilers table.

Transformations include:

* Replace . values with NA.
* Convert Y/N/NA values to boolean True/False.
* Combine month and year columns into date columns.
* Add boiler manufacturer name column.
* Convert pre-2012 efficiency percentages to proportions to match post-2012
  reporting.

* **Parameters:**
  * **raw_eia860_\_emission_control_strategies** – DataFrame extracted from EIA forms
    earlier in the ETL process.
  * **raw_eia860_\_boiler_info** – DataFrame extracted from EIA forms earlier in the ETL
    process.
* **Returns:**
  The transformed boilers table.

### pudl.transform.eia860.\_core_eia860_\_emissions_control_equipment(raw_eia860_\_emissions_control_equipment: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the emissions control equipment table.

### pudl.transform.eia860.\_core_eia860_\_boiler_emissions_control_equipment_assn(raw_eia860_\_boiler_so2: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_boiler_mercury: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_boiler_nox: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860_\_boiler_particulate: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the emissions control <> boiler ID link tables.

* **Parameters:**
  * **raw_eia860_\_boiler_so2** – Raw EIA 860 boiler to SO2 emission control equipment
    association table.
  * **raw_eia860_\_boiler_mercury** – Raw EIA 860 boiler to mercury emission control
    equipment association table.
  * **raw_eia860_\_boiler_nox** – Raw EIA 860 boiler to nox emission control equipment
    association table.
  * **raw_eia860_\_boiler_particulate** – Raw EIA 860 boiler to particulate emission
    control equipment association table.
  * **raw_eia860_\_boiler_cooling** – Raw EIA 860 boiler to cooling equipment association
    table.
  * **raw_eia860_\_boiler_stack_flue** – Raw EIA 860 boiler to stack flue equipment
    association table.
* **Returns:**
  A combination of all the emission control equipment association tables.

### pudl.transform.eia860.\_core_eia860_\_boiler_cooling(raw_eia860_\_boiler_cooling: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the EIA 860 boiler to cooler ID table.

* **Parameters:**
  **raw_eia860_\_boiler_cooling** – Raw EIA 860 boiler to cooler ID association table.
* **Returns:**
  A cleaned and normalized version of the EIA boiler to cooler ID table.

### pudl.transform.eia860.\_core_eia860_\_boiler_stack_flue(raw_eia860_\_boiler_stack_flue: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Pull and transform the EIA 860 boiler to stack flue ID table.

* **Parameters:**
  **raw_eia860_\_boiler_stack_flue** – Raw EIA 860 boiler to stack flue ID association
  table.
* **Returns:**
  A cleaned and normalized version of the EIA boiler to stack flue ID table.

### pudl.transform.eia860.\_core_eia860_\_cooling_equipment(raw_eia860_\_cooling_equipment: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the EIA 860 cooling equipment table.

- spot clean year values before converting to dates
- standardize water rate units to gallons per minute (2009-2013 used cubic
  feet per second)
- convert kilodollars to normal dollars

Note that the “power_requirement_mw” field is erroneously reported as
“_kwh” in the raw data for 2009, 2010, and 2011, even though the values are
in MW. This is corroborated by the values for these plants matching up with
the stated MW values in later years. Additionally, the PDF form for those
years indicates that the value should be in MW, and KWh isn’t even a power
measurement.

In 2009, we have two incorrectly entered `cooling_type` values of `HR`,
for utility ID 14328, plant IDs 56532/56476, and cooling ID ACC1. This
corresponds to the Colusa and Gateway generating stations run by PG&E. In
all later years, these cooling facilities are marked as `DC`, or “dry
cooling”; however, `HR` looks like the codes for hybrid systems (the
others are `HRC`, `HRF`, `HRI`). As such we drop the `HR` code
completely in `pudl.metadata.codes`.

### pudl.transform.eia860.cooling_equipment_continuity(cooling_equipment)

Check to see if columns vary as slowly as expected.

2024-03-04: pond cost, tower cost, and tower cost all have one-off
discontinuities that are worth investigating, but we’re punting on that
investigation since we’re out of time.

### pudl.transform.eia860.\_core_eia860_\_fgd_equipment(raw_eia860_\_fgd_equipment: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \_core_censuspep_\_yearly_geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform the EIA 860 FGD equipment table.

Transformations include:
- convert string booleans to boolean dtypes, and mixed strings/numbers to numbers
- convert kilodollars to normal dollars
- handle mixed reporting of percentages
- spot fix a duplicated SO2 control ID
- change an old water code to preserve detail of reporting over time
- add manufacturer name based on the code reported

### pudl.transform.eia860.fgd_equipment_continuity(fgd)

Check to see if columns vary as slowly as expected.
