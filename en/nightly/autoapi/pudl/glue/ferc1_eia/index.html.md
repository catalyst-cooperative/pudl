# pudl.glue.ferc1_eia

Extract and transform glue tables between FERC Form 1 and EIA 860/923.

FERC1 and EIA report on many of the same plants and utilities, but have no embedded
connection. We have combed through the FERC and EIA plants and utilities to generate
id’s which can connect these datasets. The resulting fields in the PUDL tables are
`plant_id_pudl` and `utility_id_pudl`, respectively. This was done by hand in a
spreadsheet which is in the `package_data/glue` directory. When mapping plants, we
considered a plant a co-located collection of electricity generation equipment. If a
coal plant was converted to a natural gas unit, our aim was to consider this the same
plant.  This module simply reads in the mapping spreadsheet and converts it to a
dictionary of dataframes.

Because these mappings were done by hand and for every one of FERC Form 1’s thousands of
reported plants, we know there are probably some incorrect or incomplete mappings. If
you see a `plant_id_pudl` or `utility_id_pudl` mapping that you think is incorrect,
please open an issue on our Github!

Note that the PUDL IDs may change over time. They are not guaranteed to be stable. If
you need to find a particular plant or utility reliably, you should use its
`plant_id_eia`, `utility_id_eia`, or `utility_id_ferc1`.

Another note about these id’s: these id’s map our definition of plants, which is not the
most granular level of plant unit. The generators are typically the smaller, more
interesting unit. FERC does not typically report in units (although it sometimes does),
but it does often break up gas units from coal units. EIA reports on the generator and
boiler level. When trying to use these PUDL id’s, consider the granularity that you
desire and the potential implications of using a co-located set of plant infrastructure
as an id.

## Attributes

| [`logger`](#pudl.glue.ferc1_eia.logger)                               |                                                       |
|-----------------------------------------------------------------------|-------------------------------------------------------|
| [`PUDL_ID_MAP_XLSX`](#pudl.glue.ferc1_eia.PUDL_ID_MAP_XLSX)           | Path to the PUDL ID mapping sheet with the plant map. |
| [`UTIL_ID_PUDL_MAP_CSV`](#pudl.glue.ferc1_eia.UTIL_ID_PUDL_MAP_CSV)   | Path to the PUDL utility ID mapping CSV.              |
| [`UTIL_ID_FERC_MAP_CSV`](#pudl.glue.ferc1_eia.UTIL_ID_FERC_MAP_CSV)   | Path to the PUDL-assign FERC1 utility ID mapping CSV. |
| [`MIN_PLANT_CAPACITY_MW`](#pudl.glue.ferc1_eia.MIN_PLANT_CAPACITY_MW) |                                                       |
| [`MAX_LOST_PLANTS_EIA`](#pudl.glue.ferc1_eia.MAX_LOST_PLANTS_EIA)     |                                                       |
| [`MAX_LOST_UTILS_EIA`](#pudl.glue.ferc1_eia.MAX_LOST_UTILS_EIA)       |                                                       |

## Classes

| [`GenericPlantFerc1TableTransformer`](#pudl.glue.ferc1_eia.GenericPlantFerc1TableTransformer)   | Generic plant table transformer.   |
|-------------------------------------------------------------------------------------------------|------------------------------------|

## Functions

| [`get_plant_map`](#pudl.glue.ferc1_eia.get_plant_map)(→ pandas.DataFrame)                                               | Read in the manual FERC to EIA plant mapping data.                           |
|-------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| [`get_utility_map_pudl`](#pudl.glue.ferc1_eia.get_utility_map_pudl)(→ pandas.DataFrame)                                 | Read in the manual FERC to EIA utility mapping data.                         |
| [`get_utility_map_ferc1`](#pudl.glue.ferc1_eia.get_utility_map_ferc1)(→ pandas.DataFrame)                               | Read in the manual XBRL to DBF FERC1 utility mapping data.                   |
| [`get_mapped_plants_eia`](#pudl.glue.ferc1_eia.get_mapped_plants_eia)(→ pandas.DataFrame)                               | Get a list of all EIA plants that have been assigned PUDL Plant IDs.         |
| [`get_util_ids_ferc1_raw_xbrl`](#pudl.glue.ferc1_eia.get_util_ids_ferc1_raw_xbrl)(→ pandas.DataFrame)                   | Grab the utility ids (reported as entity_id) in the FERC1 XBRL database.     |
| [`get_util_ids_ferc1_raw_dbf`](#pudl.glue.ferc1_eia.get_util_ids_ferc1_raw_dbf)(→ pandas.DataFrame)                     | Grab the utility ids (reported as respondent_id) in the FERC1 DBF database.  |
| [`get_plants_ferc1_raw_job`](#pudl.glue.ferc1_eia.get_plants_ferc1_raw_job)(→ dagster.JobDefinition)                    | Pull all plants in the FERC Form 1 DBF and XBRL DB for given years.          |
| [`get_missing_ids`](#pudl.glue.ferc1_eia.get_missing_ids)(→ pandas.Index)                                               | Identify IDs that are missing from the left df but show up in the right df.  |
| [`label_missing_ids_for_manual_mapping`](#pudl.glue.ferc1_eia.label_missing_ids_for_manual_mapping)(→ pandas.DataFrame) | Label unmapped IDs for manual mapping.                                       |
| [`label_plants_eia`](#pudl.glue.ferc1_eia.label_plants_eia)(→ pandas.DataFrame)                                         | Label plants with columns helpful in manual mapping.                         |
| [`label_utilities_ferc1_dbf`](#pudl.glue.ferc1_eia.label_utilities_ferc1_dbf)(→ pandas.DataFrame)                       | Get the DBF FERC1 utilities with their names.                                |
| [`label_utilities_ferc1_xbrl`](#pudl.glue.ferc1_eia.label_utilities_ferc1_xbrl)(→ pandas.DataFrame)                     | Get the XBRL FERC1 utilities with their names.                               |
| [`get_utility_most_recent_capacity`](#pudl.glue.ferc1_eia.get_utility_most_recent_capacity)(→ pandas.DataFrame)         | Calculate total generation capacity by utility in most recent reported year. |
| [`get_core_eia923_plant_ids`](#pudl.glue.ferc1_eia.get_core_eia923_plant_ids)()                                         | Compile Plant IDs for all plants that appear in core EIA-923 tables.         |
| [`get_util_ids_eia_unmapped`](#pudl.glue.ferc1_eia.get_util_ids_eia_unmapped)(→ pandas.DataFrame)                       | Get a list of all the EIA Utilities in the PUDL DB without PUDL IDs.         |
| [`glue`](#pudl.glue.ferc1_eia.glue)([ferc1, eia])                                                                       | Generates a dictionary of dataframes for glue tables between FERC1, EIA.     |

## Module Contents

### pudl.glue.ferc1_eia.logger

### pudl.glue.ferc1_eia.PUDL_ID_MAP_XLSX

Path to the PUDL ID mapping sheet with the plant map.

### pudl.glue.ferc1_eia.UTIL_ID_PUDL_MAP_CSV

Path to the PUDL utility ID mapping CSV.

### pudl.glue.ferc1_eia.UTIL_ID_FERC_MAP_CSV

Path to the PUDL-assign FERC1 utility ID mapping CSV.

### pudl.glue.ferc1_eia.MIN_PLANT_CAPACITY_MW *: [float](https://docs.python.org/3/library/functions.html#float)* *= 5.0*

### pudl.glue.ferc1_eia.MAX_LOST_PLANTS_EIA *: [int](https://docs.python.org/3/library/functions.html#int)* *= 50*

### pudl.glue.ferc1_eia.MAX_LOST_UTILS_EIA *: [int](https://docs.python.org/3/library/functions.html#int)* *= 10*

### pudl.glue.ferc1_eia.get_plant_map() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Read in the manual FERC to EIA plant mapping data.

### pudl.glue.ferc1_eia.get_utility_map_pudl() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Read in the manual FERC to EIA utility mapping data.

### pudl.glue.ferc1_eia.get_utility_map_ferc1() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Read in the manual XBRL to DBF FERC1 utility mapping data.

### pudl.glue.ferc1_eia.get_mapped_plants_eia() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get a list of all EIA plants that have been assigned PUDL Plant IDs.

Read in the list of already mapped EIA plants from the FERC 1 / EIA plant
and utility mapping spreadsheet kept in the package_data.

* **Returns:**
  A DataFrame listing the plant_id_eia and plant_name_eia values for every EIA
  plant which has already been assigned a PUDL Plant ID.

### pudl.glue.ferc1_eia.get_util_ids_ferc1_raw_xbrl(ferc1_engine_xbrl: sqlalchemy.Engine) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Grab the utility ids (reported as entity_id) in the FERC1 XBRL database.

### pudl.glue.ferc1_eia.get_util_ids_ferc1_raw_dbf(ferc1_engine_dbf: sqlalchemy.Engine) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Grab the utility ids (reported as respondent_id) in the FERC1 DBF database.

### *class* pudl.glue.ferc1_eia.GenericPlantFerc1TableTransformer(table_id: [pudl.transform.ferc1.TableIdFerc1](../../transform/ferc1/index.md#pudl.transform.ferc1.TableIdFerc1), \*\*kwargs)

Bases: [`pudl.transform.ferc1.Ferc1AbstractTableTransformer`](../../transform/ferc1/index.md#pudl.transform.ferc1.Ferc1AbstractTableTransformer)

Generic plant table transformer.

Intended for use in compiling all plant names for manual ID mapping.

#### table_id

Name of the PUDL database table that this table transformer produces.

Must be defined in the database schema / metadata. This ID is used to instantiate
the appropriate `TableTransformParams` object.

#### transform(raw_dbf_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], raw_xbrl_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Only apply the generic :meth:`transform_start`.

#### transform_main(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Basic name normalization and dropping of invalid rows.

#### drop_invalid_rows(df)

Add required valid columns before running standard drop_invalid_rows.

This parent classes’ method drops the whole df if all of the
`require_valid_columns` don’t exist in the df. For the glue tests only, we add
in empty required columns because we know that the real ETL adds columns during
the full transform step.

### pudl.glue.ferc1_eia.get_plants_ferc1_raw_job() → [dagster.JobDefinition](https://docs.dagster.io/api/dagster/jobs/#dagster.JobDefinition)

Pull all plants in the FERC Form 1 DBF and XBRL DB for given years.

This job expects ferc1_dbf.sqlite and ferc_xbrl.sqlite databases to be populated.

### pudl.glue.ferc1_eia.get_missing_ids(ids_left: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), ids_right: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), id_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.Index](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.html#pandas.Index)

Identify IDs that are missing from the left df but show up in the right df.

* **Parameters:**
  * **ids_left** – table which contains `id_cols` to be used as left table in join.
  * **ids_right** – table which contains `id_cols` to be used as left table in join.
  * **id_cols** – list of ID column(s)
* **Returns:**
  index of unique values in `id_cols` that exist in `ids_right` but not
  `ids_left`.

### pudl.glue.ferc1_eia.label_missing_ids_for_manual_mapping(missing_ids: [pandas.Index](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Index.html#pandas.Index), label_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Label unmapped IDs for manual mapping.

### pudl.glue.ferc1_eia.label_plants_eia(out_eia_\_yearly_plants: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Label plants with columns helpful in manual mapping.

### pudl.glue.ferc1_eia.label_utilities_ferc1_dbf(core_pudl_\_assn_ferc1_dbf_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), util_ids_ferc1_raw_dbf: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the DBF FERC1 utilities with their names.

### pudl.glue.ferc1_eia.label_utilities_ferc1_xbrl(core_pudl_\_assn_ferc1_xbrl_pudl_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), util_ids_ferc1_raw_xbrl: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get the XBRL FERC1 utilities with their names.

### pudl.glue.ferc1_eia.get_utility_most_recent_capacity() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Calculate total generation capacity by utility in most recent reported year.

### pudl.glue.ferc1_eia.get_core_eia923_plant_ids()

Compile Plant IDs for all plants that appear in core EIA-923 tables.

### pudl.glue.ferc1_eia.get_util_ids_eia_unmapped(out_eia_\_yearly_utilities: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), out_eia_\_yearly_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), utilities_eia_mapped: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get a list of all the EIA Utilities in the PUDL DB without PUDL IDs.

Identify any EIA Utility that appears in the data but does not have a
utility_id_pudl associated with it in our ID mapping spreadsheet. Label some of
those utilities for potential linkage to FERC 1 utilities, but only if they have
plants which report data somewhere in the EIA-923 data tables. For those utilities
that do have plants reporting in EIA-923, sum up the total capacity of all of their
plants and include that in the output dataframe so that we can effectively
prioritize mapping them.

### pudl.glue.ferc1_eia.glue(ferc1: [bool](https://docs.python.org/3/library/functions.html#bool) = False, eia: [bool](https://docs.python.org/3/library/functions.html#bool) = False)

Generates a dictionary of dataframes for glue tables between FERC1, EIA.

That data is primarily stored in the plant_output and
utility_output tabs of package_data/glue/pudl_id_mapping.xlsx in the
repository. There are a total of seven relations described in this data:

> - utilities: Unique id and name for each utility for use across the
>   PUDL DB.
> - plants: Unique id and name for each plant for use across the PUDL DB.
> - core_pudl_\_assn_eia_pudl_utilities: EIA operator ids and names attached to a PUDL
>   utility id.
> - core_pudl_\_assn_eia_pudl_plants: EIA plant ids and names attached to a PUDL plant id.
> - utilities_ferc: FERC respondent ids & names attached to a PUDL
>   utility id.
> - plants_ferc: A combination of FERC plant names and respondent ids,
>   associated with a PUDL plant ID. This is necessary because FERC does
>   not provide plant ids, so the unique plant identifier is a
>   combination of the respondent id and plant name.
> - core_pudl_\_assn_utilities_plants: An association table which describes which plants
>   have relationships with what utilities. If a record exists in this
>   table then combination of PUDL utility id & PUDL plant id does have
>   an association of some kind. The nature of that association is
>   somewhat fluid, and more scrutiny will likely be required for use in
>   analysis.

Presently, the ‘glue’ tables are a very basic piece of infrastructure for
the PUDL DB, because they contain the primary key fields for utilities and
plants in FERC1.

* **Parameters:**
  * **ferc1** – Are we ingesting FERC Form 1 data?
  * **eia** – Are we ingesting EIA data?
* **Returns:**
  A dictionary of glue table DataFrames.
