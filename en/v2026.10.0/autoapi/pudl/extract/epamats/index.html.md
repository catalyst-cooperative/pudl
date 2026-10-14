# pudl.extract.epamats

Retrieve data from EPA MATS hourly zipped CSVs.

The MATS data structure is similar to EPA CEMS but with different pollutants.
Each year’s data is stored in a zip file (e.g., epamats-2015.zip) containing
quarterly CSV files (e.g., epamats-2015q1.csv). The data tracks hourly
Hg (mercury), HCl (hydrochloric acid), and HF (hydrogen fluoride) emissions
from coal-fired power plants.

Similar to CEMS, the plant_id_epa field (Facility ID in raw data) needs to be
mapped to plant_id_eia during transformation using the core_epa_\_assn_eia_epacamd
crosswalk.

## Attributes

| [`logger`](#pudl.extract.epamats.logger)      |                                                              |
|--------------------------------------------------------------|--------------------------------------------------------------|
| [`RENAME_DICT`](#pudl.extract.epamats.RENAME_DICT) | Mapping from raw EPA MATS column names to PUDL column names. |
| [`DTYPE_DICT`](#pudl.extract.epamats.DTYPE_DICT)  | Data types for EPA MATS columns.                             |

## Classes

| [`EpaMatsPartition`](#pudl.extract.epamats.EpaMatsPartition)   | Represents a MATS partition identifying a unique quarterly resource file.   |
|---------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`EpaMatsDatastore`](#pudl.extract.epamats.EpaMatsDatastore)   | Helper class to extract MATS resources from datastore.                      |

## Functions

| [`raw_epamats__hourly_emissions`](#pudl.extract.epamats.raw_epamats__hourly_emissions)(→ pandas.DataFrame)   | Extract raw EPA MATS hourly emissions data and return as a pandas DataFrame.   |
|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|

## Module Contents

### pudl.extract.epamats.logger

### pudl.extract.epamats.RENAME_DICT

Mapping from raw EPA MATS column names to PUDL column names.

* **Type:**
  Dict

### pudl.extract.epamats.DTYPE_DICT

Data types for EPA MATS columns.

* **Type:**
  Dict

### *class* pudl.extract.epamats.EpaMatsPartition(/, \*\*data: Any)

Bases: [`pudl.extract.epacems.EpaCemsPartition`](../epacems/index.html.md#pudl.extract.epacems.EpaCemsPartition)

Represents a MATS partition identifying a unique quarterly resource file.

Inherits the year_quarter validation, year/quarter properties, and datastore
filters from `EpaCemsPartition`; only the CSV filename differs.

#### get_quarterly_file() → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Return the name of the CSV file within the zip that holds quarterly data.

### *class* pudl.extract.epamats.EpaMatsDatastore(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.html.md#pudl.workspace.datastore.Datastore))

Bases: [`pudl.extract.epacems.EpaCemsDatastore`](../epacems/index.html.md#pudl.extract.epacems.EpaCemsDatastore)

Helper class to extract MATS resources from datastore.

MATS resources are identified by a year and a quarter. Each year’s data is in
a zip file containing 4 quarterly CSV files. Inherits the zip-reading and
column-renaming logic from `EpaCemsDatastore`; only the dataset name,
column mapping, and dtypes differ.

#### dataset_name *: [str](https://docs.python.org/3/builtins/stdtypes.html#str)* *= 'epamats'*

Name of the dataset used to fetch zipfile resources from the datastore.

#### rename_dict *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]*

Mapping from raw column names to PUDL column names.

#### dtype_dict

Data types for the raw columns.

### pudl.extract.epamats.raw_epamats_\_hourly_emissions(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Extract raw EPA MATS hourly emissions data and return as a pandas DataFrame.
