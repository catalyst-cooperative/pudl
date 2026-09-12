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

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Represents a MATS partition identifying a unique quarterly resource file.

#### year_quarter *: Annotated[[str](https://docs.python.org/3/library/stdtypes.html#str), StringConstraints(strict=True, pattern='^(19|20)\\\\d{2}[q][1-4]$')]*

#### *property* year

Return the year associated with the year_quarter.

#### *property* quarter

Return the quarter associated with the year_quarter.

#### get_filters()

Returns filters for retrieving given partition resource from Datastore.

#### get_quarterly_file() → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Return the name of the CSV file within the zip that holds quarterly data.

### *class* pudl.extract.epamats.EpaMatsDatastore(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.html.md#pudl.workspace.datastore.Datastore))

Helper class to extract MATS resources from datastore.

MATS resources are identified by a year and a quarter. Each year’s data is in
a zip file containing 4 quarterly CSV files. This class implements get_data_frame
method that will rename columns for a quarterly CSV file.

#### datastore

#### get_data_frame(partition: [EpaMatsPartition](#pudl.extract.epamats.EpaMatsPartition)) → polars.LazyFrame

Constructs dataframe from a zipfile for a given (year_quarter) partition.

### pudl.extract.epamats.raw_epamats_\_hourly_emissions(context) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Extract raw EPA MATS hourly emissions data and return as a pandas DataFrame.
