# pudl.extract.epacems

Retrieve data from EPA CEMS hourly zipped CSVs.

Prior to August 2023, this data was retrieved from an FTP server. After August 2023,
this data is now retrieved from the CEMS API. The format of the files has changed from
monthly CSVs for each state to one CSV per state per year. The names of the columns
have also changed. Column name compatibility was determined by reading the CEMS API
documentation on column names.

Presently, this module is where the CEMS columns are renamed and dropped. Any columns in
the IGNORE_COLS dictionary are excluded from the final output. All of these columns are
calculable rates, measurement flags, or descriptors (like facility name) that can be
accessed by merging this data with the EIA860 plants entity table. We also remove the
FACILITY_ID field because it is internal to the EPA’s business accounting database.

Pre-transform, the plant_id_epa field is a close but not perfect indicator for
plant_id_eia. In the raw data it’s called Facility ID (ORISPL code) but that’s not
entirely accurate. The core_epa_\_assn_eia_epacamd crosswalk will show that the mapping between
Facility ID as it appears in CEMS and the plant_id_eia field used in EIA data.
Hence, we’ve called it plant_id_epa until it gets transformed into plant_id_eia
during the transform process with help from the crosswalk.

## Attributes

| [`logger`](#pudl.extract.epacems.logger)                   |                                                                               |
|------------------------------------------------------------|-------------------------------------------------------------------------------|
| [`API_RENAME_DICT`](#pudl.extract.epacems.API_RENAME_DICT) | A dictionary containing EPA CEMS column names (keys) and replacement names to |
| [`API_DTYPE_DICT`](#pudl.extract.epacems.API_DTYPE_DICT)   |                                                                               |

## Classes

| [`EpaCemsPartition`](#pudl.extract.epacems.EpaCemsPartition)   | Represents EpaCems partition identifying unique resource file.   |
|----------------------------------------------------------------|------------------------------------------------------------------|
| [`EpaCemsDatastore`](#pudl.extract.epacems.EpaCemsDatastore)   | Helper class to extract EpaCems resources from datastore.        |

## Functions

| [`extract_quarter`](#pudl.extract.epacems.extract_quarter)(→ polars.LazyFrame)   | Extract a single quarter of EPA CEMS data return it as a lazy polars DataFrame.   |
|----------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|

## Module Contents

### pudl.extract.epacems.logger

### pudl.extract.epacems.API_RENAME_DICT

A dictionary containing EPA CEMS column names (keys) and replacement names to
use when reading those columns into PUDL (values).

There are some duplicate rename values because the column names change year to year.

* **Type:**
  Dict

### pudl.extract.epacems.API_DTYPE_DICT

### *class* pudl.extract.epacems.EpaCemsPartition(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Represents EpaCems partition identifying unique resource file.

#### year_quarter *: Annotated[[str](https://docs.python.org/3/library/stdtypes.html#str), StringConstraints(strict=True, pattern='^(19|20)\\\\d{2}[q][1-4]$')]*

#### *property* year

Return the year associated with the year_quarter.

#### *property* quarter

Return the quarter associated with the year_quarter.

#### get_filters()

Returns filters for retrieving given partition resource from Datastore.

#### get_quarterly_file() → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Return the name of the CSV file that holds annual hourly data.

### *class* pudl.extract.epacems.EpaCemsDatastore(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore))

Helper class to extract EpaCems resources from datastore.

EpaCems resources are identified by a year and a quarter. Each of these zip files
contains one csv file. This class implements get_data_frame method that will
rename columns for a quarterly CSV file.

#### datastore

#### get_data_frame(partition: [EpaCemsPartition](#pudl.extract.epacems.EpaCemsPartition)) → polars.LazyFrame

Constructs dataframe from a zipfile for a given (year_quarter) partition.

### pudl.extract.epacems.extract_quarter(context, year_quarter: [str](https://docs.python.org/3/library/stdtypes.html#str)) → polars.LazyFrame

Extract a single quarter of EPA CEMS data return it as a lazy polars DataFrame.

If the requested quarter is not found in the datastore, an empty DataFrame with the
expected columns is returned.

* **Parameters:**
  * **context** – dagster keyword that provides access to resources and config.
  * **year_quarter** – Year quarter to process, formatted like ‘1995q1’.
