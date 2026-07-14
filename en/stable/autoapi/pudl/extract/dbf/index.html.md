# pudl.extract.dbf

Generalized DBF extractor for FERC data.

## Attributes

| [`logger`](#pudl.extract.dbf.logger)       |                                                          |
|--------------------------------------------|----------------------------------------------------------|
| [`DBF_TYPES`](#pudl.extract.dbf.DBF_TYPES) | A mapping of DBF field types to SQLAlchemy Column types. |

## Exceptions

| [`DbcFileMissingError`](#pudl.extract.dbf.DbcFileMissingError)   | This is raised when the DBC index file is missing.   |
|------------------------------------------------------------------|------------------------------------------------------|

## Classes

| [`DbfTableSchema`](#pudl.extract.dbf.DbfTableSchema)               | Simple data-wrapper for the fox-pro table schema.                                                                                                                      |
|--------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`FercDbfArchive`](#pudl.extract.dbf.FercDbfArchive)               | Represents API for accessing files within a single DBF archive.                                                                                                        |
| [`AbstractFercDbfReader`](#pudl.extract.dbf.AbstractFercDbfReader) | This is the interface definition for dealing with fox-pro datastores.                                                                                                  |
| [`FercFieldParser`](#pudl.extract.dbf.FercFieldParser)             | A custom DBF parser to deal with bad FERC data types.                                                                                                                  |
| [`PartitionedDataFrame`](#pudl.extract.dbf.PartitionedDataFrame)   | This class bundles [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) with partition information. |
| [`FercDbfReader`](#pudl.extract.dbf.FercDbfReader)                 | Wrapper to provide standardized access to FERC DBF databases.                                                                                                          |
| [`FercDbfExtractor`](#pudl.extract.dbf.FercDbfExtractor)           | Generalized class for loading data from foxpro databases into SQLAlchemy and parquet.                                                                                  |

## Functions

| [`convert_db_into_parquet`](#pudl.extract.dbf.convert_db_into_parquet)(db_path, parquet_dir)                                         | Convert the database into a directory of parquet files using duckdb.         |
|--------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| [`convert_and_validate_datapackage_sqlite_to_parquet`](#pudl.extract.dbf.convert_and_validate_datapackage_sqlite_to_parquet)(→ dict) | Convert the SQLite datapackage into one that points at Parquet files.        |
| [`write_datapackage`](#pudl.extract.dbf.write_datapackage)(datapackage, output_dir)                                                  | Write a datapackage to <output_dir>/datapackage.json.                        |
| [`add_key_constraints`](#pudl.extract.dbf.add_key_constraints)(→ sqlalchemy.MetaData)                                                | Adds primary and foreign key to tables present in meta.                      |
| [`deduplicate_by_year`](#pudl.extract.dbf.deduplicate_by_year)(→ pandas.DataFrame | None)                                            | Deduplicate records by year, keeping the most recent version of each record. |

## Module Contents

### pudl.extract.dbf.logger

### *exception* pudl.extract.dbf.DbcFileMissingError

Bases: [`Exception`](https://docs.python.org/3/library/exceptions.html#Exception)

This is raised when the DBC index file is missing.

### *class* pudl.extract.dbf.DbfTableSchema(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str))

Simple data-wrapper for the fox-pro table schema.

#### name

#### \_columns *= []*

#### \_column_types

#### \_short_name_map

#### add_column(col_name: [str](https://docs.python.org/3/library/stdtypes.html#str), col_type: [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type)[[sqlalchemy.types.TypeEngine](https://docs.sqlalchemy.org/en/21/core/type_api.html#sqlalchemy.types.TypeEngine)] | [sqlalchemy.types.TypeEngine](https://docs.sqlalchemy.org/en/21/core/type_api.html#sqlalchemy.types.TypeEngine), short_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None)

Adds a new column to this table schema.

#### get_columns() → [collections.abc.Iterator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterator)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type)[[sqlalchemy.types.TypeEngine](https://docs.sqlalchemy.org/en/21/core/type_api.html#sqlalchemy.types.TypeEngine)] | [sqlalchemy.types.TypeEngine](https://docs.sqlalchemy.org/en/21/core/type_api.html#sqlalchemy.types.TypeEngine)]]

Iterates over the (column_name, column_type) pairs.

#### get_column_names() → [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns set of long column names.

#### get_column_rename_map() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns dictionary that maps from short to long column names.

#### create_sa_table(sa_meta: sqlalchemy.MetaData) → sqlalchemy.Table

Creates SQLAlchemy table described by this instance.

* **Parameters:**
  **sa_meta** – new table will be written to this MetaData object.

### *class* pudl.extract.dbf.FercDbfArchive(zipfile: FercDbfArchive._\_init_\_.zipfile, dbc_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), table_file_map: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)], partition: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any], field_parser: [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type)[dbfread.FieldParser])

Represents API for accessing files within a single DBF archive.

Typically, archive contains data for a single year and single FERC form dataset
(e.g. FERC Form 1 or FERC Form 2).

#### zipfile

#### partition

#### root_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

#### dbc_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

#### \_table_file_map

#### field_parser

#### \_table_schemas *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]*

#### get_file(filename: [str](https://docs.python.org/3/library/stdtypes.html#str)) → IO[[bytes](https://docs.python.org/3/library/stdtypes.html#bytes)]

Opens the file within this archive.

#### get_db_schema() → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]

Returns dict with table names as keys, and list of column names as values.

#### get_table_dbf(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → dbfread.DBF

Opens the DBF for a given table.

#### get_table_schema(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [DbfTableSchema](#pudl.extract.dbf.DbfTableSchema)

Returns TableSchema for a given table and a given year.

#### load_table(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Returns dataframe that holds data for a table contained within this archive.

* **Parameters:**
  **table_name** – name of the table.

### *class* pudl.extract.dbf.AbstractFercDbfReader

Bases: `Protocol`

This is the interface definition for dealing with fox-pro datastores.

#### get_dataset() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns name of the dataset that this datastore provides access to.

#### get_table_names() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns list of all available table names.

#### get_archive(\*\*filters) → [FercDbfArchive](#pudl.extract.dbf.FercDbfArchive)

Returns single archive matching specific filters.

#### get_table_schema(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), year: [int](https://docs.python.org/3/library/functions.html#int)) → [DbfTableSchema](#pudl.extract.dbf.DbfTableSchema)

Returns schema for a given table and a given year.

#### load_table_dfs(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), partitions: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None)

Returns dataframe that contains data for a given table across given years.

### *class* pudl.extract.dbf.FercFieldParser(table, memofile=None)

Bases: `dbfread.FieldParser`

A custom DBF parser to deal with bad FERC data types.

#### parseN(field, data: [bytes](https://docs.python.org/3/library/stdtypes.html#bytes)) → [int](https://docs.python.org/3/library/functions.html#int) | [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None)

Augments the Numeric DBF parser to account for bad FERC data.

There are a small number of bad entries in the backlog of FERC Form 1
data. They take the form of leading/trailing zeroes or null characters
in supposedly numeric fields, and occasionally a naked ‘.’

Accordingly, this custom parser strips leading and trailing zeros and
null characters, and replaces a bare ‘.’ character with zero, allowing
all these fields to be cast to numeric values.

* **Parameters:**
  * **field** – The DBF field being parsed.
  * **data** – Binary data (bytes) read from the DBF file.

### pudl.extract.dbf.DBF_TYPES

A mapping of DBF field types to SQLAlchemy Column types.

This dictionary maps the strings which are used to denote field types in the DBF objects
to the corresponding generic SQLAlchemy Column types: These definitions come from a
combination of the dbfread example program dbf2sqlite and this DBF file format
documentation page:
[http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm](http://www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm)
: http: //www.dbase.com/KnowledgeBase/int/db7_file_fmt.htm Unmapped types left as ‘XXX’
which should result in an error if encountered.

* **Type:**
  Dict

### *class* pudl.extract.dbf.PartitionedDataFrame(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), partition: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any])

This class bundles [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) with partition information.

#### df

#### partition

### *class* pudl.extract.dbf.FercDbfReader(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), field_parser: [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type)[dbfread.FieldParser] = FercFieldParser)

Wrapper to provide standardized access to FERC DBF databases.

#### \_cache

#### datastore

#### dataset

#### field_parser

#### \_dbc_path

#### \_table_file_map

#### get_dataset() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Return the name of the dataset this datastore works with.

#### \_open_csv_resource(base_filename: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [csv.DictReader](https://docs.python.org/3/library/csv.html#csv.DictReader)

Open the given resource file as [`csv.DictReader`](https://docs.python.org/3/library/csv.html#csv.DictReader).

#### get_archive(year: [int](https://docs.python.org/3/library/functions.html#int), \*\*filters) → [FercDbfArchive](#pudl.extract.dbf.FercDbfArchive)

Returns single dbf archive matching given filters.

#### get_table_names() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns list of tables that this datastore provides access to.

#### *static* \_normalize(filters: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Casts partition values to lowercase strings.

#### valid_partition_filter(fl: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if a given filter fl is considered to be valid.

This can be used to eliminate partitions that are not suitable for processing,
e.g. for early years of FERC Form 2, databases marked with part=1 or part=2 are
not suitable.

#### load_table_dfs(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), partitions: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[PartitionedDataFrame](#pudl.extract.dbf.PartitionedDataFrame)]

Returns all data for a given table.

Merges data for a given table across all partitions.

* **Parameters:**
  * **table_name** – name of the table to load.
  * **partitions** – list of partition filters to use

### *class* pudl.extract.dbf.FercDbfExtractor(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), data_config: [pudl.settings.FercDbfToSqliteDataConfig](../../settings/index.md#pudl.settings.FercDbfToSqliteDataConfig), output_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Generalized class for loading data from foxpro databases into SQLAlchemy and parquet.

When subclassing from this generic extractor, one should implement dataset specific
logic in the following manner:

1. set DATABASE_NAME class attribute. This controls what filename is used for the output
sqlite database.
2. Implement get_dbf_reader() method to return the right kind of dataset specific
AbstractDbfReader instance.

Dataset specific logic and transformations can be injected by overriding:

1. finalize_schema() in order to modify sqlite schema. This is called just before
the schema is written into the sqlite database. This is good place for adding
primary and/or foreign key constraints to tables.
2. aggregate_table_frames() is responsible for concatenating individual data frames
(one par input partition) into single one. This is where deduplication can take place.
3. transform_table(table_name, df) will be invoked after dataframe is loaded from
the foxpro database and before it’s written to sqlite. This is good place for
table-specific preprocessing and/or cleanup.
4. postprocess() is called after data is written to sqlite. This can be used for
database level final cleanup and transformations (e.g. injecting missing
respondent_ids).

The extraction logic is invoked by calling execute() method of this class.

#### DATABASE_NAME *= None*

#### DATASET *= None*

#### data_config *: [pudl.settings.FercDbfToSqliteDataConfig](../../settings/index.md#pudl.settings.FercDbfToSqliteDataConfig)*

#### output_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

#### datastore *: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore)*

#### dbf_reader *: [AbstractFercDbfReader](#pudl.extract.dbf.AbstractFercDbfReader)*

#### sqlite_engine *: [sqlalchemy.engine.base.Engine](https://docs.sqlalchemy.org/en/21/core/connections.html#sqlalchemy.engine.Engine)* *= None*

#### sqlite_meta

#### get_data_config(ferc_to_sqlite_data_config: [pudl.settings.FercToSqliteDataConfig](../../settings/index.md#pudl.settings.FercToSqliteDataConfig)) → [pudl.settings.FercDbfToSqliteDataConfig](../../settings/index.md#pudl.settings.FercDbfToSqliteDataConfig)

Returns dataset relevant data configuration from ferc_to_sqlite_data_config.

#### get_dbf_reader(datastore: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore)) → [AbstractFercDbfReader](#pudl.extract.dbf.AbstractFercDbfReader)

Returns appropriate instance of AbstractFercDbfReader to access the data.

#### get_db_path() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns the path to the sqlite database.

#### get_db_uri() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns the connection string for the sqlite database.

#### \_clean_frictionless_types(type_: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Normalize types from SQLite to frictionless.

#### *property* datapackage_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Returns the path to the datapackage for this resource.

#### to_frictionless()

Create a frictionless DataPackage describing the DBF DB and write to a JSON file.

#### to_parquet()

Write parquet files for this resource.

#### *classmethod* get_dagster_op() → [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)

Returns dagstger op that runs this extractor.

#### execute()

Runs the extraction of the data from dbf to sqlite and parquet.

#### initialize_database()

Create sqlalchemy engine and metadata.

#### create_sqlite_tables()

Creates database schema based on the input tables.

#### transform_table(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), in_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms the content of a single table.

This method can be used to modify contents of the dataframe after it has
been loaded from fox pro database and before it’s written to sqlite database.

* **Parameters:**
  * **table_name** – name of the table that the dataframe is associated with
  * **in_df** – dataframe that holds all records.

#### *static* is_valid_partition(fl: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [bool](https://docs.python.org/3/library/functions.html#bool)

Returns True if the partition filter should be considered for processing.

#### aggregate_table_frames(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[PartitionedDataFrame](#pudl.extract.dbf.PartitionedDataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None)

Function to aggregate partitioned data frames into a single one.

By default, this simply concatenates the frames, but custom dataset specific
behaviors can be implemented.

#### load_table_data()

Loads all tables from fox pro database and writes them to sqlite.

#### finalize_schema(meta: sqlalchemy.MetaData) → sqlalchemy.MetaData

This method is called just before the schema is written to sqlite.

You can use this method to apply dataset specific alterations to the schema,
such as adding primary and foreign key constraints.

#### postprocess()

This method is called after all the data is loaded into sqlite.

### pudl.extract.dbf.convert_db_into_parquet(db_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), parquet_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Convert the database into a directory of parquet files using duckdb.

We do this using COPY. We tried using EXPORT DATABASE, but it unfortunately
sanitizes the table names, which removes the schedule numbers in the table
names so we can’t use it.

Maintainer note: this function was adapted from ferc-xbrl-extractor; changes
here should be considered for sync there and vice-versa.

### pudl.extract.dbf.convert_and_validate_datapackage_sqlite_to_parquet(datapackage_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Convert the SQLite datapackage into one that points at Parquet files.

* instead of `path` pointing at monolithic SQLite db, point at individual Parquet files instead
* update format/metadata fields

Maintainer note: this function was adapted from ferc-xbrl-extractor; changes
here should be considered for sync there and vice-versa.

### pudl.extract.dbf.write_datapackage(datapackage: [dict](https://docs.python.org/3/library/stdtypes.html#dict), output_dir: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path))

Write a datapackage to <output_dir>/datapackage.json.

output_dir must exist.

Maintainer note: this function was adapted from ferc-xbrl-extractor; changes
here should be considered for sync there and vice-versa.

### pudl.extract.dbf.add_key_constraints(meta: sqlalchemy.MetaData, pk_table: [str](https://docs.python.org/3/library/stdtypes.html#str), column: [str](https://docs.python.org/3/library/stdtypes.html#str), pk_column: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → sqlalchemy.MetaData

Adds primary and foreign key to tables present in meta.

* **Parameters:**
  * **meta** – constraints will be applied to this metadata instance
  * **pk_table** – name of the table that contains primary-key
  * **column** – foreign key column name. Tables that contain this column will
    have foreign-key constraint added.
  * **pk_column** – (optional) if specified, this is the primary key column name in
    the table. If not specified, it is assumed that this is the same as pk_column.

### pudl.extract.dbf.deduplicate_by_year(dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[PartitionedDataFrame](#pudl.extract.dbf.PartitionedDataFrame)], pk_column: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/library/constants.html#None)

Deduplicate records by year, keeping the most recent version of each record.

It will use pk_column as the primary key column. report_yr column is expected to
either be present, or it will be derived from partition[“year”].
