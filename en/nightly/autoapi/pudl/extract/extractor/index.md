# pudl.extract.extractor

Generic functionality for extractors.

## Attributes

| [`StrInt`](#pudl.extract.extractor.StrInt)                  |                                                                          |
|--------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`PartitionSelection`](#pudl.extract.extractor.PartitionSelection)      |                                                                          |
| [`logger`](#pudl.extract.extractor.logger)                  |                                                                          |
| [`TIME_PARTITION_KEYS`](#pudl.extract.extractor.TIME_PARTITION_KEYS)     | Keys of the working partitions of a data source that are points in time. |
| [`dagster_dict_str_strint`](#pudl.extract.extractor.dagster_dict_str_strint) |                                                                          |

## Classes

| [`GenericMetadata`](#pudl.extract.extractor.GenericMetadata)   | Load generic metadata from files under src/pudl/package_data.   |
|--------------------------------------------------------------------|-----------------------------------------------------------------|
| [`GenericExtractor`](#pudl.extract.extractor.GenericExtractor)  | Generic extractor base class.                                   |

## Functions

| [`working_time_partitions`](#pudl.extract.extractor.working_time_partitions)(dataset_name) → list[str]                       | Return the sorted time partitions that a data source is processed for.               |
|--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| [`expand_partitions`](#pudl.extract.extractor.expand_partitions)(df, partitions) → pandas.DataFrame                    | Expand a change log of partitions into a record of every partition.                  |
| [`concat_pages`](#pudl.extract.extractor.concat_pages)(paged_dfs) → dict[str, pandas.DataFrame]                   | Concatenate similar pages of data from different years into single dataframes.       |
| [`_is_dict_str_strint`](#pudl.extract.extractor._is_dict_str_strint)(_context, x) → bool                                 |                                                                                      |
| [`partition_extractor_factory`](#pudl.extract.extractor.partition_extractor_factory)(extractor_cls, name) → dagster.OpDefinition | Construct a Dagster op that extracts one partition of data, given an extractor.      |
| [`partitions_from_data_config_factory`](#pudl.extract.extractor.partitions_from_data_config_factory)(name) → dagster.OpDefinition        | Construct a Dagster op to get target partitions from data config in Dagster context. |
| [`raw_df_factory`](#pudl.extract.extractor.raw_df_factory)(extractor_cls, name) → dagster.AssetsDefinition          | Return a dagster graph asset to extract raw DataFrames from CSV or Excel files.      |

## Module Contents

### pudl.extract.extractor.StrInt

### pudl.extract.extractor.PartitionSelection

### pudl.extract.extractor.logger

### pudl.extract.extractor.TIME_PARTITION_KEYS *= ('years', 'half_years', 'year_quarters', 'year_months')*

Keys of the working partitions of a data source that are points in time.

### pudl.extract.extractor.working_time_partitions(dataset_name: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Return the sorted time partitions that a data source is processed for.

These are the values of the time-like keys (see [`TIME_PARTITION_KEYS`](#pudl.extract.extractor.TIME_PARTITION_KEYS)) of the
`working_partitions` of the data source in [`pudl.metadata.sources.SOURCES`](../../metadata/sources/index.md#pudl.metadata.sources.SOURCES).
Datasets that are not data sources, such as the test datasets, have none.

* **Raises:**
  [**AssertionError**](https://docs.python.org/3/builtins/exceptions.html#AssertionError) – If the data source has working partitions of more than one
  temporal resolution (e.g. both `years` and `year_months`), since the
  partitions of different resolutions can’t be meaningfully ordered or
  carried forward together.

### pudl.extract.extractor.expand_partitions(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), partitions: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Expand a change log of partitions into a record of every partition.

The rows of `df` are indexed by partition, and only need to include the
partitions in which the description of the data differs from the one in the
partition before it. Data is forward filled for the entire row to fill the gaps
between mapped partitions.

Nothing is backfilled, so partitions before the first row are not described.
A frame with a row for every partition is returned unchanged. Partitions are
compared as strings, so their labels must sort chronologically.

* **Parameters:**
  * **df** – The rows of the partitions that have a different description than the
    partition before them, in ascending order of partition.
  * **partitions** – Every partition to describe, including those without a row. The
    partitions in the index of `df` are always described.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/builtins/exceptions.html#AssertionError) – If the partitions in the index are not unique and in ascending
  order, as the partition that a row is carried forward from is the closest
  one before.

### *class* pudl.extract.extractor.GenericMetadata(dataset_name: [str](https://docs.python.org/3/builtins/stdtypes.html#str))

Load generic metadata from files under src/pudl/package_data.

When metadata object is instantiated, it is given a ${dataset} name and it will
attempt to load csv files from src/pudl/package_data/${dataset}.

It expects the following kinds of files:

* column_map/${page}.csv currently informs us how to translate input column
  names to standardized pudl names for given (partition, input_col_name). Relevant
  page is encoded in the filename.

The files that describe the structure of a dataset, such as the column maps, only
need a row for the partitions in which that structure is different from the
partition before it, like a change log. When they are loaded, each row is carried
forward to the later working partitions of the data source that do not have a row
(see [`expand_partitions()`](#pudl.extract.extractor.expand_partitions)). Only working partitions are filled in, and only
from rows of working partitions: nothing extracts the others, so nothing would
check what they were filled in with.

`file_map.csv` is never filled in. It needs a row for every partition that has
been mapped, including any that are not working partitions, and is the
authoritative record of what has been mapped.

#### \_file_map *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [None](https://docs.python.org/3/builtins/constants.html#None)*

#### \_dataset_name

#### \_path

#### \_working_partitions

#### \_column_map

#### get_dataset_name() → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Returns the name of the dataset described by this metadata.

#### get_file_name(page, \*\*partition) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Returns file name of given partition and page.

* **Raises:**
  [**FileNotFoundError**](https://docs.python.org/3/builtins/exceptions.html#FileNotFoundError) – If the dataset has no `file_map.csv`.

#### \_load_csv(package_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), filename: [str](https://docs.python.org/3/builtins/stdtypes.html#str), dtype: [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type) | [None](https://docs.python.org/3/builtins/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load metadata from a filename that is found in a package directory.

* **Raises:**
  [**AssertionError**](https://docs.python.org/3/builtins/exceptions.html#AssertionError) – If any cell is empty.

#### \_load_change_log_csv(package_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), filename: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load a metadata file that only has rows for the partitions that changed.

The rows are carried forward to the working partitions that do not have one.

#### \_load_file_map() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load the map from partitions (rows) and pages (columns) to source file names.

Every cell must be a string: either the name of the source file, or `-1` if
there is no file for that page and partition. This file is never filled in.

#### \_load_column_maps(column_map_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)

Create a dictionary of all column mapping CSVs to use in get_column_map().

#### \_get_partition_selection(partition: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [PartitionSelection](#pudl.extract.extractor.PartitionSelection)]) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Grab the partition key.

#### get_all_pages() → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Returns list of all known pages.

#### get_all_columns(page) → [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Returns list of all pudl columns for a given page across all partitions.

#### get_column_map(page, \*\*partition) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Return dictionary of original columns to renamed columns for renaming in a given partition and page.

Columns that don’t exist in this partition/page will show up as pd.nan, so we need to filter those out.

### *class* pudl.extract.extractor.GenericExtractor(ds: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore), metadata: MetadataT)

Bases: [`abc.ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Generic extractor base class.

Extractors are generic over the type of the metadata that they use, so that
extractors for datasets with more specialized metadata, such as Excel files, can
use its methods with type checking.

`MetadataT` is the conventional name for a type variable (the `T` stands for
“Type”): a placeholder, limited here to [`GenericMetadata`](#pudl.extract.extractor.GenericMetadata), that each
subclass fills in, e.g. `GenericExtractor[ExcelMetadata]`.

#### BLACKLISTED_PAGES *= []*

List of supported pages that should not be extracted.

#### \_metadata

#### \_dataset_name

#### ds

#### cols_added *: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[str](https://docs.python.org/3/builtins/stdtypes.html#str)]* *= []*

#### *abstractmethod* source_filename(page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/builtins/stdtypes.html#str)

Produce the source file name as it will appear in the archive.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “coal_stocks”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  string name of the source file

#### *abstractmethod* load_source(page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce the source data for the given page and partition(s).

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg
    “boiler_generator_assn” or “coal_stocks”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  pd.DataFrame instance with the source data

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Takes any special steps for processing raw data and renaming columns.

#### process_renamed(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Takes any special steps for processing data after columns are renamed.

#### get_page_cols(page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), partition_selection: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [pandas.RangeIndex](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.RangeIndex.html#pandas.RangeIndex)

Get the columns for a particular page and partition key.

#### validate(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/builtins/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection))

Check if there are any missing or extra columns.

#### process_final_page(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Final processing stage applied to a page DataFrame.

#### combine(dfs: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], page: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Concatenate dataframes into one, take any special steps for processing final page.

#### extract(\*\*partitions: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Extracts dataframes.

Returns dict where keys are page names and values are
DataFrames containing data across given years.

* **Parameters:**
  **partitions** – keyword argument dictionary specifying how the source is partitioned and which
  particular partitions to extract. Examples:
  {‘years’: [2009, 2010]}
  {‘year_month’: ‘2020-08’}
  {‘form’: ‘gas_distribution’, ‘year’=’2020’}

### pudl.extract.extractor.concat_pages(paged_dfs: [list](https://docs.python.org/3/builtins/stdtypes.html#list)[[dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]]) → [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Concatenate similar pages of data from different years into single dataframes.

Transform a list of dictionaries of dataframes into a single dictionary of
dataframes, where each dataframe is the concatenation of dataframes with identical
keys from the input list.

For the relatively large EIA930 dataset this is a very memory-intensive operation,
so the op is tagged with a high memory-use tag. For all the other datasets which use
this op, the time spent concatenating pages is very brief, so this tag should not
impact the overall concurrency of the DAG much.

* **Parameters:**
  **paged_dfs** – A list of dictionaries whose keys are page names, and values are
  extracted DataFrames. Each element of the list corresponds to a single
  year of the dataset being extracted.
* **Returns:**
  A dictionary of DataFrames keyed by page name, where the DataFrame contains that
  page’s data from all extracted years concatenated together.

### pudl.extract.extractor.\_is_dict_str_strint(\_context: [dagster.TypeCheckContext](https://dagster.io/docs/api/dagster/execution/#dagster.TypeCheckContext), x: Any) → [bool](https://docs.python.org/3/builtins/functions.html#bool)

### pudl.extract.extractor.dagster_dict_str_strint

### pudl.extract.extractor.partition_extractor_factory(extractor_cls: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore)], [GenericExtractor](#pudl.extract.extractor.GenericExtractor)], name: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [dagster.OpDefinition](https://dagster.io/docs/api/dagster/ops/#dagster.OpDefinition)

Construct a Dagster op that extracts one partition of data, given an extractor.

* **Parameters:**
  * **extractor_cls** – Extractor class, or other callable that makes an extractor from
    a datastore, used to extract the data.
  * **name** – Name of an Excel based dataset (e.g. “eia860”).

### pudl.extract.extractor.partitions_from_data_config_factory(name: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [dagster.OpDefinition](https://dagster.io/docs/api/dagster/ops/#dagster.OpDefinition)

Construct a Dagster op to get target partitions from data config in Dagster context.

* **Parameters:**
  **name** – Name of an Excel based dataset (e.g. “eia860”).

### pudl.extract.extractor.raw_df_factory(extractor_cls: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)[[[pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore)], [GenericExtractor](#pudl.extract.extractor.GenericExtractor)], name: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [dagster.AssetsDefinition](https://dagster.io/docs/api/dagster/assets/#dagster.AssetsDefinition)

Return a dagster graph asset to extract raw DataFrames from CSV or Excel files.

* **Parameters:**
  * **extractor_cls** – The dataset-specific CSV or Excel extractor used to extract the
    data. Must correspond to the dataset identified by `name`.
  * **name** – Name of a CSV or Excel based dataset (e.g. “eia860” or “eia930”).
