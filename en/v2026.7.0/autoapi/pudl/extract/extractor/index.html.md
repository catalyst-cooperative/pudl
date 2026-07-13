# pudl.extract.extractor

Generic functionality for extractors.

## Attributes

| [`StrInt`](#pudl.extract.extractor.StrInt)                                   |    |
|------------------------------------------------------------------------------|----|
| [`PartitionSelection`](#pudl.extract.extractor.PartitionSelection)           |    |
| [`logger`](#pudl.extract.extractor.logger)                                   |    |
| [`dagster_dict_str_strint`](#pudl.extract.extractor.dagster_dict_str_strint) |    |

## Classes

| [`GenericMetadata`](#pudl.extract.extractor.GenericMetadata)   | Load generic metadata from Python package data.   |
|----------------------------------------------------------------|---------------------------------------------------|
| [`GenericExtractor`](#pudl.extract.extractor.GenericExtractor) | Generic extractor base class.                     |

## Functions

| [`concat_pages`](#pudl.extract.extractor.concat_pages)(→ dict[str, pandas.DataFrame])                                        | Concatenate similar pages of data from different years into single dataframes.       |
|------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------|
| [`_is_dict_str_strint`](#pudl.extract.extractor._is_dict_str_strint)(→ bool)                                                 |                                                                                      |
| [`partition_extractor_factory`](#pudl.extract.extractor.partition_extractor_factory)(→ dagster.OpDefinition)                 | Construct a Dagster op that extracts one partition of data, given an extractor.      |
| [`partitions_from_data_config_factory`](#pudl.extract.extractor.partitions_from_data_config_factory)(→ dagster.OpDefinition) | Construct a Dagster op to get target partitions from data config in Dagster context. |
| [`raw_df_factory`](#pudl.extract.extractor.raw_df_factory)(→ dagster.AssetsDefinition)                                       | Return a dagster graph asset to extract raw DataFrames from CSV or Excel files.      |

## Module Contents

### pudl.extract.extractor.StrInt

### pudl.extract.extractor.PartitionSelection

### pudl.extract.extractor.logger

### *class* pudl.extract.extractor.GenericMetadata(dataset_name: [str](https://docs.python.org/3/library/stdtypes.html#str))

Load generic metadata from Python package data.

When metadata object is instantiated, it is given ${dataset} name and it
will attempt to load csv files from pudl.package_data.${dataset} package.

It expects the following kinds of files:

* column_map/${page}.csv currently informs us how to translate input column
  names to standardized pudl names for given (partition, input_col_name). Relevant
  page is encoded in the filename.

#### \_dataset_name

#### \_pkg

#### \_column_map

#### get_dataset_name() → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns the name of the dataset described by this metadata.

#### \_load_csv(package: [str](https://docs.python.org/3/library/stdtypes.html#str), filename: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Load metadata from a filename that is found in a package.

#### \_load_column_maps(column_map_pkg: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Create a dictionary of all column mapping CSVs to use in get_column_map().

#### \_get_partition_selection(partition: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [PartitionSelection](#pudl.extract.extractor.PartitionSelection)]) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Grab the partition key.

#### get_all_pages() → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns list of all known pages.

#### get_all_columns(page) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Returns list of all pudl columns for a given page across all partitions.

#### get_column_map(page, \*\*partition) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]

Return dictionary of original columns to renamed columns for renaming in a given partition and page.

Columns that don’t exist in this partition/page will show up as pd.nan, so we need to filter those out.

### *class* pudl.extract.extractor.GenericExtractor(ds: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore))

Bases: [`abc.ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Generic extractor base class.

#### METADATA *: [GenericMetadata](#pudl.extract.extractor.GenericMetadata)* *= None*

Instance of metadata object to use with this extractor.

#### BLACKLISTED_PAGES *= []*

List of supported pages that should not be extracted.

#### \_metadata *= None*

#### \_dataset_name

#### ds

#### cols_added *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= []*

#### *abstractmethod* source_filename(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Produce the source file name as it will appear in the archive.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “coal_stocks”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  string name of the source file

#### *abstractmethod* load_source(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce the source data for the given page and partition(s).

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg
    “boiler_generator_assn” or “coal_stocks”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  pd.DataFrame instance with the source data

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Takes any special steps for processing raw data and renaming columns.

#### process_renamed(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Takes any special steps for processing data after columns are renamed.

#### get_page_cols(page: [str](https://docs.python.org/3/library/stdtypes.html#str), partition_selection: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.RangeIndex](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.RangeIndex.html#pandas.RangeIndex)

Get the columns for a particular page and partition key.

#### validate(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [PartitionSelection](#pudl.extract.extractor.PartitionSelection))

Check if there are any missing or extra columns.

#### process_final_page(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Final processing stage applied to a page DataFrame.

#### combine(dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)], page: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Concatenate dataframes into one, take any special steps for processing final page.

#### extract(\*\*partitions: [PartitionSelection](#pudl.extract.extractor.PartitionSelection)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

Extracts dataframes.

Returns dict where keys are page names and values are
DataFrames containing data across given years.

* **Parameters:**
  **partitions** – keyword argument dictionary specifying how the source is partitioned and which
  particular partitions to extract. Examples:
  {‘years’: [2009, 2010]}
  {‘year_month’: ‘2020-08’}
  {‘form’: ‘gas_distribution’, ‘year’=’2020’}

### pudl.extract.extractor.concat_pages(paged_dfs: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]

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

### pudl.extract.extractor.\_is_dict_str_strint(\_context: [dagster.TypeCheckContext](https://docs.dagster.io/api/dagster/execution/#dagster.TypeCheckContext), x: Any) → [bool](https://docs.python.org/3/library/functions.html#bool)

### pudl.extract.extractor.dagster_dict_str_strint

### pudl.extract.extractor.partition_extractor_factory(extractor_cls: [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type)[[GenericExtractor](#pudl.extract.extractor.GenericExtractor)], name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dagster.OpDefinition](https://docs.dagster.io/api/dagster/ops/#dagster.OpDefinition)

Construct a Dagster op that extracts one partition of data, given an extractor.

* **Parameters:**
  * **extractor_cls** – Class of type `Extractor` used to extract the data.
  * **name** – Name of an Excel based dataset (e.g. “eia860”).

### pudl.extract.extractor.partitions_from_data_config_factory(name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dagster.OpDefinition](https://docs.dagster.io/api/dagster/ops/#dagster.OpDefinition)

Construct a Dagster op to get target partitions from data config in Dagster context.

* **Parameters:**
  **name** – Name of an Excel based dataset (e.g. “eia860”).

### pudl.extract.extractor.raw_df_factory(extractor_cls: [type](../../metadata/classes/index.md#pudl.metadata.classes.Field.type)[[GenericExtractor](#pudl.extract.extractor.GenericExtractor)], name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)

Return a dagster graph asset to extract raw DataFrames from CSV or Excel files.

* **Parameters:**
  * **extractor_cls** – The dataset-specific CSV or Excel extractor used to extract the
    data. Must correspond to the dataset identified by `name`.
  * **name** – Name of a CSV or Excel based dataset (e.g. “eia860” or “eia930”).
