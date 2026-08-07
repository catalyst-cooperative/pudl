# pudl.extract.excel

Load excel metadata CSV files form a python data package.

## Attributes

| [`logger`](#pudl.extract.excel.logger)   |    |
|------------------------------------------|----|

## Classes

| [`ExcelMetadata`](#pudl.extract.excel.ExcelMetadata)   | Load Excel metadata from Python package data.                |
|--------------------------------------------------------|--------------------------------------------------------------|
| [`ExcelExtractor`](#pudl.extract.excel.ExcelExtractor) | Logic for extracting `pd.DataFrame` from Excel spreadsheets. |

## Module Contents

### pudl.extract.excel.logger

### *class* pudl.extract.excel.ExcelMetadata(dataset_name: [str](https://docs.python.org/3/library/stdtypes.html#str))

Bases: [`pudl.extract.extractor.GenericMetadata`](../extractor/index.md#pudl.extract.extractor.GenericMetadata)

Load Excel metadata from Python package data.

Excel sheet files may contain many different tables. When we load those
into dataframes, metadata tells us how to do this. Metadata generally informs
us about the position of a given page in the file (which sheet and which row)
and it informs us how to translate excel column names into standardized
column names.

When metadata object is instantiated, it is given ${dataset} name and it
will attempt to load csv files from pudl.package_data.${dataset} package.

It expects the following kinds of files:

* skiprows.csv tells us how many initial rows should be skipped when loading
  data for given (partition, page).
* skipfooter.csv tells us how many bottom rows should be skipped when
  loading data for given partition (partition, page).
* page_map.csv tells us what is the excel sheet name that should be read
  when loading data for given (partition, page)
* column_map/${page}.csv currently informs us how to translate input column
  names to standardized pudl names for given (partition, input_col_name).
  Relevant page is encoded in the filename.

Optional file:

* page_part_map.csv tells us what secondary partition (e.g. “form”) needs to be
  specified to correctly identify the file housing the desired page. This is only
  required when a file can only be uniquely located using a combination of
  partitions (e.g. form and year).

#### \_skiprows *= None*

#### \_skipfooter *= None*

#### \_sheet_name *= None*

#### \_file_name *= None*

#### get_sheet_name(page, \*\*partition)

Return name of Excel sheet containing data for given partition and page.

#### get_skiprows(page, \*\*partition)

Return number of header rows to skip when loading a partition and page.

#### get_skipfooter(page, \*\*partition)

Return number of footer rows to skip when loading a partition and page.

#### get_file_name(page, \*\*partition)

Returns file name of given partition and page.

#### get_form(page) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Returns the form name for a given page.

### *class* pudl.extract.excel.ExcelExtractor(ds: [pudl.workspace.datastore.Datastore](../../workspace/datastore/index.md#pudl.workspace.datastore.Datastore))

Bases: [`pudl.extract.extractor.GenericExtractor`](../extractor/index.md#pudl.extract.extractor.GenericExtractor)

Logic for extracting `pd.DataFrame` from Excel spreadsheets.

This class implements the generic dataset agnostic logic to load data
from excel spreadsheet simply by using excel Metadata for given dataset.

It is expected that individual datasets will subclass this code and add
custom business logic by overriding necessary methods.

When implementing custom business logic, the following should be modified:

1. DATASET class attribute controls which excel metadata should be loaded.

2. BLACKLISTED_PAGES class attribute specifies which pages should not
be loaded from the underlying excel files even if the metadata is
available. This can be used for experimental/new code that should not be
run yet.

3. dtypes() should return dict with {column_name: pandas_datatype} if you
need to specify which datatypes should be uded upon loading.

4. If data cleanup is necessary, you can apply custom logic by overriding
one of the following functions (they all return the modified dataframe):

> * process_raw() is applied right after loading the excel DataFrame
>   from the disk.
> * process_renamed() is applied after input columns were renamed to
>   standardized pudl columns.
> * process_final_page() is applied when data from all available years
>   is merged into single DataFrame for a given page.

5. get_datapackage_resources() if partition is anything other than a year,
this method should be overwritten in the dataset-specific extractor.

#### METADATA *: [ExcelMetadata](#pudl.extract.excel.ExcelMetadata)* *= None*

Instance of metadata object to use with this extractor.

#### \_metadata *= None*

#### \_file_cache

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transforms raw dataframe and rename columns.

#### add_data_maturity(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add data_maturity column to indicate the maturity of partition data.

The three options enumerated here are `final`, `provisional` or
`monthly_update` (`incremental_ytd` is not currently implemented). We
determine if a df should be labeled as `provisional` by using the file names
because EIA seems to always include `Early_Release` in the file names. We
determine if a df should be labeled as `monthly_update` by checking if the
`self.dataset_name` is `eia860m`.

This method adds a column and thus adds `data_maturity` to
`self.cols_added`.

#### *static* get_dtypes(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Provide custom dtypes for given page and partition.

#### zipfile_resource_partitions(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Specify the partitions used for returning a zipfile from the datastore.

By default, this method appends any page to partition mapping in
`METADATA._page_part_map`. Most datasets do not have page to part
maps and just return the same partition that is passed in. If you have
dataset-specific partition mappings that are needed to return a zipfile from the
datastore, override this method to return the desired partitions.

#### load_source(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Produce the ExcelFile object for the given (partition, page).

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “coal_stocks”,
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  pd.DataFrame instance with the parsed Excel spreadsheet frame

#### source_filename(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Produce the xlsx document file name as it will appear in the archive.

* **Parameters:**
  * **page** – pudl name for the dataset contents, eg “boiler_generator_assn” or
    “coal_stocks”
  * **partition** – partition to load. Examples:
    {‘year’: 2009}
    {‘year_month’: ‘2020-08’}
* **Returns:**
  string name of the xlsx file
