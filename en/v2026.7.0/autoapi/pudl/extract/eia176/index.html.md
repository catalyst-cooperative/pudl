# pudl.extract.eia176

Extract EIA Form 176 data from CSVs.

## Attributes

| [`raw_eia176__all_dfs`](#pudl.extract.eia176.raw_eia176__all_dfs)   |    |
|---------------------------------------------------------------------|----|
| [`raw_eia176_assets`](#pudl.extract.eia176.raw_eia176_assets)       |    |

## Classes

| [`Extractor`](#pudl.extract.eia176.Extractor)   | Extractor for EIA form 176.   |
|-------------------------------------------------|-------------------------------|

## Functions

| [`raw_eia176_asset_factory`](#pudl.extract.eia176.raw_eia176_asset_factory)(in_page[, out_page])   | Create raw EIA 176 asset for a specific page.   |
|----------------------------------------------------------------------------------------------------|-------------------------------------------------|

## Module Contents

### *class* pudl.extract.eia176.Extractor(\*args, \*\*kwargs)

Bases: [`pudl.extract.csv.CsvExtractor`](../csv/index.md#pudl.extract.csv.CsvExtractor)

Extractor for EIA form 176.

#### METADATA

Instance of metadata object to use with this extractor.

#### source_filename(page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Produce the source file name as it will appear in the ZIP archive.

For this archive in particular, we control the naming of the CSV files because
they are created by scraping EIA’s natural gas query viewer interface. Rather
than creating a file_map.csv like the other EIA extractors, we handle the one
the missing company_list file in certain years here, returning “-1” since that
mirrors the behavior of the other extractors that do rely on file_map.csv.

* **Parameters:**
  * **page** – the name of the “page” within the dataset to extract. For EIA-176
    this is the descriptive portion of the name of one of the CSV files in
    the ZIP archive, e.g. “natural_gas_deliveries”.
  * **partition** – a dictionary uniquely identifying a partition to extract, e.g.
    {“year”: “2019”, “format”: “by_report”}
* **Returns:**
  Full name of the CSV file within the ZIP archive as a string.

#### process_raw(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), page: [str](https://docs.python.org/3/library/stdtypes.html#str), \*\*partition: [pudl.extract.extractor.PartitionSelection](../extractor/index.md#pudl.extract.extractor.PartitionSelection)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Append report year to df to distinguish data from other years.

### pudl.extract.eia176.raw_eia176_\_all_dfs

### pudl.extract.eia176.raw_eia176_asset_factory(in_page: [str](https://docs.python.org/3/library/stdtypes.html#str), out_page: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None)

Create raw EIA 176 asset for a specific page.

### pudl.extract.eia176.raw_eia176_assets
