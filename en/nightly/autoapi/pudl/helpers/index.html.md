# pudl.helpers

General utility functions that are used in a variety of contexts.

The functions in this module are used in various stages of the ETL and post-etl
processes. They are usually not dataset specific, but not always. If a function is
designed to be used as a general purpose tool, applicable in multiple scenarios, it
should probably live here. There are lost of transform type functions in here that help
with cleaning and restructuring dataframes.

## Attributes

| [`sum_na`](#pudl.helpers.sum_na)   | A sum function that returns NA if the Series includes any NA values.   |
|------------------------------------|------------------------------------------------------------------------|
| [`logger`](#pudl.helpers.logger)   |                                                                        |

## Classes

| [`TableDiff`](#pudl.helpers.TableDiff)     | Represent a diff between two versions of the same table.   |
|--------------------------------------------|------------------------------------------------------------|
| [`ParquetData`](#pudl.helpers.ParquetData) | Wrap table data offloaded as parquet file(s) on disk.      |

## Functions

| [`label_map`](#pudl.helpers.label_map)(→ collections.defaultdict[str, ...)                                         | Build a mapping dictionary from two columns of a labeling / coding dataframe.                                                                                                                                                                                     |
|--------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`multi_index_stack`](#pudl.helpers.multi_index_stack)(→ pandas.DataFrame)                                         | Stack multiple data columns - create categorical columns and data columns.                                                                                                                                                                                        |
| [`find_new_ferc1_strings`](#pudl.helpers.find_new_ferc1_strings)(→ set[str])                                       | Identify as-of-yet uncategorized freeform strings in FERC Form 1.                                                                                                                                                                                                 |
| [`find_foreign_key_errors`](#pudl.helpers.find_foreign_key_errors)(→ list[dict[str, Any]])                         | Report foreign key violations from a dictionary of dataframes.                                                                                                                                                                                                    |
| [`download_zip_url`](#pudl.helpers.download_zip_url)(→ None)                                                       | Download and save a Zipfile locally.                                                                                                                                                                                                                              |
| [`add_fips_ids`](#pudl.helpers.add_fips_ids)(→ pandas.DataFrame)                                                   | Add State and County FIPS IDs to a dataframe.                                                                                                                                                                                                                     |
| [`add_state_id_fips`](#pudl.helpers.add_state_id_fips)(→ pandas.DataFrame)                                         | Add the State FIPS codes.                                                                                                                                                                                                                                         |
| [`add_county_fips_id`](#pudl.helpers.add_county_fips_id)(→ pandas.DataFrame)                                       | Add the County FIPS codes to a table with State FIPS codes.                                                                                                                                                                                                       |
| [`clean_eia_counties`](#pudl.helpers.clean_eia_counties)(→ pandas.DataFrame)                                       | Replace non-standard county names with county names from US Census.                                                                                                                                                                                               |
| [`oob_to_nan`](#pudl.helpers.oob_to_nan)(→ pandas.DataFrame)                                                       | Set non-numeric values and those outside of a given range to NaN.                                                                                                                                                                                                 |
| [`oob_to_nan_with_dependent_cols`](#pudl.helpers.oob_to_nan_with_dependent_cols)(→ pandas.DataFrame)               | Call [`oob_to_nan()`](#pudl.helpers.oob_to_nan) and additionally nullify any derived columns.                                                                                                                                                                     |
| [`prep_dir`](#pudl.helpers.prep_dir)(→ pathlib.Path)                                                               | Create (or delete and recreate) a directory.                                                                                                                                                                                                                      |
| [`is_doi`](#pudl.helpers.is_doi)(→ bool)                                                                           | Determine if a string is a valid digital object identifier (DOI).                                                                                                                                                                                                 |
| [`convert_col_to_datetime`](#pudl.helpers.convert_col_to_datetime)(→ pandas.DataFrame)                             | Convert a non-datetime column in a dataframe to a datetime64[s].                                                                                                                                                                                                  |
| [`full_timeseries_date_merge`](#pudl.helpers.full_timeseries_date_merge)(→ pandas.DataFrame)                       | Merge dataframes with different date frequencies and expand to a full timeseries.                                                                                                                                                                                 |
| [`_add_suffix_to_date_on`](#pudl.helpers._add_suffix_to_date_on)(date_on)                                          | Check date_on list is valid and add \_temp_for_merge suffix.                                                                                                                                                                                                      |
| [`date_merge`](#pudl.helpers.date_merge)(→ pandas.DataFrame)                                                       | Merge two dataframes that have different report date frequencies.                                                                                                                                                                                                 |
| [`expand_timeseries`](#pudl.helpers.expand_timeseries)(→ pandas.DataFrame)                                         | Expand a dataframe to a include a full time series at a given frequency.                                                                                                                                                                                          |
| [`organize_cols`](#pudl.helpers.organize_cols)(→ pandas.DataFrame)                                                 | Organize columns into key ID & name fields & alphabetical data columns.                                                                                                                                                                                           |
| [`simplify_strings`](#pudl.helpers.simplify_strings)(→ pandas.DataFrame)                                           | Simplify the strings contained in a set of dataframe columns.                                                                                                                                                                                                     |
| [`cleanstrings_series`](#pudl.helpers.cleanstrings_series)(→ pandas.Series)                                        | Clean up the strings in a single column/Series.                                                                                                                                                                                                                   |
| [`cleanstrings`](#pudl.helpers.cleanstrings)(→ pandas.DataFrame)                                                   | Consolidate freeform strings in several dataframe columns.                                                                                                                                                                                                        |
| [`fix_int_na`](#pudl.helpers.fix_int_na)(→ pandas.DataFrame)                                                       | Convert NA containing integer columns from float to string.                                                                                                                                                                                                       |
| [`month_year_to_date`](#pudl.helpers.month_year_to_date)(→ pandas.DataFrame)                                       | Convert all pairs of year/month fields in a dataframe into Date fields.                                                                                                                                                                                           |
| [`convert_to_date`](#pudl.helpers.convert_to_date)(→ pandas.DataFrame)                                             | Convert specified year, month or day columns into a datetime object.                                                                                                                                                                                              |
| [`remove_leading_zeros_from_numeric_strings`](#pudl.helpers.remove_leading_zeros_from_numeric_strings)(...)        | Remove leading zeros frame column values that are numeric strings.                                                                                                                                                                                                |
| [`standardize_na_values`](#pudl.helpers.standardize_na_values)(→ pandas.DataFrame)                                 | Replace common apparently NA values in numerical columns with the floating point np.nan.                                                                                                                                                                          |
| [`simplify_columns`](#pudl.helpers.simplify_columns)(→ pandas.DataFrame)                                           | Simplify column labels for use as snake_case database fields.                                                                                                                                                                                                     |
| [`drop_tables`](#pudl.helpers.drop_tables)(→ None)                                                                 | Drops all tables from a SQLite database.                                                                                                                                                                                                                          |
| [`merge_dicts`](#pudl.helpers.merge_dicts)(→ dict[Any, Any])                                                       | Merge multiple dictionaries together.                                                                                                                                                                                                                             |
| [`convert_cols_dtypes`](#pudl.helpers.convert_cols_dtypes)(→ pandas.DataFrame)                                     | Convert a PUDL dataframe's columns to the correct data type.                                                                                                                                                                                                      |
| [`generate_rolling_avg`](#pudl.helpers.generate_rolling_avg)(→ pandas.DataFrame)                                   | Generate a rolling average.                                                                                                                                                                                                                                       |
| [`fillna_w_rolling_avg`](#pudl.helpers.fillna_w_rolling_avg)(→ pandas.DataFrame)                                   | Fill NA values with a rolling average.                                                                                                                                                                                                                            |
| [`groupby_agg_label_unique_source_or_mixed`](#pudl.helpers.groupby_agg_label_unique_source_or_mixed)(→ str | None) | Get either the unique source in a group or return mixed.                                                                                                                                                                                                          |
| [`count_records`](#pudl.helpers.count_records)(→ pandas.DataFrame)                                                 | Count the number of unique records in group in a dataframe.                                                                                                                                                                                                       |
| [`cleanstrings_snake`](#pudl.helpers.cleanstrings_snake)(→ pandas.DataFrame)                                       | Clean the strings in a columns in a dataframe with snake case.                                                                                                                                                                                                    |
| [`zero_pad_numeric_string`](#pudl.helpers.zero_pad_numeric_string)(→ pandas.Series)                                | Clean up fixed-width leading zero padded numeric (e.g. ZIP, FIPS) codes.                                                                                                                                                                                          |
| [`iterate_multivalue_dict`](#pudl.helpers.iterate_multivalue_dict)(\*\*kwargs)                                     | Make dicts from dict with main dict key and one value of main dict.                                                                                                                                                                                               |
| [`dedupe_on_category`](#pudl.helpers.dedupe_on_category)(→ pandas.DataFrame)                                       | Deduplicate a df using a sorted category to retain preferred values.                                                                                                                                                                                              |
| [`dedupe_and_drop_nas`](#pudl.helpers.dedupe_and_drop_nas)(→ pandas.DataFrame)                                     | Deduplicate a df by comparing primary key columns and dropping null rows.                                                                                                                                                                                         |
| [`drop_records_with_null_in_column`](#pudl.helpers.drop_records_with_null_in_column)(→ pandas.DataFrame)           | Drop a prescribed number of records with null values in a column.                                                                                                                                                                                                 |
| [`standardize_percentages_ratio`](#pudl.helpers.standardize_percentages_ratio)(→ pandas.DataFrame)                 | Standardize year-to-year changes in mixed percentage/ratio reporting in a column.                                                                                                                                                                                 |
| [`calc_capacity_factor`](#pudl.helpers.calc_capacity_factor)(→ pandas.DataFrame)                                   | Calculate capacity factor.                                                                                                                                                                                                                                        |
| [`weighted_average`](#pudl.helpers.weighted_average)(→ pandas.DataFrame)                                           | Generate a weighted average.                                                                                                                                                                                                                                      |
| [`sum_and_weighted_average_agg`](#pudl.helpers.sum_and_weighted_average_agg)(→ pandas.DataFrame)                   | Aggregate dataframe by summing and using weighted averages.                                                                                                                                                                                                       |
| [`get_eia_ferc_acct_map`](#pudl.helpers.get_eia_ferc_acct_map)(→ pandas.DataFrame)                                 | Get map of EIA technology_description/pm codes <> ferc accounts.                                                                                                                                                                                                  |
| [`dedupe_n_flatten_list_of_lists`](#pudl.helpers.dedupe_n_flatten_list_of_lists)(→ list)                           | Flatten a list of lists and remove duplicates.                                                                                                                                                                                                                    |
| [`flatten_list`](#pudl.helpers.flatten_list)(→ collections.abc.Generator)                                          | Flatten an irregular (arbitrarily nested) list of lists (or sets).                                                                                                                                                                                                |
| [`convert_df_to_excel_file`](#pudl.helpers.convert_df_to_excel_file)(→ pandas.ExcelFile)                           | Convert a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) into a [`pandas.ExcelFile`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.ExcelFile.html#pandas.ExcelFile). |
| [`get_asset_keys`](#pudl.helpers.get_asset_keys)(→ set[dagster.AssetKey])                                          | Get a set of asset keys from a list of asset definitions.                                                                                                                                                                                                         |
| [`get_asset_group_keys`](#pudl.helpers.get_asset_group_keys)(→ list[str])                                          | Get a list of asset names in a given asset group.                                                                                                                                                                                                                 |
| [`convert_col_to_bool`](#pudl.helpers.convert_col_to_bool)(→ pandas.DataFrame)                                     | Turn a column into a boolean while preserving NA values.                                                                                                                                                                                                          |
| [`fix_boolean_columns`](#pudl.helpers.fix_boolean_columns)(→ pandas.DataFrame)                                     | Fix standard issues with boolean columns.                                                                                                                                                                                                                         |
| [`scale_by_ownership`](#pudl.helpers.scale_by_ownership)(→ pandas.DataFrame)                                       | Generate proportional data by ownership %s.                                                                                                                                                                                                                       |
| [`assert_cols_areclose`](#pudl.helpers.assert_cols_areclose)(df, a_cols, b_cols, ...)                              | Check if two column sets of a dataframe are close to each other.                                                                                                                                                                                                  |
| [`diff_wide_tables`](#pudl.helpers.diff_wide_tables)(→ TableDiff)                                                  | Diff values across multiple iterations of the same wide table.                                                                                                                                                                                                    |
| [`retry`](#pudl.helpers.retry)(func, retry_on[, max_retries, base_delay_sec])                                      | Retry a function with a short sleep between each try.                                                                                                                                                                                                             |
| [`get_parquet_table_polars`](#pudl.helpers.get_parquet_table_polars)(→ polars.LazyFrame)                           | Read a table from a parquet file and return as a polars LazyFrame.                                                                                                                                                                                                |
| [`get_parquet_table`](#pudl.helpers.get_parquet_table)(...)                                                        | Read a table from Parquet files with optional column selection and filtering.                                                                                                                                                                                     |
| [`standardize_phone_column`](#pudl.helpers.standardize_phone_column)(→ pandas.DataFrame)                           | Standardize phone numbers in the specified columns of the DataFrame.                                                                                                                                                                                              |
| [`persist_table_as_parquet`](#pudl.helpers.persist_table_as_parquet)(→ ParquetData)                                | Write data from DataFrame or LazyFrame to disk as a parquet file.                                                                                                                                                                                                 |
| [`lf_from_parquet`](#pudl.helpers.lf_from_parquet)(→ polars.LazyFrame)                                             | Scan parquet file(s) from disk and return Polars LazyFrame.                                                                                                                                                                                                       |
| [`df_from_parquet`](#pudl.helpers.df_from_parquet)(→ pandas.DataFrame)                                             | Read data from a set of parquet files and return a pandas DataFrame.                                                                                                                                                                                              |
| [`duckdb_relation_from_parquet`](#pudl.helpers.duckdb_relation_from_parquet)(...)                                  | Create a duckdb relation to read from parquet files.                                                                                                                                                                                                              |
| [`duckdb_extract_zipped_csv`](#pudl.helpers.duckdb_extract_zipped_csv)() → tuple[str, ParquetData])                | Extract data from zipped CSV page(s) in a data archive.                                                                                                                                                                                                           |
| [`normalize_year_fragments`](#pudl.helpers.normalize_year_fragments)(→ pandas.Series)                              | Normalize year fragments into 4-digit years using a rolling-century rule.                                                                                                                                                                                         |
| [`make_changelog`](#pudl.helpers.make_changelog)(df_all, idx)                                                      | Make a changelog table with unique instances of values over start report and max report date.                                                                                                                                                                     |
| [`parse_address`](#pudl.helpers.parse_address)(addr)                                                               | Parse a U.S. address into components.                                                                                                                                                                                                                             |
| [`listify`](#pudl.helpers.listify)(→ list[Any])                                                                    | Listify an input that is sometimes a list and sometimes not.                                                                                                                                                                                                      |
| [`env_var_is_true`](#pudl.helpers.env_var_is_true)(→ bool)                                                         | Check that environment variable is a 'truthy' value.                                                                                                                                                                                                              |

## Module Contents

### pudl.helpers.sum_na

A sum function that returns NA if the Series includes any NA values.

In many of our aggregations we need to override the default behavior of treating NA
values as if they were zero. E.g. when calculating the heat rates of generation units,
if there are some months where fuel consumption is reported as NA, but electricity
generation is reported normally, then the fuel consumption for the year needs to be NA,
otherwise we’ll get unrealistic heat rates.

### pudl.helpers.logger

### pudl.helpers.label_map(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), from_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'code', to_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'label', null_value: [str](https://docs.python.org/3/library/stdtypes.html#str) | pandas._libs.missing.NAType = pd.NA) → [collections.defaultdict](https://docs.python.org/3/library/collections.html#collections.defaultdict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | pandas._libs.missing.NAType]

Build a mapping dictionary from two columns of a labeling / coding dataframe.

These dataframes document the meanings of the codes that show up in much of the
originally reported data. They’re defined in [`pudl.metadata.codes`](../metadata/codes/index.md#module-pudl.metadata.codes).  This
function is mostly used to build maps that can translate the hard to understand
short codes into longer human-readable codes.

* **Parameters:**
  * **df** – The coding / labeling dataframe. Must contain columns `from_col`
    and `to_col`.
  * **from_col** – Label of column containing the existing codes to be replaced.
  * **to_col** – Label of column containing the new codes to be swapped in.
  * **null_value** – Default (Null) value to map to when a value which doesn’t
    appear in `from_col` is encountered.
* **Returns:**
  A mapping dictionary suitable for use with [`pandas.Series.map()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.map.html#pandas.Series.map).

### pudl.helpers.multi_index_stack(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx_ish: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], pattern: [str](https://docs.python.org/3/library/stdtypes.html#str), data_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [str](https://docs.python.org/3/library/stdtypes.html#str), match_names: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], unstack_level: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], drop_zero_rows: [bool](https://docs.python.org/3/library/functions.html#bool) = False, expected_dropped_cols: [int](https://docs.python.org/3/library/functions.html#int) = 0) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Stack multiple data columns - create categorical columns and data columns.

Many tables are reported in a wide format, with several columns reporting
the same type of value, but within different categories. E.g. electricity sales by
customer class, with each customer class in a separate column, and separate sets of
customer class columns for the dollar value of sales, and the MWh of electricity
sold.

This function takes those groups of columns and stacks each of them into a single
data column creating another categorical column describing the class to which each
record pertains.

* **Parameters:**
  * **df** – table to edit.
  * **idx_ish** – columns in df you want to set as index of stacked table.
  * **pattern** – a regex pattern of the df’s column names you want to stack. This
    pattern should have match groups that correspond to the levels of the
    multi-index that will be created. One of these match groups must include
    the string values of the `data_cols`. For example, the
    `raw_rus7__power_requirements` table contains a set of columns which
    have sales_kwh and revenue pertaining to many different customer
    classifications. The raw columns always have many different types of
    customer_classifications at the beginning and either the sales or revenue
    at the end. So the pattern for this table is:
    `rf"^(.+)_(sales_kwh|revenue)$"`
  * **data_cols** – names of data columns - these are strings within the df’s original
    column names that you will leave unstacked. The resulting dataframe will
    include these columns. Using the same `raw_rus7__power_requirements`
    example, the data columns would be: `["sales_kwh", "revenue"]`
  * **match_names** – the assigned names of each of the match groups in the regex
    pattern - in the order they appear in the pattern. The match group’s name
    we won’t use is the group containing the data_cols values - this can be
    named anything but for clarity name this ‘data_cols’. Using the same
    `raw_rus7__power_requirements` example, the `match_names` would be
    `["customer_classification", "data_cols"]` because the customer
    classification is the first match in the `pattern` and the data columns
    are the second match in the pattern.
  * **unstack_level** – list of match_names to unstack. These are the names of the
    matches that get unstacked - these end up as columns in the resulting
    table. Presumably this will be all of the match_names except ‘data_cols’.
  * **drop_zero_rows** – if True, drop rows where all data_cols are 0. Function
    already drops rows where data_cols are all NaN.
  * **expected_dropped_cols** – The number of cols we expect to be dropping during the
    stack. Defaults to zero.

### pudl.helpers.find_new_ferc1_strings(table: [str](https://docs.python.org/3/library/stdtypes.html#str), field: [str](https://docs.python.org/3/library/stdtypes.html#str), strdict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]], ferc1_engine: sqlalchemy.Engine) → [set](https://docs.python.org/3/library/stdtypes.html#set)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Identify as-of-yet uncategorized freeform strings in FERC Form 1.

* **Parameters:**
  * **table** – Name of the FERC Form 1 DB to search.
  * **field** – Name of the column in that table to search.
  * **strdict** – A string cleaning dictionary. See
    e.g. `pudl.transform.ferc1.FUEL_UNIT_STRINGS`
  * **ferc1_engine** – SQL Alchemy DB connection engine for the FERC Form 1 DB.
* **Returns:**
  Any string found in the searched table + field that was not part of any of
  categories enumerated in strdict.

### pudl.helpers.find_foreign_key_errors(dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]]

Report foreign key violations from a dictionary of dataframes.

The database schema to check against is generated based on the names of the
dataframes (keys of the dictionary) and the PUDL metadata structures.

* **Parameters:**
  **dfs** – Keys are table names, and values are dataframes ready for loading
  into the SQLite database.
* **Returns:**
  A list of dictionaries, each one pertains to a single database table
  in which a foreign key constraint violation was found, and it includes
  the table name, foreign key definition, and the elements of the
  dataframe that violated the foreign key constraint.

### pudl.helpers.download_zip_url(url: [str](https://docs.python.org/3/library/stdtypes.html#str), save_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), chunk_size: [int](https://docs.python.org/3/library/functions.html#int) = 128, timeout: [float](https://docs.python.org/3/library/functions.html#float) = 9.05) → [None](https://docs.python.org/3/library/constants.html#None)

Download and save a Zipfile locally.

Useful for acquiring and storing non-PUDL data locally.

* **Parameters:**
  * **url** – The URL from which to download the Zipfile
  * **save_path** – The location to save the file.
  * **chunk_size** – Data chunk in bytes to use while downloading.
  * **timeout** – Time to wait for the server to accept a connection.
    See [https://requests.readthedocs.io/en/latest/user/advanced/#timeouts](https://requests.readthedocs.io/en/latest/user/advanced/#timeouts)
* **Returns:**
  None

### pudl.helpers.add_fips_ids(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), state_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'state', county_col: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = 'county') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add State and County FIPS IDs to a dataframe.

To just add State FIPS IDs, make county_col = None.

### pudl.helpers.add_state_id_fips(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), state_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add the State FIPS codes.

### pudl.helpers.add_county_fips_id(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), geocodes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), county_col: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Add the County FIPS codes to a table with State FIPS codes.

### pudl.helpers.clean_eia_counties(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), fixes: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), state_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'state', county_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'county') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Replace non-standard county names with county names from US Census.

### pudl.helpers.oob_to_nan(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], lb: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None, ub: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Set non-numeric values and those outside of a given range to NaN.

* **Parameters:**
  * **df** – The dataframe containing values to be altered.
  * **cols** – Labels of the columns whose values are to be changed.
  * **lb** – Lower bound, below which values are set to NaN. If None, don’t use a lower
    bound.
  * **ub** – Upper bound, below which values are set to NaN. If None, don’t use an upper
    bound.
* **Returns:**
  The altered DataFrame.

### pudl.helpers.oob_to_nan_with_dependent_cols(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], dependent_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], lb: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None, ub: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Call [`oob_to_nan()`](#pudl.helpers.oob_to_nan) and additionally nullify any derived columns.

Set values in `cols` to NaN if values are non-numeric or outside of a
given range. The corresponding values in `dependent_cols` are then set
to NaN. `dependent_cols` should be columns derived from one or multiple
of the columns in `cols`.

* **Parameters:**
  * **df** – The dataframe containing values to be altered.
  * **cols** – Labels of the columns whose values are to be changed.
  * **dependent_cols** – Labels of the columns whose corresponding values should also be
    nullified. Columns are derived from one or multiple of the columns in
    `cols`.
  * **lb** – Lower bound, below which values are set to NaN. If None, don’t use a lower
    bound.
  * **ub** – Upper bound, below which values are set to NaN. If None, don’t use an upper
    bound.
* **Returns:**
  The altered DataFrame.

### pudl.helpers.prep_dir(dir_path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)

Create (or delete and recreate) a directory.

* **Parameters:**
  * **dir_path** – path to the directory that you are trying to clean and prepare.
  * **clobber** – If True and dir_path exists, it will be removed and replaced with a
    new, empty directory.
* **Raises:**
  [**FileExistsError**](https://docs.python.org/3/library/exceptions.html#FileExistsError) – if a file or directory already exists at dir_path.
* **Returns:**
  Path to the created directory.

### pudl.helpers.is_doi(doi: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Determine if a string is a valid digital object identifier (DOI).

Function simply checks whether the offered string matches a regular
expression – it doesn’t check whether the DOI is actually registered
with the relevant authority.

* **Parameters:**
  **doi** – String to validate.
* **Returns:**
  True if doi matches the regex for valid DOIs, False otherwise.

### pudl.helpers.convert_col_to_datetime(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), date_col_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert a non-datetime column in a dataframe to a datetime64[s].

If the column isn’t a datetime, it needs to be converted to a string type
first so that integer years are formatted correctly.

* **Parameters:**
  * **df** – Dataframe with column to convert.
  * **date_col_name** – name of the datetime column to convert.
* **Returns:**
  Dataframe with the converted datetime column.

### pudl.helpers.full_timeseries_date_merge(left: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), right: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), on: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], left_date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', right_date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', new_date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', date_on: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = ['year'], how: Literal['inner', 'outer', 'left', 'right', 'cross'] = 'inner', report_at_start: [bool](https://docs.python.org/3/library/functions.html#bool) = True, freq: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'MS', \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge dataframes with different date frequencies and expand to a full timeseries.

Arguments: see arguments for `date_merge` and `expand_timeseries`

### pudl.helpers.\_add_suffix_to_date_on(date_on)

Check date_on list is valid and add \_temp_for_merge suffix.

### pudl.helpers.date_merge(left: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), right: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), on: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], left_date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', right_date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', new_date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', date_on: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] = None, how: Literal['inner', 'outer', 'left', 'right', 'cross'] = 'inner', report_at_start: [bool](https://docs.python.org/3/library/functions.html#bool) = True, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Merge two dataframes that have different report date frequencies.

We often need to bring together data that is reported at different
temporal granularities e.g. monthly basis versus annual basis. This function
acts as a wrapper on a pandas merge to allow merging at different temporal
granularities. The date columns of both dataframes are separated into
year, quarter, month, and day columns. Then, the dataframes are merged according
to `how` on the columns specified by the `on` and `date_on` argument,
which list the new temporal columns to merge on as well any additional shared columns.
Finally, the datetime column is reconstructed in the output dataframe and
named according to the `new_date_col` parameter.

* **Parameters:**
  * **left** – The left dataframe in the merge. Typically monthly in our use
    cases if doing a left merge E.g. `core_eia923__monthly_generation`.
    Must contain columns specified by `left_date_col` and
    `on` argument.
  * **right** – The right dataframe in the merge. Typically annual in our uses
    cases if doing a left merge E.g. `core_eia860__scd_generators`.
    Must contain columns specified by `right_date_col` and
    `on` argument.
  * **on** – The columns to merge on that are shared between both
    dataframes. Typically ID columns like `plant_id_eia`, `generator_id`
    or `boiler_id`.
  * **left_date_col** – Column in `left` containing datetime like data. Default is
    `report_date`. Must be a Datetime or convertible to a Datetime using
    [`pandas.to_datetime()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime)
  * **right_date_col** – Column in `right` containing datetime like data. Default is
    `report_date`. Must be a Datetime or convertible to a Datetime using
    [`pandas.to_datetime()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime).
  * **new_date_col** – Name of the reconstructed datetime column in the output dataframe.
  * **date_on** – The temporal columns to merge on. Values in this list
    of columns must be [`year`, `quarter`, `month`, `day`].
    E.g. if a monthly reported dataframe is being merged onto a daily reported
    dataframe, then the merge would be performed on `["year", "month"]`.
    If one of these temporal columns already exists in the dataframe it will not
    be clobbered by the merge, as the suffix “_temp_for_merge” is added when
    expanding the datetime column into year, quarter, month, and day. By default,
    `date_on` will just include year.
  * **how** – How the dataframes should be merged. See `pandas.DataFrame.merge()`.
  * **report_at_start** – Whether the data in the dataframe whose report date is not being
    kept in the merged output (in most cases the less frequently reported dataframe)
    is reported at the start or end of the time period e.g. January 1st
    for annual data.
  * **kwargs** – Additional arguments to pass to `pandas.DataFrame.merge()`.
* **Returns:**
  Merged contents of left and right input dataframes.
* **Raises:**
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – if `left_date_col` or `right_date_col` columns are missing from their
    respective input dataframes.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – if any of the labels referenced in `on` are missing from either
    the left or right dataframes.

### pudl.helpers.expand_timeseries(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), key_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', freq: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'MS', fill_through_freq: Literal['year', 'month', 'day'] = 'year') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Expand a dataframe to a include a full time series at a given frequency.

This function adds a full timeseries to the given dataframe for each group
of columns specified by `key_cols`. The data in the timeseries will be filled
with the next previous chronological observation for a group of primary key columns
specified by `key_cols`.

* **Parameters:**
  * **df** – The dataframe to expand. Must have `date_col` in columns.
  * **key_cols** – Column names of the non-date primary key columns in the dataframe.
    The resulting dataframe will have a full timeseries expanded for each
    unique group of these ID columns that are present in the dataframe.
  * **date_col** – Name of the datetime column being expanded into a full timeseries.
  * **freq** – The frequency of the time series to expand the data to.
    See `pandas.tseries.offsets` and
    [`pandas.tseries.frequencies.to_offset()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.tseries.frequencies.to_offset.html#pandas.tseries.frequencies.to_offset) for valid frequency
    aliases.
  * **fill_through_freq** – The frequency in which to fill in the data through. For
    example, if equal to “year” the data will be filled in through the end of
    the last reported year for each grouping of `key_cols`. Valid frequencies
    are only “year”, “month”, or “day”.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – if `fill_through_freq` is not one of “year”, “month” or “day”.

### pudl.helpers.organize_cols(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Organize columns into key ID & name fields & alphabetical data columns.

For readability, it’s nice to group a few key columns at the beginning
of the dataframe (e.g. report_year or report_date, plant_id…) and then
put all the rest of the data columns in alphabetical order.

* **Parameters:**
  * **df** – The DataFrame to be re-organized.
  * **cols** – The columns to put first, in their desired output ordering.
* **Returns:**
  A dataframe with the same columns as the input
  DataFrame df, but with cols first, in the same order as they
  were passed in, and the remaining columns sorted alphabetically.
* **Return type:**
  [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.helpers.simplify_strings(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], copy: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Simplify the strings contained in a set of dataframe columns.

Performs several operations to simplify strings for comparison and parsing purposes.
These include removing Unicode control characters, stripping leading and trailing
whitespace, using lowercase characters, and compacting all internal whitespace to a
single space.

Leaves null values unaltered. Casts other values with astype(str).

Running with `copy=False` is intended for memory-intensive data frames where no
upstream process retains a reference to the data. Use care with this option,
and keep an eye out for spooky data changes showing up in unexpected places.

* **Parameters:**
  * **df** – DataFrame whose columns are being cleaned up.
  * **columns** – The labels of the string columns to be simplified.
  * **copy** – (Default True) Return a copy, making no changes to the original data.
* **Returns:**
  The whole DataFrame that was passed in, with the string columns cleaned up.

### pudl.helpers.cleanstrings_series(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), str_map: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]], unmapped: [str](https://docs.python.org/3/library/stdtypes.html#str) | pandas._libs.missing.NAType | [None](https://docs.python.org/3/library/constants.html#None) = None, simplify: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Clean up the strings in a single column/Series.

* **Parameters:**
  * **col** – A pandas Series, typically a single column of a
    dataframe, containing the freeform strings that are to be cleaned.
  * **str_map** – A dictionary of lists of strings, in which the keys are
    the simplified canonical strings, with which each string found in
    the corresponding list will be replaced.
  * **unmapped** – A value with which to replace any string found in col
    that is not found in one of the lists of strings in map. Typically
    the null string ‘’. If None, these strings will not be replaced.
  * **simplify** – If True, strip and compact whitespace, and lowercase
    all strings in both the list of values to be replaced, and the
    values found in col. This can reduce the number of strings that
    need to be kept track of.
* **Returns:**
  The cleaned up Series / column, suitable for replacing the original messy column
  in a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame).

### pudl.helpers.cleanstrings(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], stringmaps: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]]], unmapped: [str](https://docs.python.org/3/library/stdtypes.html#str) | pandas._libs.missing.NAType | [None](https://docs.python.org/3/library/constants.html#None) = None, simplify: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Consolidate freeform strings in several dataframe columns.

This function will consolidate freeform strings found in `columns` into simplified
categories, as defined by `stringmaps`. This is useful when a field contains many
different strings that are really meant to represent a finite number of categories,
e.g. a type of fuel. It can also be used to create simplified categories that apply
to similar attributes that are reported in various data sources from different
agencies that use their own taxonomies.

The function takes and returns a pandas.DataFrame, making it suitable for use with
the `pandas.DataFrame.pipe()` method in a chain.

* **Parameters:**
  * **df** – the DataFrame containing the string columns to be cleaned up.
  * **columns** – a list of string column labels found in the column index of df. These
    are the columns that will be cleaned.
  * **stringmaps** – a list of dictionaries. The keys of these dictionaries are strings,
    and the values are lists of strings. Each dictionary in the list corresponds
    to a column in columns. The keys of the dictionaries are the values with
    which every string in the list of values will be replaced.
  * **unmapped** – the value with which strings not found in the stringmap dictionary
    will be replaced. Typically the null string ‘’. If None, then strings found
    in the columns but not in the stringmap will be left unchanged.
  * **simplify** – If true, strip whitespace, remove duplicate whitespace, and force
    lower-case on both the string map and the values found in the columns to be
    cleaned. This can reduce the overall number of string values that need to be
    tracked.
* **Returns:**
  The function returns a new DataFrame containing the cleaned strings.

### pudl.helpers.fix_int_na(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], float_na: [float](https://docs.python.org/3/library/functions.html#float) = np.nan, int_na: [int](https://docs.python.org/3/library/functions.html#int) = -1, str_na: [str](https://docs.python.org/3/library/stdtypes.html#str) = '') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert NA containing integer columns from float to string.

Numpy doesn’t have a real NA value for integers. When pandas stores integer data
which has NA values, it thus upcasts integers to floating point values, using np.nan
values for NA. However, in order to dump some of our dataframes to CSV files for use
in data packages, we need to write out integer formatted numbers, with empty strings
as the NA value. This function replaces np.nan values with a sentinel value,
converts the column to integers, and then to strings, finally replacing the sentinel
value with the desired NA string.

This is an interim solution – now that pandas extension arrays have been
implemented, we need to go back through and convert all of these integer columns
that contain NA values to Nullable Integer types like Int64.

* **Parameters:**
  * **df** – The dataframe to be fixed. This argument allows method chaining with the
    pipe() method.
  * **columns** – A list of DataFrame column labels indicating which columns need to be
    reformatted for output.
  * **float_na** – The floating point value to be interpreted as NA and replaced in col.
  * **int_na** – Sentinel value to substitute for float_na prior to conversion of the
    column to integers.
  * **str_na** – String value to substitute for int_na after the column has been
    converted to strings.
* **Returns:**
  A new DataFrame, with the selected columns converted to strings that look like
  integers, compatible with the postgresql COPY FROM command.

### pudl.helpers.month_year_to_date(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert all pairs of year/month fields in a dataframe into Date fields.

This function finds all column names within a dataframe that match the
regular expression ‘_month$’ and ‘_year$’, and looks for pairs that have
identical prefixes before the underscore. These fields are assumed to
describe a date, accurate to the month.  The two fields are used to
construct a new \_date column (having the same prefix) and the month/year
columns are then dropped.

* **Parameters:**
  **fields.** (*The DataFrame in which to convert year/months fields to Date*)
* **Returns:**
  A DataFrame in which the year/month fields have been converted into Date fields.

### pudl.helpers.convert_to_date(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), date_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_date', year_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_year', month_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_month', day_col: [str](https://docs.python.org/3/library/stdtypes.html#str) = 'report_day', month_na_value: [int](https://docs.python.org/3/library/functions.html#int) = 1, day_na_value: [int](https://docs.python.org/3/library/functions.html#int) = 1, copy: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert specified year, month or day columns into a datetime object.

If the input `date_col` already exists in the input dataframe, then no
conversion is applied, and the original dataframe is returned unchanged.
Otherwise the constructed date is placed in that column, and the columns
which were used to create the date are dropped.

Running with `copy=False` is intended for memory-intensive data frames where no
upstream process retains a reference to the data. Use care with this option,
and keep an eye out for spooky data changes showing up in unexpected places.

* **Parameters:**
  * **df** – dataframe to convert
  * **date_col** – the name of the column you want in the output.
  * **year_col** – the name of the year column in the original table.
  * **month_col** – the name of the month column in the original table.
  * **day_col** – the name of the day column in the original table.
  * **month_na_value** – generated month if no month exists or if the month
    value is NA.
  * **day_na_value** – generated day if no day exists or if the day value is NA.
  * **copy** – (default True) return a copy, making no changes to the original data.
* **Returns:**
  A DataFrame in which the year, month, day columns values have been converted
  into datetime objects.

### pudl.helpers.remove_leading_zeros_from_numeric_strings(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), col_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Remove leading zeros frame column values that are numeric strings.

Sometimes an ID column (like generator_id or unit_id) will be reported with leading
zeros and sometimes it won’t. For example, in the Excel spreadsheets published by
EIA, the same generator may show up with the ID “0001” and “1” in different years
This function strips the leading zeros from those numeric strings so the data can
be mapped across years and datasets more reliably.

Alphanumeric generator IDs with leadings zeroes are not affected, as we found no
instances in which an alphanumeric ID appeared both with and without leading zeroes.
The ID “0A1” will stay “0A1”.

* **Parameters:**
  * **df** – A DataFrame containing the column you’d like to remove numeric leading zeros
    from.
  * **col_name** – The name of the column you’d like to remove numeric leading zeros
    from.
* **Returns:**
  A DataFrame without leading zeros for numeric string values in the desired
  column.

### pudl.helpers.standardize_na_values(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Replace common apparently NA values in numerical columns with the floating point np.nan.

Currently replaces the empty string, any string entirely composed of whitespace,
bare decimal points, and any string entirely composed of hyphens, all of which are
common stand-ins for NA in spreadsheets. Note that this function should only be
applied to columns whose true type is expected to be numeric.

* **Parameters:**
  **df** – The DataFrame to clean.
* **Returns:**
  DataFrame with regularized NA values.

### pudl.helpers.simplify_columns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Simplify column labels for use as snake_case database fields.

All column labels will be simplified by:

* Replacing all non-alphanumeric characters with spaces.
* Forcing all letters to be lower case.
* Compacting internal whitespace to a single “ “.
* Stripping leading and trailing whitespace.
* Replacing all remaining whitespace with underscores.

* **Parameters:**
  **df** – The DataFrame whose column labels to simplify.
* **Returns:**
  A dataframe with simplified column names.

### pudl.helpers.drop_tables(engine: sqlalchemy.Engine, clobber: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [None](https://docs.python.org/3/library/constants.html#None)

Drops all tables from a SQLite database.

Creates an sa.schema.MetaData object reflecting the structure of the
database that the passed in `engine` refers to, and uses that schema to
drop all existing tables.

* **Parameters:**
  * **engine** – An SQL Alchemy SQLite database Engine pointing at an existing SQLite
    database to be deleted.
  * **clobber** – Whether or not to allow a non-empty DB to be removed.
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – if clobber is False and there are any tables in the database.
* **Returns:**
  None

### pudl.helpers.merge_dicts(lods: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, Any]]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[Any, Any]

Merge multiple dictionaries together.

Given any number of dicts, shallow copy and merge into a new dict, precedence goes
to key value pairs in latter dicts within the input list.

* **Parameters:**
  **lods** – a list of dictionaries.
* **Returns:**
  A single merged dictionary.

### pudl.helpers.convert_cols_dtypes(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), data_source: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Convert a PUDL dataframe’s columns to the correct data type.

Boolean type conversions created a special problem, because null values in
boolean columns get converted to True (which is bonkers!)… we generally
want to preserve the null values and definitely don’t want them to be True,
so we are keeping those columns as objects and preforming a simple mask for
the boolean columns.

The other exception in here is with the `utility_id_eia` column. It is
often an object column of strings. All of the strings are numbers, so it
should be possible to convert to `pandas.Int32Dtype()` directly, but it
is requiring us to convert to int first. There will probably be other
columns that have this problem… and hopefully pandas just enables this
direct conversion.

* **Parameters:**
  * **df** – dataframe with columns that appear in the PUDL tables.
  * **data_source** – the name of the datasource (eia, ferc1, etc.)
  * **name** – name of the table; used for logging and for looking up the table’s
    schema-defined field types.
* **Returns:**
  Input dataframe, but with column types as specified by
  [`pudl.metadata.fields.FIELD_METADATA`](../metadata/fields/index.md#pudl.metadata.fields.FIELD_METADATA)

### pudl.helpers.generate_rolling_avg(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), group_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], data_col: [str](https://docs.python.org/3/library/stdtypes.html#str), window: [int](https://docs.python.org/3/library/functions.html#int), \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Generate a rolling average.

For a given dataframe with a `report_date` column, generate a monthly
rolling average and use this rolling average to impute missing values.

* **Parameters:**
  * **df** – Original dataframe. Must have group_cols column, a data_col column and a
    `report_date` column.
  * **group_cols** – a list of columns to groupby.
  * **data_col** – the name of the data column.
  * **window** – rolling window argument to pass to [`pandas.Series.rolling()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.rolling.html#pandas.Series.rolling).
  * **kwargs** – Additional arguments to pass to [`pandas.Series.rolling()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.rolling.html#pandas.Series.rolling).
* **Returns:**
  DataFrame with an additional rolling average column.

### pudl.helpers.fillna_w_rolling_avg(df_og: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), group_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], data_col: [str](https://docs.python.org/3/library/stdtypes.html#str), window: [int](https://docs.python.org/3/library/functions.html#int) = 12, \*\*kwargs) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fill NA values with a rolling average.

Imputes null values from a dataframe using a rolling monthly average.

* **Parameters:**
  * **df_og** – Original dataframe. Must have `group_cols` columns, a `data_col`
    column and a `report_date` column.
  * **group_cols** – a list of columns to groupby.
  * **data_col** – the name of the data column we’re trying to fill.
  * **window** – rolling window to pass to [`pandas.Series.rolling()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.rolling.html#pandas.Series.rolling).
  * **kwargs** – Additional arguments to pass to [`pandas.Series.rolling()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.rolling.html#pandas.Series.rolling).
* **Returns:**
  dataframe with nulls filled in.
* **Return type:**
  [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.helpers.groupby_agg_label_unique_source_or_mixed(x: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)) → [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)

Get either the unique source in a group or return mixed.

Custom function for groupby.agg. Written specifically for
aggregating records with fuel_cost_per_mmbtu_source.

### pudl.helpers.count_records(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], new_count_col_name: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Count the number of unique records in group in a dataframe.

* **Parameters:**
  * **df** – dataframe you would like to groupby and count.
  * **cols** – list of columns to group and count by.
  * **new_count_col_name** – the name that will be assigned to the column that will
    contain the count.
* **Returns:**
  DataFrame containing only `cols` and `new_count_col_name`.

### pudl.helpers.cleanstrings_snake(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Clean the strings in a columns in a dataframe with snake case.

* **Parameters:**
  * **df** – original dataframe.
  * **cols** – list of columns in to apply snake case to.

### pudl.helpers.zero_pad_numeric_string(col: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), n_digits: [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Clean up fixed-width leading zero padded numeric (e.g. ZIP, FIPS) codes.

Often values like ZIP and FIPS codes are stored as integers, or get
converted to floating point numbers because there are NA values in the
column. Sometimes other non-digit strings are included like Canadian
postal codes mixed in with ZIP codes, or IMP (imported) instead of a
FIPS county code. This function attempts to manage these irregularities
and produce either fixed-width leading zero padded strings of digits
having a specified length (n_digits) or NA.

* Convert the Series to a nullable string.
* Remove any decimal point and all digits following it.
* Remove any non-digit characters.
* Replace any empty strings with NA.
* Replace any strings longer than n_digits with NA.
* Pad remaining digit-only strings to n_digits length.
* Replace (invalid) all-zero codes with NA.

* **Parameters:**
  * **col** – The Series to clean. May be numeric, string, object, etc.
  * **n_digits** – the desired length of the output strings.
* **Returns:**
  A Series of nullable strings, containing only all-numeric strings
  having length n_digits, padded with leading zeroes if necessary.

### pudl.helpers.iterate_multivalue_dict(\*\*kwargs)

Make dicts from dict with main dict key and one value of main dict.

If kwargs is {‘form;: ‘gas_distribution’, ‘years’: [2019, 2020]}, it will yield these results:
: {‘form’: ‘gas_distribution’, ‘years’: 2019}
  {‘form’: ‘gas_distribution’, ‘years’: 2020}

### pudl.helpers.dedupe_on_category(dedup_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), base_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], category_name: [str](https://docs.python.org/3/library/stdtypes.html#str), sorter: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Deduplicate a df using a sorted category to retain preferred values.

Use a sorted category column to retain your preferred values when a
dataframe is deduplicated.

* **Parameters:**
  * **dedup_df** – the dataframe with the records to deduplicate.
  * **base_cols** – list of columns which must not be duplicated.
  * **category_name** – name of the categorical column to order values for deduplication.
  * **sorter** – sorted list of categorical values found in the `category_name` column.
* **Returns:**
  The deduplicated dataframe.

### pudl.helpers.dedupe_and_drop_nas(dedup_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), primary_key_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Deduplicate a df by comparing primary key columns and dropping null rows.

When a primary key appears twice in a dataframe, and one record is all null other
than the primary keys, drop the null row.

* **Parameters:**
  * **dedup_df** – the dataframe with the records to deduplicate.
  * **primary_key_cols** – list of columns which must not be duplicated.
* **Returns:**
  The deduplicated dataframe.

### pudl.helpers.drop_records_with_null_in_column(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), column: [str](https://docs.python.org/3/library/stdtypes.html#str), num_of_expected_nulls: [int](https://docs.python.org/3/library/functions.html#int)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Drop a prescribed number of records with null values in a column.

* **Parameters:**
  * **df** – table with column to check.
  * **column** – name of column with potential null values.
  * **num_of_expected_nulls** – the number of records with null values in the column
* **Raises:**
  [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – If there are more nulls in the df then the
  num_of_expected_nulls.

### pudl.helpers.standardize_percentages_ratio(frac_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), mixed_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], years_to_standardize: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Standardize year-to-year changes in mixed percentage/ratio reporting in a column.

When a column uses both 0-1 and 0-100 scales to describe percentages, standardize
the years using 0-100 scales to 0-1 ratios/fractions.

* **Parameters:**
  * **frac_df** – the dataframe with the columns to standardize.
  * **mixed_cols** – list of columns which should get standardized to the 0-1 scale.
  * **years_to_standardize** – range of dates over which the standardization should occur.
* **Returns:**
  The standardized dataframe.

### pudl.helpers.calc_capacity_factor(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), freq: Literal['YS', 'MS'], min_cap_fact: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None, max_cap_fact: [float](https://docs.python.org/3/library/functions.html#float) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Calculate capacity factor.

Capacity factor is calculated from the capacity, the net generation over a
time period and the hours in that same time period. The dates from that
dataframe are pulled out to determine the hours in each period based on
the frequency. The number of hours is used in calculating the capacity
factor. Then records with capacity factors outside the range specified by
`min_cap_fact` and `max_cap_fact` are dropped.

* **Parameters:**
  * **df** – table with required inputs for capacity factor (`report_date`,
    `net_generation_mwh` and `capacity_mw`).
  * **freq** – String describing time frequency at which to aggregate the reported data,
    such as `MS` (month start) or `YS` (annual start).
  * **min_cap_fact** – Lower bound, below which values are set to NaN. If None, don’t use
    a lower bound. Default is None.
  * **max_cap_fact** – Upper bound, below which values are set to NaN.  If None, don’t
    use an upper bound. Default is None.
* **Returns:**
  Modified version of the input DataFrame with an additional `capacity_factor`
  column.

### pudl.helpers.weighted_average(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), data_col: [str](https://docs.python.org/3/library/stdtypes.html#str), weight_col: [str](https://docs.python.org/3/library/stdtypes.html#str), by: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Generate a weighted average.

* **Parameters:**
  * **df** – A DataFrame containing, at minimum, the columns specified in the other
    parameters data_col and weight_col.
  * **data_col** – column name of data column to average
  * **weight_col** – column name to weight on
  * **by** – List of columns to group by when calculating the weighted average value.
* **Returns:**
  A table with `by` columns as the index and the weighted `data_col`.

### pudl.helpers.sum_and_weighted_average_agg(df_in: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), by: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], sum_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], wtavg_dict: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Aggregate dataframe by summing and using weighted averages.

Many times we want to aggregate a data table using the same groupby columns but with
different aggregation methods. This function combines two of our most common
aggregation methods (summing and applying a weighted average) into one function.
Because pandas does not have a built-in weighted average method for groupby we use
:func:`weighted_average`.

* **Parameters:**
  * **df_in** – input table to aggregate. Must have columns in `id_cols`, `sum_cols`
    and keys from `wtavg_dict`.
  * **by** – columns to group/aggregate based on. These columns will be passed as an
    argument into grouby as `by` arg.
  * **sum_cols** – columns to sum.
  * **wtavg_dict** – dictionary of columns to average (keys) and columns to weight by
    (values).
* **Returns:**
  table with join of columns from `by`, `sum_cols` and keys of `wtavg_dict`.
  Primary key of table will be `by`.

### pudl.helpers.get_eia_ferc_acct_map() → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Get map of EIA technology_description/pm codes <> ferc accounts.

* **Returns:**
  table which maps the combination of EIA’s technology
  : description and prime mover code to FERC Uniform System of Accounts
    (USOA) accounting names. Read more about USOA
    [here](https://www.ferc.gov/enforcement-legal/enforcement/accounting-matters)
    The output table has the following columns: `['technology_description',
    'prime_mover_code', 'ferc_acct_name']`
* **Return type:**
  [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.helpers.dedupe_n_flatten_list_of_lists(mega_list: [list](https://docs.python.org/3/library/stdtypes.html#list)) → [list](https://docs.python.org/3/library/stdtypes.html#list)

Flatten a list of lists and remove duplicates.

### pudl.helpers.flatten_list(xs: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)) → [collections.abc.Generator](https://docs.python.org/3/library/collections.abc.html#collections.abc.Generator)

Flatten an irregular (arbitrarily nested) list of lists (or sets).

Inspiration from
[here](https://stackoverflow.com/questions/2158395/flatten-an-irregular-arbitrarily-nested-list-of-lists)

### pudl.helpers.convert_df_to_excel_file(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), \*\*kwargs) → [pandas.ExcelFile](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.ExcelFile.html#pandas.ExcelFile)

Convert a [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) into a [`pandas.ExcelFile`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.ExcelFile.html#pandas.ExcelFile).

* **Parameters:**
  * **df** – The DataFrame to convert.
  * **kwargs** – Additional arguments to pass into `pandas.to_excel()`.
* **Returns:**
  The contents of the input DataFrame, represented as an ExcelFile.

### pudl.helpers.get_asset_keys(assets: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)], exclude_asset_specs: [bool](https://docs.python.org/3/library/functions.html#bool) = True) → [set](https://docs.python.org/3/library/stdtypes.html#set)[[dagster.AssetKey](https://docs.dagster.io/api/dagster/assets/#dagster.AssetKey)]

Get a set of asset keys from a list of asset definitions.

* **Parameters:**
  * **assets** – list of asset definitions.
  * **exclude_asset_specs** – exclude AssetSpecs in the returned list.
    Some selection operations don’t allow AssetSpec keys.
* **Returns:**
  A set of asset keys.

### pudl.helpers.get_asset_group_keys(asset_group: [str](https://docs.python.org/3/library/stdtypes.html#str), all_assets: [list](https://docs.python.org/3/library/stdtypes.html#list)[[dagster.AssetsDefinition](https://docs.dagster.io/api/dagster/assets/#dagster.AssetsDefinition)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Get a list of asset names in a given asset group.

* **Parameters:**
  * **asset_group** – the name of the asset group.
  * **all_assets** – the collection of assets to select the group from.
* **Returns:**
  A list of asset names in the asset_group.

### pudl.helpers.convert_col_to_bool(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), col_name: [str](https://docs.python.org/3/library/stdtypes.html#str), true_values: [list](https://docs.python.org/3/library/stdtypes.html#list), false_values: [list](https://docs.python.org/3/library/stdtypes.html#list)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Turn a column into a boolean while preserving NA values.

You don’t have to specify NA as true or false - it will preserve it’s NA-ness unless
you add it to one of the input true/false lists.

* **Parameters:**
  * **df** – The dataframe containing the column you want to change.
  * **col_name** – The name of the column you want to turn into a boolean (must be an
    existing column, not a new column name).
  * **true_values** – The list of values in col_name that you want to be marked as True.
  * **false_values** – The list of values appearing in col_name that you want to be
    False.
* **Raises:**
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – if there are non-NA values in col_name that aren’t specified in
    true_values or false_values.
  * [**AssertionError**](https://docs.python.org/3/library/exceptions.html#AssertionError) – if there are values that appear in both true_values and
    false_values.
* **Returns:**
  The original dataframe with col_name as a boolean column.
* **Return type:**
  pd.DataFrame

### pudl.helpers.fix_boolean_columns(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), boolean_columns_to_fix: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], inplace: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Fix standard issues with boolean columns.

Most boolean columns have either “Y” for True or “N” for False. A subset of the
columns have “X” values which represents a False value. A subset of the columns
have “U” values, presumably for “Unknown,” which must be set to null in order to
convert the columns to datatype Boolean. Other data sources may use a 1/0 system
instead, with 1 as True and 0 as False.

If running with `inplace=True`, will run in-place versions of the fill and
replace operations instead of returning a copy of the data frame. This mode is
useful for memory-intensive data frames, but be aware that upstream processes
retaining a reference to the data will see the changes made here.

### pudl.helpers.scale_by_ownership(gens: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), own_eia860: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), scale_cols: [list](https://docs.python.org/3/library/stdtypes.html#list), validate: [str](https://docs.python.org/3/library/stdtypes.html#str) = '1:m') → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Generate proportional data by ownership %s.

Why do we have to do this at all? Sometimes generators are owned by
many different utility owners that own slices of that generator. EIA
reports which portion of each generator is owned by which utility
relatively clearly in their ownership table. On the other hand, in
FERC1, sometimes a partial owner reports the full plant-part, sometimes
they report only their ownership portion of the plant-part. And of
course it is not labeled in FERC1. Because of this, we need to compile
all of the possible ownership slices of the EIA generators.

In order to accumulate every possible version of how a generator could
be reported, this method generates two records for each generator’s
reported owners: one of the portion of the plant part they own and one
for the plant-part as a whole. The portion records are labeled in the
`ownership_record_type` column as “owned” and the total records are labeled as
“total”.

In this function we merge in the ownership table so that generators
with multiple owners then have one record per owner with the
ownership fraction (in column `fraction_owned`). Because the ownership
table only contains records for generators that have multiple owners,
we assume that all other generators are owned 100% by their operator.
Then we generate the “total” records by duplicating the “owned” records
but assigning the `fraction_owned` to be 1 (i.e. 100%).

* **Parameters:**
  * **gens** – table with records at the generator level and generator attributes
    to be scaled by ownership, must have columns `plant_id_eia`,
    `generator_id`, and `report_date`
  * **own_eia860** – the `core_eia860__scd_ownership` table
  * **scale_cols** – a list of columns in the generator table to slice by ownership
    fraction
  * **validate** – how to validate merging the ownership table onto the
    generators table
* **Returns:**
  Table of generator records with `scale_cols` sliced by ownership fraction
  such that there is a “total” and “owned” record for each generator owner.
  The “owned” records have the generator’s data scaled to the ownership
  percentage (e.g. if a 200 MW generator has a 75% stake owner and a 25%
  stake owner, this will result in two “owned” records with 150 MW and 50 MW).
  The “total” records correspond to the full plant for every owner (e.g. using
  the same 2-owner 200 MW generator as above, each owner will have a
  records with 200 MW).

### pudl.helpers.assert_cols_areclose(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), a_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], b_cols: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], mismatch_threshold: [float](https://docs.python.org/3/library/functions.html#float), message: [str](https://docs.python.org/3/library/stdtypes.html#str))

Check if two column sets of a dataframe are close to each other.

Ignores NANs and raises if there are too many mismatches.

### *class* pudl.helpers.TableDiff

Bases: `NamedTuple`

Represent a diff between two versions of the same table.

#### deleted *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

#### added *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

#### changed *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

#### old_df *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

#### new_df *: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)*

### pudl.helpers.diff_wide_tables(primary_key: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[str](https://docs.python.org/3/library/stdtypes.html#str)], old: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), new: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [TableDiff](#pudl.helpers.TableDiff)

Diff values across multiple iterations of the same wide table.

We often have tables with many value columns; a straightforward comparison of two
versions of the same table will show you that two rows are different, but
won’t show which of the many values changed.

So we melt the table based on some sort of primary key columns then diff
the old and new values.

### pudl.helpers.retry(func: [collections.abc.Callable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable), retry_on: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[type](../metadata/classes/index.md#pudl.metadata.classes.Field.type)[[BaseException](https://docs.python.org/3/library/exceptions.html#BaseException)], Ellipsis], max_retries=5, base_delay_sec=1, \*\*kwargs)

Retry a function with a short sleep between each try.

Sleeps twice as long before each retry as the last one, e.g. 1/2/4/8/16
seconds.

* **Parameters:**
  * **func** – the function to retry
  * **retry_on** – the errors to catch.
  * **base_delay_sec** – how much time to sleep for the first retry.
  * **kwargs** – keyword arguments to pass to the wrapped function. Pass non-kwargs as
    kwargs too.

### pudl.helpers.get_parquet_table_polars(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), partitions: [dict](https://docs.python.org/3/library/stdtypes.html#dict) | [None](https://docs.python.org/3/library/constants.html#None) = None, paths: [pudl.workspace.setup.PudlPaths](../workspace/setup/index.md#pudl.workspace.setup.PudlPaths) | [None](https://docs.python.org/3/library/constants.html#None) = None) → polars.LazyFrame

Read a table from a parquet file and return as a polars LazyFrame.

* **Parameters:**
  * **table_name** – Name of the table to read.
  * **partitions** – Optional dictionary of partitions to filter the data. See
    [`ParquetData`](#pudl.helpers.ParquetData) definition for details.
* **Returns:**
  A polars LazyFrame representing the table.

### pudl.helpers.get_parquet_table(table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [None](https://docs.python.org/3/library/constants.html#None) = None, filters: [list](https://docs.python.org/3/library/stdtypes.html#list)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [list](https://docs.python.org/3/library/stdtypes.html#list)[[list](https://docs.python.org/3/library/stdtypes.html#list)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str), Any]]] | [None](https://docs.python.org/3/library/constants.html#None) = None, paths: [pudl.workspace.setup.PudlPaths](../workspace/setup/index.md#pudl.workspace.setup.PudlPaths) | [None](https://docs.python.org/3/library/constants.html#None) = None) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | [geopandas.GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html#geopandas.GeoDataFrame)

Read a table from Parquet files with optional column selection and filtering.

This function provides a general-purpose interface for reading PUDL tables from
Parquet files. It supports selective column reading for performance, optional
filters for data subsetting, and automatic schema validation.

* **Parameters:**
  * **table_name** – Name of the table to read.
  * **columns** – List of columns to read. If None, all columns are read.
  * **filters** – Optional filters to apply when reading the Parquet file. See the
    [`pyarrow.parquet.read_table()`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html#pyarrow.parquet.read_table) documentation for details on filter
    syntax. If None, no filters are applied.
* **Returns:**
  DataFrame with the requested data, with PUDL schema validation applied.
* **Raises:**
  * [**FileNotFoundError**](https://docs.python.org/3/library/exceptions.html#FileNotFoundError) – If the Parquet file for the table doesn’t exist.
  * [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – If the table_name is not a valid PUDL resource.

### pudl.helpers.standardize_phone_column(df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), columns: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Standardize phone numbers in the specified columns of the DataFrame.

US numbers: ###-###-####
International numbers with the international code at the beginning.
Numbers with extensions will be appended with “x#”.
Non-numeric entries will be returned as np.nan. Entries with fewer than
10 digits will be returned with no hyphens.

* **Parameters:**
  * **df** – The DataFrame to modify.
  * **columns** – A list of the names of the columns that need to be standardized.
* **Returns:**
  The modified DataFrame with standardized phone numbers in the same column.

### *class* pudl.helpers.ParquetData(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

Wrap table data offloaded as parquet file(s) on disk.

Writing data to disk as parquet files enables the use of highly efficient
processing/transforms with tools like Polars or duckdb. This class provides
helpers for managing paths to parquet data on disk.

#### table_name *: [str](https://docs.python.org/3/library/stdtypes.html#str)*

Name of the table corresponding to the parquet data.

#### partitions *: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]* *= None*

Optional dictionary of partition values indicating what data is being offloaded to disk.

If passed `{'years': 1995}` then this class will produce a parquet file at the
path `PudlPaths().parquet_path() / table_name / 1995.parquet`.  if passed
`{'years': 1995, 'states': 'CA'}` then this class will produce a parquet file at
the path `PudlPaths().parquet_path() / table_name / 1995_ca.parquet`.  If
partitions is empty, then the parquet file will be written
`PudlPaths().parquet_path() / table_name / {table_name}.parquet`.

#### *property* parquet_directory *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Get path to directory for writing/reading parquet files.

#### *property* parquet_path *: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)*

Get name of an individual parquet file corresponding to a single partition of data.

### pudl.helpers.persist_table_as_parquet(table_data: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame) | polars.LazyFrame | [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), table_name: [str](https://docs.python.org/3/library/stdtypes.html#str), partitions: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, compression: Literal['zstd', 'snappy', 'gzip', 'brotli'] = 'zstd') → [ParquetData](#pudl.helpers.ParquetData)

Write data from DataFrame or LazyFrame to disk as a parquet file.

Offloading data to disk allows Polars and duckdb to perform highly efficient
transforms.

* **Parameters:**
  * **table_data** – Tabular data to write to disk.
  * **table_name** – Table name used to construct path to/name of parquet file.
  * **partitions** – Optional partition dimension values indicating the data to be
    written.

### pudl.helpers.lf_from_parquet(parquet_data: [ParquetData](#pudl.helpers.ParquetData), use_all_partitions: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → polars.LazyFrame

Scan parquet file(s) from disk and return Polars LazyFrame.

* **Parameters:**
  * **parquet_data** – Points to parquet data on disk.
  * **use_all_partitions** – If true read the entire directory of parquet files.
    Otherwise only read data from the partition specified in parquet_data.

### pudl.helpers.df_from_parquet(parquet_data: [ParquetData](#pudl.helpers.ParquetData), use_all_partitions: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Read data from a set of parquet files and return a pandas DataFrame.

* **Parameters:**
  * **parquet_data** – Points to parquet data on disk.
  * **use_all_partitions** – If true read the entire directory of parquet files.
    Otherwise only read data from the partition specified in parquet_data.

### pudl.helpers.duckdb_relation_from_parquet(parquet_data: [ParquetData](#pudl.helpers.ParquetData), use_all_partitions: [bool](https://docs.python.org/3/library/functions.html#bool) = False) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)]

Create a duckdb relation to read from parquet files.

This method is intended to be used as a context manager to keep the duckdb
connection open while the relation is in use.

* **Parameters:**
  * **parquet_data** – Points to parquet data on disk.
  * **use_all_partitions** – If true read the entire directory of parquet files.
    Otherwise only read data from the partition specified in parquet_data.

### pudl.helpers.duckdb_extract_zipped_csv(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str), partitions: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any], pages: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], datasore, zip_path: [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path) = Path()) → [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), [ParquetData](#pudl.helpers.ParquetData)]

Extract data from zipped CSV page(s) in a data archive.

A common pattern for raw PUDL data is a set of zipfiles with one zipfile per
partition, and one or more CSV files within each zipfile. This function provides
a highly efficient way to extract data from archives that conform to this pattern
using duckdb. This function is a generator, which will yield a duckdb relation for
each CSV file within a zipfile, allowing the caller to perform transforms using
the relation before writing to disk with the `offload_table` function.

* **Parameters:**
  * **dataset** – Name of dataset (required to get archive from datastore).
  * **partition** – Partitions of resource to extract data from.
  * **pages** – List of csv files to extract from archive.
  * **datastore** – Instance of PUDL datastore to get raw data.
  * **zip_path** – Base path within zipfile that points to where CSV files are stored.
    If not explicitly set, assume CSV files are at the top level of the zipfile.

### pudl.helpers.normalize_year_fragments(raw_year: [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series), min_valid_year: [int](https://docs.python.org/3/library/functions.html#int), max_valid_year: [int](https://docs.python.org/3/library/functions.html#int), base_century: [int](https://docs.python.org/3/library/functions.html#int) = 2000) → [pandas.Series](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.html#pandas.Series)

Normalize year fragments into 4-digit years using a rolling-century rule.

This helper accepts a Series of string years that are either 2-digit (e.g. `"05"`
or `"95"`) or 4-digit (e.g. `"2008"`) and returns integer 4-digit years.

Two-digit years are interpreted using a rolling-century rule anchored to
`base_century`. By default `base_century=2000`, so this logic is geared toward
20th/21st century data: two-digit values are first mapped into 2000-2099, and then
shifted back 100 years when they exceed `max_valid_year`.

For example, with `base_century=2000` and `max_valid_year=2026`:

* `"05"` -> `2005`
* `"95"` -> `1995`

* **Parameters:**
  * **raw_year** – Series of string-like year tokens extracted from raw date text.
  * **min_valid_year** – Lower year bound used to avoid mapping to past years.
  * **max_valid_year** – Upper year bound used to avoid mapping to future years.
  * **base_century** – Century anchor for two-digit years. Must be divisible by 100.
    Defaults to 2000.
* **Returns:**
  A Series of integer-like 4-digit year values.
* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – If the valid year range is too wide to apply the rolling-century
  logic, or if any resulting year falls outside the valid range.

### pudl.helpers.make_changelog(df_all: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), idx: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)])

Make a changelog table with unique instances of values over start report and max report date.

### pudl.helpers.parse_address(addr: [str](https://docs.python.org/3/library/stdtypes.html#str))

Parse a U.S. address into components.

### pudl.helpers.listify(x: Any) → [list](https://docs.python.org/3/library/stdtypes.html#list)[Any]

Listify an input that is sometimes a list and sometimes not.

### pudl.helpers.env_var_is_true(env_var: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [bool](https://docs.python.org/3/library/functions.html#bool)

Check that environment variable is a ‘truthy’ value.

Check will pass if the variable is one of (all values are case insensitive):
- ‘true’
- ‘1’
- ‘yes’
- ‘on’
