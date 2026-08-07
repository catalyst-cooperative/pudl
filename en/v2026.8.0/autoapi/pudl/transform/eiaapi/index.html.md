# pudl.transform.eiaapi

Clean and normalize EIA bulk electricity data.

EIA’s bulk electricity data contains >750,000 timeseries. These timeseries contain a
variety of measures (fuel amount and cost are just two) across multiple levels of
aggregation, from individual plants to national averages.

The data is formatted as a single >1GB text file of line-delimited JSON with one line
per timeseries. Each JSON structure has two nested levels: the top level contains
metadata describing the series and the second level (under the “data” heading) contains
an array of timestamp/value pairs. This structure leads to a natural normalization into
two tables: one of metadata and one of timeseries. That is the format delivered by the
extract module.

The transform module parses a compound primary key out of long string IDs (“series_id”).
The rest of the metadata is not very valuable so is not transformed or returned.

The EIA aggregates are related to their component categories via a set of association
tables defined in pudl.metadata.dfs. For example, the “all_coal” fuel aggregate is
linked to all the coal-related energy_source_code values: BIT, SUB, LIG, and WC. Similar
relationships are defined for aggregates over fuel, sector, geography, and time.

## Functions

| [`_extract_keys_from_series_id`](#pudl.transform.eiaapi._extract_keys_from_series_id)(→ pandas.DataFrame)           | Parse primary key codes from EIA series_id.    |
|---------------------------------------------------------------------------------------------------------------------|------------------------------------------------|
| [`_map_key_codes_to_readable_values`](#pudl.transform.eiaapi._map_key_codes_to_readable_values)(→ pandas.DataFrame) |                                                |
| [`_transform_timeseries`](#pudl.transform.eiaapi._transform_timeseries)(→ pandas.DataFrame)                         | Transform raw timeseries.                      |
| [`transform`](#pudl.transform.eiaapi.transform)(→ pandas.DataFrame)                                                 | Transform raw EIA bulk electricity aggregates. |

## Module Contents

### pudl.transform.eiaapi.\_extract_keys_from_series_id(raw_df: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Parse primary key codes from EIA series_id.

These codes comprise the compound primary key that uniquely identifies a data
series: (metric, fuel, region, sector, frequency).

### pudl.transform.eiaapi.\_map_key_codes_to_readable_values(compound_keys: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

### pudl.transform.eiaapi.\_transform_timeseries(raw_ts: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform raw timeseries.

Transform to tidy format and replace the obscure series_id with a readable
compound primary key.

* **Returns:**
  A dataframe with compound key (“fuel_agg”, “geo_agg”, “sector_agg”,
  “temporal_agg”, “report_date”) and two value columns: “fuel_received_mmbtu”,
  “fuel_cost_per_mmbtu”

### pudl.transform.eiaapi.transform(raw_dfs: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)]) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Transform raw EIA bulk electricity aggregates.

* **Parameters:**
  **raw_dfs** – raw timeseries dataframe
* **Returns:**
  (“fuel_agg”, “geo_agg”, “sector_agg”, “temporal_agg”, “report_date”)
  and two value columns: “fuel_received_mmbtu”, “fuel_cost_per_mmbtu”
* **Return type:**
  Transformed timeseries dataframe with compound key
