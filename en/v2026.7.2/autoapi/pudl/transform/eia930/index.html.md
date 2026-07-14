# pudl.transform.eia930

Module to perform data cleaning functions on EIA930 data tables.

## Attributes

| [`logger`](#pudl.transform.eia930.logger)   |    |
|---------------------------------------------|----|

## Functions

| [`_transform_netgen_by_source`](#pudl.transform.eia930._transform_netgen_by_source)(→ pudl.helpers.ParquetData)               | Transform the eia930 netgen by source table.                             |
|-------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`_transform_hourly_operations`](#pudl.transform.eia930._transform_hourly_operations)(→ pudl.helpers.ParquetData)             | Transform the eia930 hourly operations table.                            |
| [`core_eia930__hourly_operations_assets`](#pudl.transform.eia930.core_eia930__hourly_operations_assets)(raw_eia930_\_balance) | Separate raw_eia930_\_balance into net generation and demand tables.     |
| [`core_eia930__hourly_subregion_demand`](#pudl.transform.eia930.core_eia930__hourly_subregion_demand)(raw_eia930_\_subregion) | Produce a normalized table of hourly electricity demand by BA subregion. |
| [`core_eia930__hourly_interchange`](#pudl.transform.eia930.core_eia930__hourly_interchange)(raw_eia930_\_interchange)         | Produce a normalized table of hourly interchange by balancing authority. |

## Module Contents

### pudl.transform.eia930.logger

### pudl.transform.eia930.\_transform_netgen_by_source(table: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Transform the eia930 netgen by source table.

### pudl.transform.eia930.\_transform_hourly_operations(table: [duckdb.DuckDBPyRelation](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyRelation), conn: [duckdb.DuckDBPyConnection](https://duckdb.org/docs/lts/clients/python/reference/index.html#duckdb.DuckDBPyConnection)) → [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData)

Transform the eia930 hourly operations table.

### pudl.transform.eia930.core_eia930_\_hourly_operations_assets(raw_eia930_\_balance: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Separate raw_eia930_\_balance into net generation and demand tables.

Energy source starts out in the column names, but is stacked into a categorical
column. For structural purposes “interchange” is also treated as an “energy source”
and stacked into the same column. For the moment “total” (sum of all energy sources)
is also included, because the reported and calculated totals across all energy
sources have significant differences which should be further explored.

### pudl.transform.eia930.core_eia930_\_hourly_subregion_demand(raw_eia930_\_subregion: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Produce a normalized table of hourly electricity demand by BA subregion.

### pudl.transform.eia930.core_eia930_\_hourly_interchange(raw_eia930_\_interchange: [pudl.helpers.ParquetData](../../helpers/index.md#pudl.helpers.ParquetData))

Produce a normalized table of hourly interchange by balancing authority.
