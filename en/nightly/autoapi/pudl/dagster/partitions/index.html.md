# pudl.dagster.partitions

Dagster partition definitions for PUDL.

This module is the shared home for reusable partition definitions that multiple assets,
asset checks, sensors, or jobs need to reference consistently. Define partition objects
here when they represent orchestration-time slicing of the workload, such as a fixed
set of reporting periods, rather than resource configuration or dataset metadata.

For the underlying Dagster concept, see
[https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets)

## Attributes

| [`ferceqr_year_quarters`](#pudl.dagster.partitions.ferceqr_year_quarters)   |    |
|-----------------------------------------------------------------------------|----|

## Module Contents

### pudl.dagster.partitions.ferceqr_year_quarters *: [dagster.StaticPartitionsDefinition](https://docs.dagster.io/api/dagster/partitions/#dagster.StaticPartitionsDefinition)*
