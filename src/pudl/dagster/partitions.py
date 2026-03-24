"""Dagster partition definitions for PUDL.

This module is the shared home for reusable partition definitions that multiple assets,
asset checks, sensors, or jobs need to reference consistently. Define partition objects
here when they represent orchestration-time slicing of the workload, such as a fixed
set of reporting periods, rather than resource configuration or dataset metadata.

For the underlying Dagster concept, see
https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets
"""

from dagster import StaticPartitionsDefinition

from pudl.metadata.classes import DataSource

ferceqr_year_quarters: StaticPartitionsDefinition = StaticPartitionsDefinition(
    DataSource.from_id("ferceqr").working_partitions["year_quarters"]
)

__all__ = ["ferceqr_year_quarters"]
