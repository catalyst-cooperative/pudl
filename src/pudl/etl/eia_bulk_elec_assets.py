"""EIA Bulk Electricity Aggregate assets."""

from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(
    io_manager_key="pudl_io_manager",
    required_resource_keys={"datastore"},
)
def core_eia__yearly_fuel_receipts_costs_aggs(context):
    """Extract and transform EIA bulk electricity aggregates.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.
    """
    logger.info("Processing EIA bulk electricity aggregates.")
    ds = context.resources.datastore
    raw_bulk_dfs = pudl.extract.eiaapi.extract(ds)
    return pudl.transform.eiaapi.transform(raw_bulk_dfs)
