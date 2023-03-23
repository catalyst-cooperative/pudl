"""EIA Bulk Electricty Aggregate assets."""
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(
    io_manager_key="pudl_sqlite_io_manager",
    required_resource_keys={"datastore"},
)
def fuel_receipts_costs_aggs_eia(context):
    """Extract and transform EIA bulk electricity aggregates.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.
    """
    logger.info("Processing EIA bulk electricity aggregates.")
    ds = context.resources.datastore
    raw_bulk_dfs = pudl.extract.eia_bulk_elec.extract(ds)
    return pudl.transform.eia_bulk_elec.transform(raw_bulk_dfs)
