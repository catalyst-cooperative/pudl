"""Dagster assets for EIA API electricity aggregates.

This module defines asset logic for the aggregate EIA electricity data products that
PUDL derives from the archived EIA API electricity JSON data and then loads into the
core asset graph.
"""

import dagster as dg

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@dg.asset(
    io_manager_key="pudl_io_manager",
    required_resource_keys={"datastore"},
)
def core_eia__yearly_fuel_receipts_costs_aggs(context):
    """Extract and transform EIA API electricity aggregates.

    Returns:
        A dictionary of DataFrames whose keys are the names of the corresponding
        database table.
    """
    logger.info("Processing EIA API electricity aggregates.")
    ds = context.resources.datastore
    raw_eiaapi_dfs = pudl.extract.eiaapi.extract(ds)
    return pudl.transform.eiaapi.transform(raw_eiaapi_dfs)
