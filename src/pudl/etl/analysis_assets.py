"""Derived / analysis assets that aren't simple to construct.

This is really too large & generic of a category. Should we have an asset group for each
set of related analyses? E.g.

* mcoe_assets
* service_territory_assets
* heat_rate_assets
* state_demand_assets
* depreciation_assets
* plant_parts_eia_assets
* ferc1_eia_record_linkage_assets

Not sure what the right organization is but they'll be defined across a bunch of
different modules. Eventually I imagine these would just be the novel derived values,
probably in pretty skinny tables, which get joined / aggregated with other data in the
denormalized tables.
"""
import pandas as pd
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def utility_analysis(utils_eia860: pd.DataFrame) -> pd.DataFrame:
    """Example of how to create an analysis table that depends on an output view.

    This final dataframe will be written to the database (without a schema).
    """
    # Do some analysis on utils_eia860
    return utils_eia860
