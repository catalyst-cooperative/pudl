"""Run the PUDL ETL Pipeline.

The PUDL project integrates several different public datasets into a well
normalized relational database allowing easier access and interaction between all
datasets. This module coordinates the extract/transfrom/load process for
data from:

 - US Energy Information Agency (EIA):
   - Form 860 (eia860)
   - Form 923 (eia923)
 - US Federal Energy Regulatory Commission (FERC):
   - Form 1 (ferc1)
 - US Environmental Protection Agency (EPA):
   - Continuous Emissions Monitory System (epacems)
"""

from dagster import AssetOut, Output, multi_asset

import pudl
from pudl.metadata.classes import DataSource

logger = pudl.logging_helpers.get_logger(__name__)

# TODO (bendnorman): Add this information to the metadata
eia_raw_table_names = (
    "raw_boiler_fuel_eia923",
    "raw_boiler_generator_assn_eia860",
    "raw_fuel_receipts_costs_eia923",
    "raw_generation_fuel_eia923",
    "raw_generator_eia860",
    "raw_generator_eia923",
    "raw_generator_existing_eia860",
    "raw_generator_proposed_eia860",
    "raw_generator_retired_eia860",
    "raw_ownership_eia860",
    "raw_plant_eia860",
    "raw_stocks_eia923",
    "raw_utility_eia860",
)


# TODO (bendnorman): Figure out type hint for context keyword and mutli_asset return
@multi_asset(
    outs={table_name: AssetOut() for table_name in sorted(eia_raw_table_names)},
    required_resource_keys={"datastore", "dataset_settings"},
)
def extract_eia(context):
    """Extract raw EIA data from excel sheets into dataframes.

    Args:
        context: dagster keyword that provides access to resources and config.

    Returns:
        A tuple of extracted EIA dataframes.
    """
    eia_settings = context.resources.dataset_settings.eia

    ds = context.resources.datastore
    eia923_raw_dfs = pudl.extract.eia923.Extractor(ds).extract(
        year=eia_settings.eia923.years
    )
    eia860_raw_dfs = pudl.extract.eia860.Extractor(ds).extract(
        year=eia_settings.eia860.years
    )

    if eia_settings.eia860.eia860m:
        eia860m_data_source = DataSource.from_id("eia860m")
        eia860m_date = eia860m_data_source.working_partitions["year_month"]
        eia860m_raw_dfs = pudl.extract.eia860m.Extractor(ds).extract(
            year_month=eia860m_date
        )
        eia860_raw_dfs = pudl.extract.eia860m.append_eia860m(
            eia860_raw_dfs=eia860_raw_dfs, eia860m_raw_dfs=eia860m_raw_dfs
        )

    # create descriptive table_names
    eia860_raw_dfs = {
        "raw_" + table_name + "_eia860": df for table_name, df in eia860_raw_dfs.items()
    }
    eia923_raw_dfs = {
        "raw_" + table_name + "_eia923": df for table_name, df in eia923_raw_dfs.items()
    }

    eia_raw_dfs = {}
    eia_raw_dfs.update(eia860_raw_dfs)
    eia_raw_dfs.update(eia923_raw_dfs)
    eia_raw_dfs = dict(sorted(eia_raw_dfs.items()))

    return (
        Output(output_name=table_name, value=df)
        for table_name, df in eia_raw_dfs.items()
    )
