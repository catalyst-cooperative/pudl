"""Dagster repositories for PUDL."""
from functools import partial

import pandas as pd
from dagster import (
    AssetsDefinition,
    Definitions,
    GraphOut,
    Out,
    define_asset_job,
    graph,
    load_assets_from_modules,
    op,
)

import pudl
from pudl.etl import create_glue_tables, fuel_receipts_costs_aggs_eia
from pudl.io_managers import (
    combined_epacems_io_manager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    partitioned_epacems_io_manager,
    pudl_sqlite_io_manager,
)
from pudl.settings import dataset_settings, ferc_to_sqlite_settings
from pudl.workspace.datastore import datastore

logger = pudl.logging_helpers.get_logger(__name__)


@op(out=Out(io_manager_key="combined_epacems_io_manager"))
def merge_dfs(clean_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate EPA CEMS partitions into a single DataFrame."""
    return pd.concat(clean_dfs)


@graph(out={"hourly_emissions_epacems": GraphOut()})
def epacems_etl(epacamd_eia, plants_entity_eia):
    """Process EPA CEMS data."""
    raw_epacems_dfs = pudl.extract.epacems.extract()
    timezones = pudl.transform.epacems.load_plant_utc_offset(plants_entity_eia)

    partial_transform_epacems = partial(
        pudl.transform.epacems.transform, epacamd_eia=epacamd_eia, timezones=timezones
    )

    results = raw_epacems_dfs.map(partial_transform_epacems)
    return merge_dfs(results.collect())


hourly_emissions_epacems = AssetsDefinition.from_graph(
    epacems_etl, group_name="epacems"
)


defs = Definitions(
    assets=[
        hourly_emissions_epacems,
        fuel_receipts_costs_aggs_eia,
        create_glue_tables,
        *load_assets_from_modules([pudl.extract.eia], group_name="eia_raw_assets"),
        *load_assets_from_modules(
            [pudl.transform.eia], group_name="eia_harvested_assets"
        ),
        *load_assets_from_modules([pudl.static_assets], group_name="static_assets"),
        *load_assets_from_modules(
            [pudl.output.output_assets], group_name="output_assets"
        ),
        *load_assets_from_modules([pudl.extract.ferc1], group_name="ferc_assets"),
        *load_assets_from_modules([pudl.transform.ferc1], group_name="ferc_assets"),
    ],
    resources={
        "datastore": datastore,
        "pudl_sqlite_io_manager": pudl_sqlite_io_manager,
        "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
        "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
        "dataset_settings": dataset_settings,
        "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
        "combined_epacems_io_manager": combined_epacems_io_manager,
        "partitioned_epacems_io_manager": partitioned_epacems_io_manager,
    },
    jobs=[
        define_asset_job(name="etl_full"),
        define_asset_job(
            name="etl_fast",
            config={
                "resources": {
                    "dataset_settings": {
                        "config": {
                            "eia": {
                                "eia860": {"years": [2020, 2021], "eia860m": True},
                                "eia923": {"years": [2020, 2021]},
                            },
                            "ferc1": {"years": [2020, 2021]},
                            "epacems": {
                                "states": ["ID", "ME"],
                                "years": [2019, 2020, 2021],
                            },
                        }
                    }
                }
            },
        ),
    ],
)
