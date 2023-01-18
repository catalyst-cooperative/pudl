"""Dagster repositories for PUDL."""
from dagster import Definitions, define_asset_job, load_assets_from_modules

import pudl
from pudl.etl import create_glue_tables, fuel_receipts_costs_aggs_eia
from pudl.io_managers import (
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    pudl_sqlite_io_manager,
)
from pudl.settings import dataset_settings, ferc_to_sqlite_settings
from pudl.workspace.datastore import datastore

logger = pudl.logging_helpers.get_logger(__name__)


defs = Definitions(
    assets=[
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
