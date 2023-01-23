"""Dagster repositories for PUDL."""
from dagster import Definitions, define_asset_job, load_assets_from_modules

import pudl
from pudl.io_managers import (
    epacems_io_manager,
    ferc1_dbf_sqlite_io_manager,
    ferc1_xbrl_sqlite_io_manager,
    pudl_sqlite_io_manager,
)
from pudl.settings import dataset_settings, ferc_to_sqlite_settings
from pudl.workspace.datastore import datastore

from . import (  # noqa: F401
    eia_api_assets,
    epacems_assets,
    glue_assets,
    output_assets,
    static_assets,
)

logger = pudl.logging_helpers.get_logger(__name__)

assets = (
    *load_assets_from_modules([output_assets], group_name="output_tables"),
    *load_assets_from_modules([epacems_assets], group_name="epacems"),
    *load_assets_from_modules([eia_api_assets], group_name="eia_api"),
    *load_assets_from_modules([glue_assets], group_name="glue"),
    *load_assets_from_modules([pudl.extract.eia], group_name="eia_raw_assets"),
    *load_assets_from_modules([pudl.transform.eia], group_name="eia_harvested_assets"),
    *load_assets_from_modules([static_assets], group_name="static_assets"),
    *load_assets_from_modules([pudl.extract.ferc1], group_name="ferc_assets"),
    *load_assets_from_modules([pudl.transform.ferc1], group_name="ferc_assets"),
)

defs = Definitions(
    assets=assets,
    resources={
        "datastore": datastore,
        "pudl_sqlite_io_manager": pudl_sqlite_io_manager,
        "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
        "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
        "dataset_settings": dataset_settings,
        "ferc_to_sqlite_settings": ferc_to_sqlite_settings,
        "epacems_io_manager": epacems_io_manager,
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
