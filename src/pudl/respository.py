"""Dagster repositories for PUDL."""
from dagster import (
    define_asset_job,
    load_assets_from_modules,
    repository,
    with_resources,
)

import pudl
from pudl.etl import eia_raw_dfs, static_eia_assets
from pudl.io_managers import pudl_sqlite_io_manager
from pudl.workspace.datastore import datastore


@repository
def pudl_repository():
    """Dagster repository for all PUDL assets."""
    return [
        *with_resources(
            [
                static_eia_assets,
                eia_raw_dfs,
                *load_assets_from_modules(
                    [pudl.transform.eia], group_name="eia_harvested_dfs"
                ),
            ],
            resource_defs={
                "datastore": datastore,
                "pudl_sqlite_io_manager": pudl_sqlite_io_manager,
            },
        ),
        define_asset_job(name="pudl"),
    ]
