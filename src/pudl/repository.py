"""PUDL dagster repositories."""
from dagster import (
    AssetsDefinition,
    asset,
    define_asset_job,
    repository,
    with_resources,
)

from pudl.etl import etl_epacems_job, pudl_engine, pudl_etl, pudl_settings
from pudl.extract.ferc1 import ferc1_to_sqlite
from pudl.io_managers import pudl_sqlite_io_manager
from pudl.workspace.datastore import datastore


@asset(group_name="output_tables", io_manager_key="sqlite_io_manager")
def generators_asset(generators_eia860, generators_entity_eia):
    """Output table test."""
    return generators_eia860


@repository
def pudl():
    """Define a dagster repository to hold pudl jobs."""
    pudl_assets = AssetsDefinition.from_graph(
        pudl_etl,
        resource_defs={
            "pudl_settings": pudl_settings,
            "datastore": datastore,
            "pudl_engine": pudl_engine,
            "sqlite_io_manager": pudl_sqlite_io_manager,
        },
    )
    output_assets = with_resources(
        [generators_asset],
        resource_defs={
            "sqlite_io_manager": pudl_sqlite_io_manager,
        },
    )
    return [
        define_asset_job("graph_asset"),
        pudl_assets,
        output_assets,
        ferc1_to_sqlite,
        etl_epacems_job,
    ]
