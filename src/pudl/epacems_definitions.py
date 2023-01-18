"""EPA CEMS Dagster definitions."""
from dagster import (
    AssetKey,
    Definitions,
    MultiPartitionsDefinition,
    SourceAsset,
    StaticPartitionsDefinition,
    asset,
)

import pudl
from pudl.extract.epacems import EpaCemsDatastore, EpaCemsPartition
from pudl.io_managers import epacems_io_manager, pudl_sqlite_io_manager
from pudl.metadata.classes import DataSource
from pudl.workspace.datastore import datastore

data_source = DataSource.from_id("epacems")

epacamd_eia = SourceAsset(
    key=AssetKey("epacamd_eia"), io_manager_key="pudl_sqlite_io_manager"
)
plants_entity_eia = SourceAsset(
    key=AssetKey("plants_entity_eia"), io_manager_key="pudl_sqlite_io_manager"
)


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "year": StaticPartitionsDefinition(
                tuple(str(year) for year in data_source.working_partitions["years"])
            ),
            "state": StaticPartitionsDefinition(
                data_source.working_partitions["states"]
            ),
        }
    ),
    required_resource_keys={"datastore"},
    io_manager_key="epacems_io_manager",
)
def hourly_emissions_epacems(context, epacamd_eia, plants_entity_eia):
    """EPA CEMS asset."""
    ds = context.resources.datastore
    ds = EpaCemsDatastore(ds)

    timezones = pudl.transform.epacems.load_plant_utc_offset(plants_entity_eia)

    partition = EpaCemsPartition(**context.partition_key.keys_by_dimension)
    df = pudl.extract.epacems.extract(partition, ds)
    return pudl.transform.epacems.transform(df, epacamd_eia, timezones)


defs = Definitions(
    assets=[hourly_emissions_epacems, epacamd_eia, plants_entity_eia],
    resources={
        "datastore": datastore,
        "pudl_sqlite_io_manager": pudl_sqlite_io_manager,
        "epacems_io_manager": epacems_io_manager,
    },
)
