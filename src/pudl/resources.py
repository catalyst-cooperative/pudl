"""Collection of Dagster resources for PUDL."""
from dagster import Field, resource
from pathlib import Path

import pyarrow.parquet as pq

from pudl.helpers import EnvVar
from pudl.settings import DatasetsSettings, FercToSqliteSettings, create_dagster_config
from pudl.workspace.datastore import Datastore
from pudl.metadata.classes import Resource


@resource(config_schema=create_dagster_config(DatasetsSettings()))
def dataset_settings(init_context) -> DatasetsSettings:
    """Dagster resource for parameterizing PUDL ETL assets.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.
    """
    return DatasetsSettings(**init_context.resource_config)


@resource(config_schema=create_dagster_config(FercToSqliteSettings()))
def ferc_to_sqlite_settings(init_context) -> FercToSqliteSettings:
    """Dagster resource for parameterizing the ``ferc_to_sqlite`` graph.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.
    """
    return FercToSqliteSettings(**init_context.resource_config)


@resource(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
    }
)
def pq_writer(init_context) -> pq.ParquetWriter:
    """Get monolithic parquet writer."""
    monolithic_path = Path(init_context.resource_config["pudl_output_path"]) / "hourly_emissions_epacems.parquet"
    schema = Resource.from_id("hourly_emissions_epacems").to_pyarrow()

    with pq.ParquetWriter(
        where=monolithic_path, schema=schema, compression="snappy", version="2.6"
    ) as monolithic_writer:
        yield monolithic_writer


@resource(
    config_schema={
        "local_cache_path": Field(
            EnvVar(
                env_var="PUDL_INPUT",
            ),
            description="Path to local cache of raw data.",
            default_value=None,
        ),
        "gcs_cache_path": Field(
            str,
            description="Load datastore resources from Google Cloud Storage.",
            default_value="",
        ),
        "use_local_cache": Field(
            bool,
            description="If enabled, the local file cache for datastore will be used.",
            default_value=True,
        ),
        "sandbox": Field(
            bool,
            description="Use the Zenodo sandbox rather than production",
            default_value=False,
        ),
    },
)
def datastore(init_context) -> Datastore:
    """Dagster resource to interact with Zenodo archives."""
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]
    ds_kwargs["sandbox"] = init_context.resource_config["sandbox"]

    if init_context.resource_config["use_local_cache"]:
        ds_kwargs["local_cache_path"] = init_context.resource_config["local_cache_path"]
    return Datastore(**ds_kwargs)
