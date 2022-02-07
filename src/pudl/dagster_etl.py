"""Dagster version of EPA CEMS ETL."""
from pathlib import Path

import sqlalchemy as sa
from dagster import (AssetMaterialization, EventMetadata, Field, Output, job,
                     op, resource)

import pudl
from pudl.workspace.datastore import Datastore


@op(required_resource_keys={"pudl_settings"})
def load_epacems(context, transformed_df):
    """Load epacems to parquet."""
    root_path = Path(context.resources.pudl_settings["parquet_dir"]) / "epacems"

    pudl.load.df_to_parquet(
        transformed_df,
        resource_id="hourly_emissions_epacems",
        root_path=root_path,
        partition_cols=["year", "state"]
    )

    # Format of the mapping key makes it difficult to recreate the final parquet path.
    yield AssetMaterialization(
        asset_key="hourly_emissions_epacems",
        description="Persisted result to storage",
        partition=context.get_mapping_key(),
        metadata={
            "Description": "Parquet file.",
            "Path": EventMetadata.path(str(root_path)),
            "DataFrame Size (Bytes)": int(transformed_df.memory_usage().sum())
        },
    )
    yield Output(root_path)


@resource
def pudl_settings(init_context):
    """Create a pudl engine Resource."""
    # TODO: figure out how to config the pudl workspace using dagster instead of just pulling the defaults.
    return pudl.workspace.setup.get_defaults()


@resource(required_resource_keys={"pudl_settings"})
def pudl_engine(init_context):
    """Create a pudl engine Resource."""
    return sa.create_engine(init_context.resources.pudl_settings["pudl_db"])


@resource(config_schema={"gcs_cache_path": Field(str, description='Load datastore resources from Google Cloud Storage.', default_value=""), "use_local_cache": Field(bool, description='If enabled, the local file cache for datastore will be used.', default_value=True), }, required_resource_keys={"pudl_settings"})
def datastore(init_context):
    """Datastore resource. This can be configured in the dagit UI."""
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]
    ds_kwargs["sandbox"] = init_context.resources.pudl_settings.get("sandbox", False)

    if init_context.resource_config["use_local_cache"]:
        ds_kwargs["local_cache_path"] = Path(
            init_context.resources.pudl_settings["pudl_in"]) / "data"
    return Datastore(**ds_kwargs)


@job(resource_defs={"pudl_settings": pudl_settings, "datastore": datastore, "pudl_engine": pudl_engine})
def etl_epacems_dagster():
    """Run the full EPA CEMS ETL."""
    partitions = pudl.extract.epacems.gather_partitions()
    epacems_raw_dfs = partitions.map(pudl.extract.epacems.extract)
    epacems_transformed_dfs = epacems_raw_dfs.map(pudl.transform.epacems.transform)
    epacems_transformed_dfs.map(load_epacems)
