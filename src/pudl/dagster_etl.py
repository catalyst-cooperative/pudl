"""Dagster version of EPA CEMS ETL."""
from pathlib import Path

from dagster import Field, job, op, resource

import pudl
from pudl.workspace.datastore import Datastore


@op(required_resource_keys={"pudl_settings"})
def load_epacems(context, transformed_df):
    """Load epacems to parquet."""
    pudl.load.df_to_parquet(
        transformed_df,
        resource_id="hourly_emissions_epacems",
        root_path=Path(context.resources.pudl_settings["parquet_dir"]) / "epacems",
        partition_cols=["year", "state"]
    )


@resource
def pudl_settings(init_context):
    """Create a pudl engine Resource."""
    # TODO: figure out how to config the pudl workspace using dagster instead of just pulling the defaults.
    return pudl.workspace.setup.get_defaults()


@resource(config_schema={"gcs_cache_path": Field(str, description='oad datastore resources from Google Cloud Storage.', default_value=""), "use_local_cache": Field(bool, description='If enabled, the local file cache for datastore will be used.', default_value=True), }, required_resource_keys={"pudl_settings"})
def datastore(init_context):
    """Datastore resource. This can be configured in the dagit UI."""
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]
    ds_kwargs["sandbox"] = init_context.resources.pudl_settings.get("sandbox", False)

    if init_context.resource_config["use_local_cache"]:
        ds_kwargs["local_cache_path"] = Path(
            init_context.resources.pudl_settings["pudl_in"]) / "data"
    return Datastore(**ds_kwargs)


@job(resource_defs={"pudl_settings": pudl_settings, "datastore": datastore})
def etl_epacems_dagster():
    """Run the full EPA CEMS ETL."""
    partitions = pudl.extract.epacems.gather_partitions()
    epacems_raw_dfs = partitions.map(pudl.extract.epacems.extract)
    epacems_transformed_dfs = epacems_raw_dfs.map(pudl.transform.epacems.transform)
    epacems_transformed_dfs.map(load_epacems)
