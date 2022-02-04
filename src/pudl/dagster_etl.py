"""Dagster version of EPA CEMS ETL."""
from pathlib import Path

from dagster import job, op, resource

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


@resource(config_schema={"gcs_cache_path": str, "local_cache_path": str})
def datastore(init_context):
    """Datastore resource. This can be configured in the dagit UI."""
    gcs_cache_path = init_context.resource_config["gcs_cache_path"]
    local_cache_path = init_context.resource_config["local_cache_path"]
    return Datastore(gcs_cache_path=gcs_cache_path, local_cache_path=local_cache_path)


@job(resource_defs={"pudl_settings": pudl_settings, "datastore": datastore})
def etl_epacems_dagster():
    """Run the full EPA CEMS ETL."""
    epacems_raw_dfs = pudl.extract.epacems.extract()
    epacems_transformed_dfs = epacems_raw_dfs.map(pudl.transform.epacems.transform)
    epacems_transformed_dfs.map(load_epacems)
