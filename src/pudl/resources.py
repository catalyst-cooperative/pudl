"""Collection of Dagster resources for PUDL."""

from dagster import Field, resource

from pudl.settings import DatasetsSettings, FercToSqliteSettings, create_dagster_config
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


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
    },
)
def datastore(init_context) -> Datastore:
    """Dagster resource to interact with Zenodo archives."""
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]

    if init_context.resource_config["use_local_cache"]:
        # TODO(rousik): we could also just use PudlPaths().input_dir here, because
        # it should be initialized to the right values.
        ds_kwargs["local_cache_path"] = PudlPaths().input_dir
    return Datastore(**ds_kwargs)
