"""Collection of Dagster resources for PUDL."""
import os

from dagster import Field, resource

from pudl.settings import DatasetsSettings, FercToSqliteSettings, create_dagster_config
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths, set_path_overrides


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
        "PUDL_INPUT": Field(
            str,
            description="Path to the input directory.",
            default_value="",
            is_required=False,
        ),
        "PUDL_OUTPUT": Field(
            str,
            description="Path to the output directory.",
            default_value="",
            is_required=False,
        ),
    },
)
def pudl_paths(init_context) -> PudlPaths:
    """Dagster resource for obtaining PudlPaths instance.

    Paths can be overriden when non-empty configuration fields are set. Default values
    are pulled from PUDL_INPUT and PUDL_OUTPUT env variables.
    """
    if init_context.resource_config["PUDL_INPUT"]:
        set_path_overrides(intput_dir=init_context.resource_config["PUDL_INPUT"])
    elif not os.getenv("PUDL_INPUT"):
        raise ValueError("PUDL_INPUT env variable is not set")

    if init_context.resource_config["PUDL_OUTPUT"]:
        set_path_overrides(output_dir=init_context.resource_config["PUDL_OUTPUT"])
    elif not os.getenv("PUDL_OUTPUT"):
        raise ValueError("PUDL_OUTPUT env variable is not set")

    return PudlPaths()


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
        "sandbox": Field(
            bool,
            description="Use the Zenodo sandbox rather than production",
            default_value=False,
        ),
    },
    required_resource_keys={"pudl_paths"},
)
def datastore(init_context) -> Datastore:
    """Dagster resource to interact with Zenodo archives."""
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]
    ds_kwargs["sandbox"] = init_context.resource_config["sandbox"]

    if init_context.resource_config["use_local_cache"]:
        # TODO(rousik): we could also just use PudlPaths().input_dir here, because
        # it should be initialized to the right values.
        ds_kwargs["local_cache_path"] = init_context.resources.pudl_paths.input_dir
    return Datastore(**ds_kwargs)
