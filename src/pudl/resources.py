"""Collection of Dagster resources for PUDL."""
from pathlib import Path

from dagster import Field, resource

import pudl
from pudl.settings import DatasetsSettings, FercToSqliteSettings, create_dagster_config
from pudl.workspace.datastore import Datastore


@resource(config_schema=create_dagster_config(DatasetsSettings()))
def dataset_settings(init_context):
    """Dagster resource for parameterizing assets.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.

    We configure the assets using a resource instead of op configs so the settings can
    be accesible by any op.
    """
    return DatasetsSettings(**init_context.resource_config)


@resource(config_schema=create_dagster_config(FercToSqliteSettings()))
def ferc_to_sqlite_settings(init_context):
    """Dagster resource for parameterizing assets.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.

    We configure the assets using a resource instead of op configs so the settings can
    be accesible by any op.
    """
    return FercToSqliteSettings(**init_context.resource_config)


@resource(
    config_schema={
        "pudl_out": Field(
            str, default_value=pudl.workspace.setup.get_defaults()["pudl_out"]
        ),
        "pudl_in": Field(
            str, default_value=pudl.workspace.setup.get_defaults()["pudl_in"]
        ),
    }
)
def pudl_settings(init_context) -> dict[str, str]:
    """PUDL settings, mostly paths for inputs & outputs."""
    pudl_out = init_context.resource_config["pudl_out"]
    pudl_in = init_context.resource_config["pudl_in"]
    return pudl.workspace.setup.derive_paths(pudl_in=pudl_in, pudl_out=pudl_out)


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
    required_resource_keys={"pudl_settings"},
)
def datastore(init_context):
    """Datastore resource.

    This can be configured in the dagit UI.
    """
    ds_kwargs = {}
    ds_kwargs["gcs_cache_path"] = init_context.resource_config["gcs_cache_path"]
    ds_kwargs["sandbox"] = init_context.resource_config["sandbox"]

    if init_context.resource_config["use_local_cache"]:
        pudl_settings = init_context.resources.pudl_settings
        ds_kwargs["local_cache_path"] = Path(pudl_settings["pudl_in"]) / "data"
    return Datastore(**ds_kwargs)
