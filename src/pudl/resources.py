"""Collection of Dagster resources for PUDL."""

from dagster import ConfigurableResource, Field, resource

from pudl.settings import (
    DatasetsSettings,
    FercToSqliteSettings,
    create_dagster_config,
    load_etl_settings,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


class RuntimeSettings(ConfigurableResource):
    """Encodes runtime settings for the ferc_to_sqlite graphs."""

    xbrl_num_workers: None | int = None
    xbrl_batch_size: int = 50
    xbrl_loglevel: str = "INFO"


@resource(
    config_schema={
        "etl_settings_path": Field(
            str,
            description=(
                "Optional path to an ETL settings YAML file. If provided, "
                "datasets settings are loaded from this file."
            ),
            is_required=False,
        ),
        **create_dagster_config(DatasetsSettings()),
    }
)
def dataset_settings(init_context) -> DatasetsSettings:
    """Dagster resource for parameterizing PUDL ETL assets.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.
    """
    etl_settings_path = init_context.resource_config.get("etl_settings_path")
    if etl_settings_path:
        settings = load_etl_settings(etl_settings_path)
        if settings.datasets is None:
            raise ValueError("Missing datasets settings in ETL settings file.")
        return settings.datasets

    # Strip helper-only config key before constructing settings directly.
    resource_config = {
        key: value
        for key, value in init_context.resource_config.items()
        if key != "etl_settings_path"
    }
    return DatasetsSettings(**resource_config)


@resource(
    config_schema={
        "etl_settings_path": Field(
            str,
            description=(
                "Optional path to an ETL settings YAML file. If provided, "
                "ferc_to_sqlite settings are loaded from this file."
            ),
            is_required=False,
        ),
        **create_dagster_config(FercToSqliteSettings()),
    }
)
def ferc_to_sqlite_settings(init_context) -> FercToSqliteSettings:
    """Dagster resource for parameterizing the ``ferc_to_sqlite`` graph.

    This resource allows us to specify the years we want to process for each datasource
    in the Dagit UI.
    """
    etl_settings_path = init_context.resource_config.get("etl_settings_path")
    if etl_settings_path:
        settings = load_etl_settings(etl_settings_path)
        if settings.ferc_to_sqlite_settings is None:
            raise ValueError("Missing ferc_to_sqlite settings in ETL settings file.")
        return settings.ferc_to_sqlite_settings

    # Strip helper-only config key before constructing settings directly.
    resource_config = {
        key: value
        for key, value in init_context.resource_config.items()
        if key != "etl_settings_path"
    }
    return FercToSqliteSettings(**resource_config)


@resource(
    config_schema={
        "cloud_cache_path": Field(
            str,
            description="Load datastore resources from this GCS or S3 path.",
            default_value="s3://pudl.catalyst.coop/zenodo",
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
    ds_kwargs["cloud_cache_path"] = init_context.resource_config["cloud_cache_path"]

    if init_context.resource_config["use_local_cache"]:
        # TODO(rousik): we could also just use PudlPaths().input_dir here, because
        # it should be initialized to the right values.
        ds_kwargs["local_cache_path"] = PudlPaths().input_dir  # type: ignore[call-arg]
    return Datastore(**ds_kwargs)
