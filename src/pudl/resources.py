"""Collection of Dagster resources for PUDL."""

from dagster import ConfigurableResource

from pudl.settings import (
    EtlSettings,
    load_etl_settings,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


class RuntimeSettings(ConfigurableResource):
    """Encodes runtime settings for the ferc_to_sqlite graphs."""

    xbrl_num_workers: None | int = None
    xbrl_batch_size: int = 50
    xbrl_loglevel: str = "INFO"


class PudlEtlSettingsResource(ConfigurableResource):
    """Load validated PUDL ETL settings from a shared ETL YAML file."""

    etl_settings_path: str

    def create_resource(self, context) -> EtlSettings:
        """Create runtime ETL settings from the configured ETL settings file."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        return load_etl_settings(self.etl_settings_path)


class DatastoreResource(ConfigurableResource):
    """Dagster resource to interact with Zenodo archives."""

    cloud_cache_path: str = "s3://pudl.catalyst.coop/zenodo"
    use_local_cache: bool = True

    def create_resource(self, context) -> Datastore:
        """Create a configured datastore runtime object."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        ds_kwargs = {"cloud_cache_path": self.cloud_cache_path}

        if self.use_local_cache:
            ds_kwargs["local_cache_path"] = PudlPaths().input_dir  # type: ignore[call-arg]
        return Datastore(**ds_kwargs)


etl_settings = PudlEtlSettingsResource.configure_at_launch()
datastore = DatastoreResource()
