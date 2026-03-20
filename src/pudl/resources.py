"""Collection of Dagster resources for PUDL."""

from dagster import ConfigurableResource

from pudl.settings import (
    DatasetsSettings,
    FercToSqliteSettings,
    load_etl_settings,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


class RuntimeSettings(ConfigurableResource):
    """Encodes runtime settings for the ferc_to_sqlite graphs."""

    xbrl_num_workers: None | int = None
    xbrl_batch_size: int = 50
    xbrl_loglevel: str = "INFO"


class DatasetSettingsResource(ConfigurableResource):
    """Load dataset settings for the ETL from a shared ETL settings file."""

    etl_settings_path: str

    def create_resource(self, context) -> DatasetsSettings:
        """Create runtime dataset settings from the configured ETL settings file."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        settings = load_etl_settings(self.etl_settings_path)
        if settings.datasets is None:
            raise ValueError("Missing datasets settings in ETL settings file.")
        return settings.datasets


class FercToSqliteSettingsResource(ConfigurableResource):
    """Load FERC-to-SQLite settings from a shared ETL settings file."""

    etl_settings_path: str

    def create_resource(self, context) -> FercToSqliteSettings:
        """Create runtime FERC-to-SQLite settings from the ETL settings file."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        settings = load_etl_settings(self.etl_settings_path)
        if settings.ferc_to_sqlite_settings is None:
            raise ValueError("Missing ferc_to_sqlite settings in ETL settings file.")
        return settings.ferc_to_sqlite_settings


class DatastoreResource(ConfigurableResource):
    """Dagster resource to interact with Zenodo archives."""

    cloud_cache_path: str = "s3://pudl.catalyst.coop/zenodo"
    use_local_cache: bool = True

    def create_resource(self, context) -> Datastore:
        """Create a configured datastore runtime object."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        ds_kwargs = {"cloud_cache_path": self.cloud_cache_path}

        if self.use_local_cache:
            # TODO(rousik): we could also just use PudlPaths().input_dir here, because
            # it should be initialized to the right values.
            ds_kwargs["local_cache_path"] = PudlPaths().input_dir  # type: ignore[call-arg]
        return Datastore(**ds_kwargs)


dataset_settings = DatasetSettingsResource.configure_at_launch()
ferc_to_sqlite_settings = FercToSqliteSettingsResource.configure_at_launch()
datastore = DatastoreResource()
