"""Dagster resources for PUDL.

This module defines the configurable resources that PUDL assets depend on at runtime,
such as data configuration, datastore access, and other run-scoped helpers, along with
the default resource mapping used by the assembled code location. Add
:class:`dagster.ConfigurableResource` classes and configured singleton instances here
when they provide external services or shared runtime context to assets and jobs. Keep
asset logic out of this module; it should focus on dependency injection and default
resource wiring.

For the underlying Dagster concept, see
https://docs.dagster.io/guides/build/external-resources
"""

import os
from collections.abc import Iterator, Mapping
from typing import Any

import dagster as dg
from upath import UPath

from pudl.settings import GlobalDataConfig, load_global_data_config
from pudl.workspace.datastore import Datastore, ZenodoDoiSettings
from pudl.workspace.setup import PudlPaths


class FercXbrlRuntimeSettings(dg.ConfigurableResource):
    """Encodes runtime settings for the ferc_to_sqlite graphs."""

    xbrl_num_workers: None | int = None
    xbrl_batch_size: int = 50
    xbrl_loglevel: str = "INFO"


class GlobalDataConfigResource(dg.ConfigurableResource):
    """Load validated PUDL data configuration from a shared ETL YAML file."""

    global_data_config_path: str

    def create_resource(self, context) -> GlobalDataConfig:
        """Create runtime data configuration from the configured YAML file."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        return load_global_data_config(self.global_data_config_path)


class ZenodoDoiSettingsResource(dg.ConfigurableResource):
    """Load the canonical Zenodo DOI settings for Dagster-managed runs."""

    zenodo_dois_path: str | None = None

    def create_resource(self, context) -> ZenodoDoiSettings:
        """Create runtime DOI settings, optionally from an override YAML file."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        if self.zenodo_dois_path is None:
            return ZenodoDoiSettings()
        return ZenodoDoiSettings.from_yaml(self.zenodo_dois_path)


class DatastoreResource(dg.ConfigurableResource):
    """Dagster resource to interact with Zenodo archives."""

    zenodo_dois: dg.ResourceDependency[ZenodoDoiSettingsResource]
    cloud_cache_path: str = "s3://pudl.catalyst.coop/zenodo"
    use_local_cache: bool = True

    def create_resource(self, context) -> Datastore:
        """Create a configured datastore runtime object."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        ds_kwargs = {
            "cloud_cache_path": self.cloud_cache_path,
            "zenodo_dois": self.zenodo_dois,
        }

        if self.use_local_cache:
            ds_kwargs["local_cache_path"] = PudlPaths().input_dir  # type: ignore[call-arg]
        return Datastore(**ds_kwargs)


class FercEqrDataConfig(dg.ConfigurableResource):
    """Configure which archived FERC EQR filings are available for extraction."""

    ferceqr_archive_uri: str = "gs://archives.catalyst.coop/ferceqr/published"

    @property
    def ferceqr_archive_path(self) -> UPath:
        """Return UPath pointing to archive base path."""
        return UPath(self.ferceqr_archive_uri)


global_data_config_resource = GlobalDataConfigResource.configure_at_launch()
zenodo_doi_settings_resource = ZenodoDoiSettingsResource()
datastore_resource = DatastoreResource(zenodo_dois=zenodo_doi_settings_resource)
ferc_xbrl_runtime_settings = FercXbrlRuntimeSettings()
ferceqr_data_config = FercEqrDataConfig()
ferceqr_archive_uri = os.getenv(
    "FERCEQR_ARCHIVE_PATH", "gs://archives.catalyst.coop/ferceqr/published"
)


class _LazyDefaultResources(Mapping[str, Any]):
    """Lazily assemble the default Dagster resource mapping."""

    def __init__(self):
        self._resources: dict[str, Any] | None = None

    def _materialize(self) -> dict[str, Any]:
        if self._resources is None:
            from pudl.dagster.io_managers import (
                ferc1_dbf_sqlite_io_manager,
                ferc1_xbrl_sqlite_io_manager,
                ferc714_xbrl_sqlite_io_manager,
                geoparquet_io_manager,
                parquet_io_manager,
                pudl_mixed_format_io_manager,
            )

            self._resources = {
                "datastore": datastore_resource,
                "zenodo_dois": zenodo_doi_settings_resource,
                "pudl_io_manager": pudl_mixed_format_io_manager,
                "ferc1_dbf_sqlite_io_manager": ferc1_dbf_sqlite_io_manager,
                "ferc1_xbrl_sqlite_io_manager": ferc1_xbrl_sqlite_io_manager,
                "ferc714_xbrl_sqlite_io_manager": ferc714_xbrl_sqlite_io_manager,
                "global_data_config": global_data_config_resource,
                "runtime_settings": ferc_xbrl_runtime_settings,
                "parquet_io_manager": parquet_io_manager,
                "geoparquet_io_manager": geoparquet_io_manager,
                "ferceqr_data_config": ferceqr_data_config,
            }
        return self._resources

    def __getitem__(self, key: str) -> Any:
        return self._materialize()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._materialize())

    def __len__(self) -> int:
        return len(self._materialize())


default_resources = _LazyDefaultResources()

__all__ = [
    "DatastoreResource",
    "FercEqrDataConfig",
    "FercXbrlRuntimeSettings",
    "GlobalDataConfigResource",
    "ZenodoDoiSettingsResource",
    "default_resources",
    "datastore_resource",
    "ferceqr_data_config",
    "ferc_xbrl_runtime_settings",
    "global_data_config_resource",
    "zenodo_doi_settings_resource",
]
