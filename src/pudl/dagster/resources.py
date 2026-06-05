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

import json
import os
from typing import Any

import dagster as dg
import requests
from upath import UPath

from pudl import PUDL_SETTINGS_PATH, logging_helpers
from pudl.settings import GlobalDataConfig
from pudl.workspace.datastore import Datastore, ZenodoDoiSettings
from pudl.workspace.setup import PudlPaths

logger = logging_helpers.get_logger(__name__)


class PudlPathsResource(dg.ConfigurableResource):
    """Load the input/output paths used by Dagster-managed PUDL runs.

    Explicit Dagster resource config takes precedence. Any unset field falls back to
    the current process environment so `dg` runs, local `.env` files, test fixtures,
    and container-provided environment variables all share a single typed entry point.
    """

    pudl_input: str = dg.EnvVar("PUDL_INPUT")
    pudl_output: str = dg.EnvVar("PUDL_OUTPUT")

    def create_resource(self, context) -> PudlPaths:
        """Create validated runtime path settings for the current Dagster run."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        return PudlPaths(pudl_input=self.pudl_input, pudl_output=self.pudl_output)


class FercXbrlRuntimeSettings(dg.ConfigurableResource):
    """Encodes runtime settings for the ferc_to_sqlite graphs."""

    xbrl_num_workers: None | int = None
    xbrl_batch_size: int = 50
    xbrl_loglevel: str = "INFO"


class GlobalDataConfigResource(dg.ConfigurableResource):
    """Load validated PUDL data configuration from a shared ETL YAML file."""

    global_data_config_path: str = str(PUDL_SETTINGS_PATH / "etl_full.yml")

    def create_resource(self, context) -> GlobalDataConfig:
        """Create runtime data configuration from the configured YAML file."""
        del context  # Required by Dagster's hook signature; intentionally unused here.
        return GlobalDataConfig.from_yaml(self.global_data_config_path)


class ZenodoDoiSettingsResource(dg.ConfigurableResource):
    """Load the canonical Zenodo DOI settings for Dagster-managed runs.

    Two configuration paths are supported:

    * **Inline defaults** (``zenodo_dois_path=None``): uses the canonical Zenodo DOIs
      that are hardcoded as defaults in :class:`~pudl.workspace.datastore.ZenodoDoiSettings`.
      This is the normal production path — no extra config file is needed.
    * **Path override** (``zenodo_dois_path="..."``): loads DOIs from an external YAML
      file, allowing deployments or tests to substitute different DOIs without modifying
      the source code.
    """

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
    pudl_paths: dg.ResourceDependency[PudlPathsResource]
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
            ds_kwargs["local_cache_path"] = self.pudl_paths.pudl_input
        return Datastore(**ds_kwargs)


class FercEqrDataConfig(dg.ConfigurableResource):
    """Configure which archived FERC EQR filings are available for extraction.

    The default value of ``ferceqr_archive_uri`` points to the published archive of FERC
    EQR filings on GCS which is what we use in production. For testing or development,
    this can be overridden to point to a local path with a subset of the archive.
    """

    ferceqr_archive_uri: str = os.getenv(
        "FERCEQR_ARCHIVE_PATH", default="gs://archives.catalyst.coop/ferceqr/published"
    )

    @property
    def ferceqr_archive_path(self) -> UPath:
        """Return UPath pointing to archive base path."""
        return UPath(self.ferceqr_archive_uri)


class ZulipNotificationResource(dg.ConfigurableResource):
    """Send notifications to Zulip streams via the Zulip API."""

    base_url: str = "https://catalyst-cooperative.zulipchat.com"
    bot_email: str = "build-status-bot@catalyst-cooperative.zulipchat.com"
    api_key: str = dg.EnvVar("ZULIP_API_KEY")
    timeout_seconds: int = 30

    def send_stream_message(self, *, stream: str, topic: str, content: str) -> dict:
        """Send a message to a Zulip stream topic and return the API response.

        Sends are best-effort: all failures are logged as warnings and returned
        in the result dict so callers can inspect them, but no exception is
        raised. This ensures a notification hiccup never crashes an asset.
        """
        try:
            response = requests.post(
                f"{self.base_url}/api/v1/messages",
                auth=(self.bot_email, self.api_key),
                data={
                    "type": "stream",
                    "to": stream,
                    "topic": topic,
                    "content": content,
                },
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Zulip notification failed (request error): {e}")
            return {"result": "error", "msg": str(e)}

        try:
            payload = response.json()
        except json.JSONDecodeError as e:
            logger.warning(
                f"Zulip returned 200 with invalid JSON body: {e}. "
                f"Message was almost certainly delivered."
            )
            return {"result": "error", "msg": f"invalid JSON: {e}"}

        if payload.get("result") != "success":
            logger.warning(f"Zulip notification returned error payload: {payload}")
        return payload


global_data_config_resource = GlobalDataConfigResource.configure_at_launch()
pudl_paths_resource = PudlPathsResource.configure_at_launch()
zenodo_doi_settings_resource = ZenodoDoiSettingsResource()
datastore_resource = DatastoreResource(
    zenodo_dois=zenodo_doi_settings_resource,
    pudl_paths=pudl_paths_resource,
)
ferc_xbrl_runtime_settings = FercXbrlRuntimeSettings()
ferceqr_data_config = FercEqrDataConfig()
zulip_notification_resource = ZulipNotificationResource()

default_resources: dict[str, Any] = {
    "datastore": datastore_resource,
    "global_data_config": global_data_config_resource,
    "pudl_paths": pudl_paths_resource,
    "ferceqr_data_config": ferceqr_data_config,
    "runtime_settings": ferc_xbrl_runtime_settings,
    "zenodo_dois": zenodo_doi_settings_resource,
    "zulip_notifications": zulip_notification_resource,
}

__all__ = [
    "DatastoreResource",
    "FercEqrDataConfig",
    "FercXbrlRuntimeSettings",
    "GlobalDataConfigResource",
    "PudlPathsResource",
    "ZulipNotificationResource",
    "ZenodoDoiSettingsResource",
    "datastore_resource",
    "default_resources",
    "ferceqr_data_config",
    "ferc_xbrl_runtime_settings",
    "global_data_config_resource",
    "pudl_paths_resource",
    "zulip_notification_resource",
    "zenodo_doi_settings_resource",
]
