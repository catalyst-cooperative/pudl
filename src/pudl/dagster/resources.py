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
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import dagster as dg
import yaml
from pydantic import field_validator
from upath import UPath

from pudl import PUDL_SETTINGS_PATH
from pudl.settings import GlobalDataConfig
from pudl.workspace.datastore import Datastore, ZenodoDoiSettings
from pudl.workspace.setup import PudlPaths

DEFAULT_FERCEQR_DEPLOYMENT_CONFIG_PATH = (
    PUDL_SETTINGS_PATH / "ferceqr_deployment_targets.yml"
)


def validate_local_deployment_directory(local_path: Path) -> None:
    """Validate a local deployment target directory."""
    if not local_path.is_absolute():
        raise ValueError(
            "Local deployment target paths must be absolute filesystem paths."
        )
    if not local_path.exists():
        raise ValueError(f"Local deployment target path {local_path} does not exist.")
    if not local_path.is_dir():
        raise ValueError(
            f"Local deployment target path {local_path} is not a directory."
        )
    if not os.access(local_path, os.W_OK):
        raise ValueError(f"Local deployment target path {local_path} is not writable.")


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


class FercEqrArchiveResource(dg.ConfigurableResource):
    """Configure which archived FERC EQR filings are available for extraction.

    The default value of ``path`` points to the published archive of FERC EQR filings on
    GCS which is what we use in production. For testing or development, this can be
    overridden to point to a local path with a subset of the archive.
    """

    path: str = dg.EnvVar("PUDL_FERCEQR_ARCHIVE_PATH")

    @property
    def upath(self) -> UPath:
        """Return UPath pointing to archive base path."""
        return UPath(self.path)


class FercEqrDeploymentTargetConfig(dg.Config):
    """A single deployment destination for FERC EQR outputs.

    ``path`` is a UPath-compatible string: an absolute local directory path, ``file://``
    URI, ``gs://`` URI, or ``s3://`` URI.  ``storage_options`` is unpacked as
    ``**kwargs`` when constructing the :class:`~upath.UPath`, allowing per-target fsspec
    settings such as ``requester_pays=True`` for requester-pays GCS buckets.
    """

    path: str
    storage_options: dict[str, Any] = {}
    append_build_id: bool = False

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        """Validate deployment targets as remote URLs or local directories."""
        normalized_value = value.strip()
        if not normalized_value:
            raise ValueError("Deployment target path cannot be empty.")

        if normalized_value.startswith(("s3://", "gs://")):
            parsed_path = urlparse(normalized_value)
            if parsed_path.scheme not in {"s3", "gs"} or not parsed_path.netloc:
                raise ValueError(
                    "Deployment target path must be a valid s3:// or gs:// URL."
                )
            return normalized_value

        if normalized_value.startswith("file://"):
            parsed_path = urlparse(normalized_value)
            if parsed_path.scheme != "file" or parsed_path.netloc not in {
                "",
                "localhost",
            }:
                raise ValueError(
                    "Deployment target path must be a valid file:// URI with an absolute local path."
                )
            local_path = Path(parsed_path.path)
        else:
            if "://" in normalized_value:
                raise ValueError(
                    "Deployment target path must be a valid s3:// URL, gs:// URL, "
                    "file:// URI, or local filesystem path."
                )

            local_path = Path(normalized_value)

        validate_local_deployment_directory(local_path)

        return normalized_value


class FercEqrDeploymentResource(dg.ConfigurableResource):
    """One or more deployment destinations for FERC EQR outputs.

    Deployment targets can be provided directly as structured config or loaded from a
    YAML file. Direct ``deployment_targets`` take precedence. When neither explicit
    targets nor a deployment config path are provided, deployment is skipped.
    """

    deployment_targets: list[FercEqrDeploymentTargetConfig] = []
    deployment_config_path: str | None = None

    @classmethod
    def from_yaml(
        cls, deployment_config_path: str | Path
    ) -> "FercEqrDeploymentResource":
        """Create a FERC EQR deployment resource from a YAML config file."""
        yaml_data = yaml.safe_load(Path(deployment_config_path).read_text()) or {}
        resource = cls.model_validate(yaml_data)
        return resource

    def configured_targets(self) -> list[FercEqrDeploymentTargetConfig]:
        """Return deployment-target config with explicit overrides taking precedence."""
        if self.deployment_targets:
            return self.deployment_targets

        deployment_config_path = self.deployment_config_path
        if deployment_config_path is None:
            deployment_config_path = dg.EnvVar(
                "PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH"
            ).get_value(default=None)
        if deployment_config_path is None:
            return []

        return self.from_yaml(
            deployment_config_path=str(deployment_config_path)
        ).deployment_targets

    def resolved_targets(self) -> list[UPath]:
        """Return the list of :class:`~upath.UPath` deployment destinations.

        Each configured target is converted to a :class:`~upath.UPath` using its
        provided ``storage_options``.
        """
        resolved_targets: list[UPath] = []
        for target in self.configured_targets():
            resolved_target = UPath(str(target.path), **target.storage_options)
            if target.append_build_id:
                build_id = os.getenv("BUILD_ID")
                if not build_id:
                    raise ValueError(
                        "BUILD_ID must be set for deployment targets with append_build_id enabled."
                    )
                resolved_target = resolved_target / build_id
            resolved_targets.append(resolved_target)

        return resolved_targets


global_data_config_resource = GlobalDataConfigResource.configure_at_launch()
pudl_paths_resource = PudlPathsResource.configure_at_launch()
zenodo_doi_settings_resource = ZenodoDoiSettingsResource()
datastore_resource = DatastoreResource(
    zenodo_dois=zenodo_doi_settings_resource,
    pudl_paths=pudl_paths_resource,
)
ferc_xbrl_runtime_settings = FercXbrlRuntimeSettings()
ferceqr_archive = FercEqrArchiveResource()
ferceqr_deployment_targets = FercEqrDeploymentResource()

default_resources: dict[str, Any] = {
    "datastore": datastore_resource,
    "global_data_config": global_data_config_resource,
    "pudl_paths": pudl_paths_resource,
    "ferceqr_data_config": ferceqr_archive,
    "runtime_settings": ferc_xbrl_runtime_settings,
    "zenodo_dois": zenodo_doi_settings_resource,
    "ferceqr_deployment_targets": ferceqr_deployment_targets,
}

__all__ = [
    "DatastoreResource",
    "FercEqrArchiveResource",
    "FercEqrDeploymentTargetConfig",
    "FercEqrDeploymentResource",
    "FercXbrlRuntimeSettings",
    "GlobalDataConfigResource",
    "PudlPathsResource",
    "ZenodoDoiSettingsResource",
    "datastore_resource",
    "default_resources",
    "ferceqr_archive",
    "ferceqr_deployment_targets",
    "ferc_xbrl_runtime_settings",
    "global_data_config_resource",
    "pudl_paths_resource",
    "zenodo_doi_settings_resource",
]
