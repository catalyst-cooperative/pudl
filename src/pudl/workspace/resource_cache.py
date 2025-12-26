"""Implementations of datastore resource caches."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, NamedTuple

import boto3
import google.auth
from upath import UPath

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


class PudlResourceKey(NamedTuple):
    """Uniquely identifies a specific resource."""

    dataset: str
    doi: str
    name: str

    def __repr__(self) -> str:
        """Returns string representation of PudlResourceKey."""
        return f"Resource({self.dataset}/{self.doi}/{self.name})"

    def get_local_path(self) -> Path:
        """Returns (relative) path that should be used when caching this resource."""
        doi_dirname = self.doi.replace("/", "-")
        return Path(self.dataset) / doi_dirname / self.name


class AbstractCache(ABC):
    """Defines interaface for the generic resource caching layer."""

    def __init__(self, read_only: bool = False):
        """Constructs instance and sets read_only attribute."""
        self._read_only = read_only

    def is_read_only(self) -> bool:
        """Returns true if the cache is read-only and should not be modified."""
        return self._read_only

    @abstractmethod
    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves content of given resource or throws KeyError."""

    @abstractmethod
    def add(self, resource: PudlResourceKey, content: bytes) -> None:
        """Adds resource to the cache and sets the content."""

    @abstractmethod
    def delete(self, resource: PudlResourceKey) -> None:
        """Removes the resource from cache."""

    @abstractmethod
    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if the resource is present in the cache."""


class UPathCache(AbstractCache):
    """Implements file cache using UPath for unified access to multiple storage backends.

    This cache uses universal_pathlib's UPath to provide a unified interface
    for accessing data stored in S3, GCS, or local filesystems. It handles backend-specific
    authentication and credential management internally.

    Requires UPath objects with explicit protocols:
        - s3://bucket-name/path/prefix
        - gs://bucket-name/path/prefix
        - file:///local/path
    """

    supported_protocols: set[str] = {"s3", "gs", "file"}

    def __init__(self, storage_upath: UPath, **kwargs: Any):
        """Constructs new cache using UPath for storage backend access.

        Args:
            storage_upath: UPath object pointing to where data should be stored.
                Must have an explicit protocol (file://, s3://, or gs://).

        Raises:
            ValueError: if storage_upath uses an unsupported scheme
        """
        super().__init__(**kwargs)

        self._protocol = storage_upath.protocol

        # Validate supported schemes
        if self._protocol not in self.supported_protocols:
            raise ValueError(
                f"Unsupported storage scheme: {self._protocol}. "
                f"Supported schemes are: {self.supported_protocols}"
            )

        # Initialize UPath with appropriate storage options
        self._storage_options = self._setup_credentials()
        self._base_path = UPath(storage_upath, **self._storage_options)

        logger.debug(
            f"UPathCache initialized with scheme={self._protocol}, "
            f"base_path={self._base_path}"
        )

    def _setup_credentials(self) -> dict[str, Any]:
        """Set up backend-specific credentials and storage options.

        This should be the only place where backend-specific logic is required.

        Returns:
            Dictionary of storage options to pass to UPath
        """
        storage_options = {}

        if self._protocol == "s3":
            # Check if AWS credentials are available
            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials is None:
                # No credentials available, use anonymous access for public buckets
                logger.debug("No AWS credentials found, using anonymous S3 access")
                storage_options["anon"] = True
            else:
                logger.debug("Using AWS credentials for S3 access")
                storage_options["anon"] = False

        elif self._protocol == "gs":
            # For GCS, attempt to get default credentials
            try:
                credentials, project_id = google.auth.default()
                logger.debug(f"Using GCP credentials with project_id={project_id}")
                # fsspec's gcsfs will automatically use default credentials
                # We can pass the project for requester-pays buckets
                if project_id:
                    storage_options["project"] = project_id
            except Exception as e:
                logger.warning(
                    f"Could not load GCP credentials: {e}. Attempting anonymous access."
                )
                storage_options["token"] = "anon"  # noqa: S105

        # For local filesystem, no special credentials needed
        return storage_options

    def _resource_path(self, resource: PudlResourceKey) -> UPath:
        """Get the UPath for a given resource.

        Args:
            resource: The resource to get the path for

        Returns:
            UPath object pointing to the resource location
        """
        return self._base_path / resource.get_local_path()

    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves value associated with given resource.

        Args:
            resource: The resource to retrieve

        Returns:
            The content of the resource as bytes

        Raises:
            KeyError: if the resource doesn't exist
            Exception: for other storage backend errors
        """
        path = self._resource_path(resource)
        logger.debug(f"Getting {resource} from UPath cache at {path}")

        try:
            return path.read_bytes()
        except FileNotFoundError as e:
            raise KeyError(f"{resource} not found at {path}") from e

    def add(self, resource: PudlResourceKey, content: bytes):
        """Adds (or updates) resource to the cache with given content.

        Args:
            resource: The resource to add
            content: The content to store

        Raises:
            RuntimeError: if cache is read-only or credentials are insufficient
        """
        if self.is_read_only():
            logger.warning(f"Read only cache: ignoring add({resource})")
            return

        # Check if we can write (for S3/GCS without credentials)
        if self._protocol == "s3" and self._storage_options.get("anon", False):
            raise RuntimeError(
                "Cannot write to S3 without credentials. "
                "Please configure AWS credentials to write to S3."
            )

        if self._protocol == "gs" and self._storage_options.get("token") == "anon":
            raise RuntimeError(
                "Cannot write to GCS without credentials. "
                "Please configure GCP credentials to write to GCS."
            )

        path = self._resource_path(resource)
        logger.debug(f"Adding {resource} to UPath cache at {path}")

        # Ensure parent directories exist
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write the content
        path.write_bytes(content)

    def delete(self, resource: PudlResourceKey):
        """Deletes resource from the cache.

        Args:
            resource: The resource to delete

        Raises:
            RuntimeError: if cache is read-only or credentials are insufficient
        """
        if self.is_read_only():
            logger.warning(f"Read only cache: ignoring delete({resource})")
            return

        # Check if we can write (for S3/GCS without credentials)
        if self._protocol == "s3" and self._storage_options.get("anon", False):
            raise RuntimeError(
                "Cannot delete from S3 without credentials. "
                "Please configure AWS credentials to delete from S3."
            )

        if self._protocol == "gs" and self._storage_options.get("token") == "anon":
            raise RuntimeError(
                "Cannot delete from GCS without credentials. "
                "Please configure GCP credentials to delete from GCS."
            )

        path = self._resource_path(resource)
        logger.debug(f"Deleting {resource} from UPath cache at {path}")

        try:
            path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Error deleting {resource} from UPath cache: {e}")
            raise

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if resource is present in the cache.

        Args:
            resource: The resource to check

        Returns:
            True if the resource exists, False otherwise
        """
        path = self._resource_path(resource)

        try:
            return path.exists()
        except Exception as e:
            logger.debug(f"Error checking if {resource} exists: {e}")
            return False


class LayeredCache(AbstractCache):
    """Implements multi-layered system of caches.

    This allows building multi-layered system of caches. The idea is that you can have
    faster local caches with fall-back to the more remote or expensive caches that can
    be accessed in case of missing content.

    Only the closest layer is being written to (set, delete), while all remaining layers
    are read-only (get).
    """

    def __init__(self, *caches: list[AbstractCache], **kwargs: Any):
        """Creates layered cache consisting of given cache layers.

        Args:
            caches: List of caching layers to uses. These are given in the order
              of decreasing priority.
        """
        super().__init__(**kwargs)
        self._caches: list[AbstractCache] = list(caches)

    def add_cache_layer(self, cache: AbstractCache):
        """Adds caching layer.

        The priority is below all other.
        """
        self._caches.append(cache)

    def num_layers(self):
        """Returns number of caching layers that are in this LayeredCache."""
        return len(self._caches)

    def get(self, resource: PudlResourceKey) -> bytes:
        """Returns content of a given resource."""
        for i, cache in enumerate(self._caches):
            if cache.contains(resource):
                logger.debug(
                    f"get:{resource} found in layer {i} ({cache.__class__.__name__})."
                )
                return cache.get(resource)
        logger.debug(f"get:{resource} not found in the layered cache.")
        raise KeyError(f"{resource} not found in the layered cache")

    def add(self, resource: PudlResourceKey, value):
        """Adds (or replaces) resource into the cache with given value."""
        if self.is_read_only():
            logger.warning(f"Read only cache: ignoring set({resource})")
            return
        for cache_layer in self._caches:
            if cache_layer.is_read_only():
                continue
            logger.debug(f"Adding {resource} to cache {cache_layer.__class__.__name__}")
            cache_layer.add(resource, value)
            logger.debug(
                f"Added {resource} to cache layer {cache_layer.__class__.__name__})"
            )
            break

    def delete(self, resource: PudlResourceKey):
        """Removes resource from the cache if the cache is not in the read_only mode."""
        if self.is_read_only():
            logger.warning(f"Read only cache: not removing {resource}")
            return
        for cache_layer in self._caches:
            if cache_layer.is_read_only():
                continue
            cache_layer.delete(resource)
            break

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if resource is present in the cache."""
        for i, cache in enumerate(self._caches):
            if cache.contains(resource):
                logger.debug(
                    f"contains: {resource} found in layer {i} ({cache.__class__.__name__})."
                )
                return True
        logger.debug(f"contains: {resource} not found in layered cache.")
        return False

    def is_optimally_cached(self, resource: PudlResourceKey) -> bool:
        """Return True if resource is contained in the closest write-enabled layer."""
        for cache_layer in self._caches:
            if cache_layer.is_read_only():
                continue
            logger.debug(
                f"{resource} optimally cached in {cache_layer.__class__.__name__}"
            )
            return cache_layer.contains(resource)
        return False
