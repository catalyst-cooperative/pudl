"""Implementations of datastore resource caches."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlparse

import google.auth
from google.api_core.exceptions import BadRequest
from google.api_core.retry import Retry
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.retry import _should_retry
from google.resumable_media.common import DataCorruption

import pudl.logging_helpers

logger = pudl.logging_helpers.get_logger(__name__)


def extend_gcp_retry_predicate(predicate, *exception_types):
    """Extend a GCS predicate function with additional exception_types."""

    def new_predicate(erc):
        """Predicate for checking an exception type."""
        return predicate(erc) or isinstance(erc, exception_types)

    return new_predicate


# Add BadRequest to default predicate _should_retry.
# GCS get requests occasionally fail because of BadRequest errors.
# See issue #1734.
gcs_retry = Retry(
    predicate=extend_gcp_retry_predicate(_should_retry, BadRequest, DataCorruption)
)


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
        pass

    @abstractmethod
    def add(self, resource: PudlResourceKey, content: bytes) -> None:
        """Adds resource to the cache and sets the content."""
        pass

    @abstractmethod
    def delete(self, resource: PudlResourceKey) -> None:
        """Removes the resource from cache."""
        pass

    @abstractmethod
    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if the resource is present in the cache."""
        pass


class LocalFileCache(AbstractCache):
    """Simple key-value store mapping PudlResourceKeys to ByteIO contents."""

    def __init__(self, cache_root_dir: Path, **kwargs: Any):
        """Constructs LocalFileCache that stores resources under cache_root_dir."""
        super().__init__(**kwargs)
        self.cache_root_dir = cache_root_dir

    def _resource_path(self, resource: PudlResourceKey) -> Path:
        return self.cache_root_dir / resource.get_local_path()

    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves value associated with a given resource."""
        return self._resource_path(resource).open("rb").read()

    def add(self, resource: PudlResourceKey, content: bytes):
        """Adds (or updates) resource to the cache with given value."""
        if self.is_read_only():
            logger.debug(f"Read only cache: ignoring set({resource})")
            return
        path = self._resource_path(resource)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.open("wb").write(content)

    def delete(self, resource: PudlResourceKey):
        """Deletes resource from the cache."""
        if self.is_read_only():
            logger.debug(f"Read only cache: ignoring delete({resource})")
            return
        self._resource_path(resource).unlink(missing_ok=True)

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if resource is present in the cache."""
        return self._resource_path(resource).exists()


class GoogleCloudStorageCache(AbstractCache):
    """Implements file cache backed by Google Cloud Storage bucket."""

    def __init__(self, gcs_path: str, **kwargs: Any):
        """Constructs new cache that stores files in Google Cloud Storage.

        Args:
            gcs_path (str): path to where the data should be stored. This should
              be in the form of gs://{bucket-name}/{optional-path-prefix}
        """
        super().__init__(**kwargs)
        parsed_url = urlparse(gcs_path)
        if parsed_url.scheme != "gs":
            raise ValueError(f"gsc_path should start with gs:// (found: {gcs_path})")
        self._path_prefix = Path(parsed_url.path)
        # Get GCP credentials and billing project id
        # A billing project is now required because zenodo-cache is requester pays.
        credentials, project_id = google.auth.default()
        self._bucket = storage.Client(credentials=credentials).bucket(
            parsed_url.netloc, user_project=project_id
        )
        # need to store arguments passed to __init__ so an instance can be restored
        # from a pickle
        self._state = {"gcs_path": gcs_path, **kwargs}

    def _blob(self, resource: PudlResourceKey) -> Blob:
        """Retrieve Blob object associated with given resource."""
        p = (self._path_prefix / resource.get_local_path()).as_posix().lstrip("/")
        return self._bucket.blob(p)

    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves value associated with given resource."""
        return self._blob(resource).download_as_bytes(retry=gcs_retry)

    def add(self, resource: PudlResourceKey, value: bytes):
        """Adds (or updates) resource to the cache with given value."""
        return self._blob(resource).upload_from_string(value)

    def delete(self, resource: PudlResourceKey):
        """Deletes resource from the cache."""
        self._blob(resource).delete()

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if resource is present in the cache."""
        return self._blob(resource).exists(retry=gcs_retry)

    def __getstate__(self):
        """Get current object state for serializing.

        Because some of this object's internals explicitly cannot be pickled, namely
        :class:`google.cloud.storage.Client`, we only serialize the arguments passed to
        :meth:`.GoogleCloudStorageCache.__init__`.
        """
        logger.warning(
            "When serializing %s, only %s is preserved",
            self.__class__.__qualname__,
            self._state,
        )
        return self._state.copy()

    def __setstate__(self, state):
        """Restore the object's state from a dictionary.

        :meth:`.GoogleCloudStorageCache.__init__` already does all the required setup,
        so we call it with the arguments originally passed when the object we are
        restoring was originally instantiated.
        """
        logger.warning(
            "Restoring %s, from %s",
            self.__class__.__qualname__,
            state,
        )
        self.__init__(**state)


class LayeredCache(AbstractCache):
    """Implements multi-layered system of caches.

    This allows building multi-layered system of caches. The idea is that you can have
    faster local caches with fall-back to the more remote or expensive caches that can
    be acessed in case of missing content.

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
                    f"get:{resource} found in {i}-th layer ({cache.__class__.__name__})."
                )
                return cache.get(resource)
        logger.debug(f"get:{resource} not found in the layered cache.")
        raise KeyError(f"{resource} not found in the layered cache")

    def add(self, resource: PudlResourceKey, value):
        """Adds (or replaces) resource into the cache with given value."""
        if self.is_read_only():
            logger.debug(f"Read only cache: ignoring set({resource})")
            return
        for cache_layer in self._caches:
            if cache_layer.is_read_only():
                continue
            cache_layer.add(resource, value)
            logger.debug(
                f"Add {resource} to cache layer {cache_layer.__class__.__name__})"
            )
            break

    def delete(self, resource: PudlResourceKey):
        """Removes resource from the cache if the cache is not in the read_only mode."""
        if self.is_read_only():
            logger.debug(f"Readonly cache: not removing {resource}")
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
                    f"contains: {resource} found in {i}-th layer ({cache.__class__.__name__})."
                )
                return True
        logger.debug(f"contains: {resource} not found in layered cache.")

    def is_optimally_cached(self, resource: PudlResourceKey) -> bool:
        """Return True if resource is contained in the closest write-enabled layer."""
        for cache_layer in self._caches:
            if cache_layer.is_read_only():
                continue
            logger.debug(
                f"{resource} optimally cached in {cache_layer.__class__.__name__}"
            )
            return cache_layer.contains(resource)
