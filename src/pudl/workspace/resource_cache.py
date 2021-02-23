"""Implementations of datastore resource caches."""

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, NamedTuple, Optional

import gcsfs
from fsspec.implementations.local import LocalFileSystem

from pudl.helpers import fsspec_exists, get_fs

logger = logging.getLogger(__name__)


class PudlResourceKey(NamedTuple):
    """Uniquely identifies a specific resource."""

    dataset: str
    doi: str
    name: str

    def __repr__(self) -> str:
        """Returns string representation of PudlResourceKey."""
        return f'Resource({self.dataset}/{self.doi}/{self.name})'

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


class FSSpecCache(AbstractCache):
    """Implements fsspec-based cache that stores datastore resources under cache_root_dir."""

    def __init__(self, cache_root_dir: str, gcs_requester_pays: Optional[str] = None, **kwargs: Any):
        """Create instance of FSSpecCache storing resources under cache_root_dir."""
        super().__init__(**kwargs)
        self.cache_root_dir = cache_root_dir
        self.fs = get_fs(cache_root_dir)
        if gcs_requester_pays and isinstance(self.fs, gcsfs.GCSFileSystem):
            self.fs.requester_pays = gcs_requester_pays
        if isinstance(self.fs, LocalFileSystem):
            self.fs.auto_mkdir = True

    def _resource_path(self, resource: PudlResourceKey) -> str:
        return os.path.join(self.cache_root_dir, resource.get_local_path())

    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves content of given resource or throws KeyError."""
        with self.fs.open(self._resource_path(resource), "rb") as fd:
            return fd.read()

    def add(self, resource: PudlResourceKey, content: bytes) -> None:
        """Adds resource to the cache and sets the content."""
        if self.is_read_only():
            return
        with self.fs.open(self._resource_path(resource), "wb") as fd:
            fd.write(content)

    def delete(self, resource: PudlResourceKey) -> None:
        """Removes the resource from cache."""
        if self.is_read_only():
            return
        p = self._resource_path(resource)
        get_fs(p).delete(p)

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if the resource is present in the cache."""
        return fsspec_exists(self._resource_path(resource))


class LayeredCache(AbstractCache):
    """Implements multi-layered system of caches.

    This allows building multi-layered system of caches. The idea is that you can
    have faster local caches with fall-back to the more remote or expensive caches
    that can be acessed in case of missing content.

    Only the closest layer is being written to (set, delete), while all remaining
    layers are read-only (get).
    """

    def __init__(self, *caches: List[AbstractCache], **kwargs: Any):
        """Creates layered cache consisting of given cache layers.

        Args:
            caches: List of caching layers to uses. These are given in the order
              of decreasing priority.
        """
        super().__init__(**kwargs)
        self._caches = list(caches)  # type: List[AbstractCache]

    def add_cache_layer(self, cache: AbstractCache):
        """Adds caching layer. The priority is below all other."""
        self._caches.append(cache)

    def num_layers(self):
        """Returns number of caching layers that are in this LayeredCache."""
        return len(self._caches)

    def get(self, resource: PudlResourceKey) -> bytes:
        """Returns content of a given resource."""
        for i, cache in enumerate(self._caches):
            if cache.contains(resource):
                logger.debug(
                    f"get:{resource} found in {i}-th layer ({cache.__class__.__name__}).")
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
                    f"contains: {resource} found in {i}-th layer ({cache.__class__.__name__}).")
                return True
        logger.debug(f"contains: {resource} not found in layered cache.")
        return False
