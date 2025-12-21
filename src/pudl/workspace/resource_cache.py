"""Implementations of datastore resource caches."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, NamedTuple
from urllib.parse import urlparse

import boto3
import google.auth
from botocore.client import Config
from botocore.exceptions import ClientError, NoCredentialsError
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

    @abstractmethod
    def add(self, resource: PudlResourceKey, content: bytes) -> None:
        """Adds resource to the cache and sets the content."""

    @abstractmethod
    def delete(self, resource: PudlResourceKey) -> None:
        """Removes the resource from cache."""

    @abstractmethod
    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if the resource is present in the cache."""


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
        with self._resource_path(resource).open("rb") as res:
            logger.debug(f"Getting {resource} from local file cache.")
            return res.read()

    def add(self, resource: PudlResourceKey, content: bytes):
        """Adds (or updates) resource to the cache with given value."""
        logger.debug(f"Adding {resource} to {self._resource_path}")
        if self.is_read_only():
            logger.debug(f"Read only cache: ignoring set({resource})")
            return
        path = self._resource_path(resource)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as file:
            file.write(content)

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
            raise ValueError(f"gcs_path should start with gs:// (found: {gcs_path})")
        self._path_prefix = Path(parsed_url.path)
        # Get GCP credentials and billing project id
        # A billing project is now required because zenodo-cache is requester pays.
        credentials, project_id = google.auth.default()
        self._bucket = storage.Client(credentials=credentials).bucket(
            parsed_url.netloc, user_project=project_id
        )

    def _blob(self, resource: PudlResourceKey) -> Blob:
        """Retrieve Blob object associated with given resource."""
        p = (self._path_prefix / resource.get_local_path()).as_posix().lstrip("/")
        return self._bucket.blob(p)

    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves value associated with given resource."""
        logger.debug(f"Getting {resource} from {self._blob.__name__}")
        return self._blob(resource).download_as_bytes(retry=gcs_retry)

    def add(self, resource: PudlResourceKey, value: bytes):
        """Adds (or updates) resource to the cache with given value."""
        logger.debug(f"Adding {resource} to {self._blob.__name__}")
        return self._blob(resource).upload_from_string(value)

    def delete(self, resource: PudlResourceKey):
        """Deletes resource from the cache."""
        self._blob(resource).delete()

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if resource is present in the cache."""
        return self._blob(resource).exists(retry=gcs_retry)


class S3Cache(AbstractCache):
    """Implements file cache backed by AWS S3 bucket.

    This cache can access both public and private S3 buckets. For public buckets,
    no AWS credentials are required. For private buckets, AWS credentials should be
    available in the environment (via environment variables, AWS config files, or
    IAM roles).
    """

    def __init__(self, s3_path: str, **kwargs: Any):
        """Constructs new cache that stores files in AWS S3.

        Args:
            s3_path: path to where the data should be stored. This should
                be in the form of s3://{bucket-name}/{optional-path-prefix}

        Raises:
            ValueError: if s3_path doesn't start with s3://
        """
        super().__init__(**kwargs)
        parsed_url = urlparse(s3_path)
        if parsed_url.scheme != "s3":
            raise ValueError(f"s3_path should start with s3:// (found: {s3_path})")

        self._bucket_name = parsed_url.netloc
        self._path_prefix = Path(parsed_url.path)

        # Try to create S3 client with credentials from the environment
        # If no credentials are available, create an unsigned client for public buckets
        try:
            # Try with credentials first (from environment, AWS config, or IAM role)
            self._s3_client = boto3.client("s3")
            self._unsigned = False
            logger.debug(
                f"S3Cache initialized with credentials for bucket {self._bucket_name}"
            )
        except (NoCredentialsError, ClientError) as e:
            # No credentials available, use unsigned requests for public buckets
            logger.debug(
                f"No AWS credentials found ({e}), using unsigned requests for public bucket {self._bucket_name}"
            )
            self._s3_client = boto3.client(
                "s3", config=Config(signature_version="UNSIGNED")
            )
            self._unsigned = True

    def _get_object_key(self, resource: PudlResourceKey) -> str:
        """Get the S3 object key for a given resource."""
        return (self._path_prefix / resource.get_local_path()).as_posix().lstrip("/")

    def get(self, resource: PudlResourceKey) -> bytes:
        """Retrieves value associated with given resource.

        Raises:
            KeyError: if the resource doesn't exist in S3
            ClientError: for other S3 errors
        """
        key = self._get_object_key(resource)
        logger.debug(f"Getting {resource} from S3 bucket {self._bucket_name}")

        try:
            response = self._s3_client.get_object(Bucket=self._bucket_name, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise KeyError(
                    f"{resource} not found in S3 bucket {self._bucket_name}"
                ) from e
            raise

    def add(self, resource: PudlResourceKey, content: bytes):
        """Adds (or updates) resource to the cache with given content.

        Raises:
            RuntimeError: if cache is read-only or using unsigned requests
            ClientError: for S3 errors
        """
        if self.is_read_only():
            logger.debug(f"Read only cache: ignoring add({resource})")
            return

        if self._unsigned:
            raise RuntimeError(
                "Cannot write to S3 without credentials. "
                "Please configure AWS credentials to write to S3."
            )

        key = self._get_object_key(resource)
        logger.debug(f"Adding {resource} to S3 bucket {self._bucket_name}")

        self._s3_client.put_object(Bucket=self._bucket_name, Key=key, Body=content)

    def delete(self, resource: PudlResourceKey):
        """Deletes resource from the cache.

        Raises:
            RuntimeError: if cache is read-only or using unsigned requests
            ClientError: for S3 errors
        """
        if self.is_read_only():
            logger.debug(f"Read only cache: ignoring delete({resource})")
            return

        if self._unsigned:
            raise RuntimeError(
                "Cannot delete from S3 without credentials. "
                "Please configure AWS credentials to delete from S3."
            )

        key = self._get_object_key(resource)
        logger.debug(f"Deleting {resource} from S3 bucket {self._bucket_name}")

        self._s3_client.delete_object(Bucket=self._bucket_name, Key=key)

    def contains(self, resource: PudlResourceKey) -> bool:
        """Returns True if resource is present in the cache.

        Returns:
            True if the resource exists in S3, False otherwise
        """
        key = self._get_object_key(resource)

        try:
            self._s3_client.head_object(Bucket=self._bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return False
            # For other errors, log and re-raise
            logger.error(f"Error checking if {resource} exists in S3: {e}")
            raise


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
            logger.debug(f"Adding {resource} to cache {cache_layer.__class__.__name__}")
            cache_layer.add(resource, value)
            logger.debug(
                f"Added {resource} to cache layer {cache_layer.__class__.__name__})"
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
