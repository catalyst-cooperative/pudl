"""Parallel bulk file transfer to, from, and within object storage.

PUDL publishes large collections of Parquet files to public buckets on Google
Cloud Storage and Amazon S3. Moving that data through a Python object-storage
abstraction (``fsspec`` / ``UPath``) is slow: transfers run one file at a time
with no multipart concurrency. This module uses purpose-built tooling instead --
:mod:`boto3` (with the AWS Common Runtime transfer client) for S3 and
:command:`gcloud storage` for GCS -- which parallelize aggressively and saturate
the available bandwidth. For local filesystem targets (used in tests and local
development) it falls back to :mod:`shutil`.

Obtain a client for a destination with :meth:`ObjectStore.for_uri`, which selects
the implementation from the URI scheme::

    store = ObjectStore.for_uri("s3://pudl.catalyst.coop/ferceqr")
    store.upload_files(local_parquet_files, "s3://pudl.catalyst.coop/._staging/data")
    store.move(
        "s3://pudl.catalyst.coop/._staging/data",
        "s3://pudl.catalyst.coop/ferceqr",
    )

All ``prefix`` / ``uri`` arguments are full URIs (``s3://bucket/key``,
``gs://bucket/key``). :class:`LocalObjectStore` additionally accepts plain
filesystem paths and ``file://`` URIs.

Upload integrity
----------------
:meth:`ObjectStore.object_sizes` exposes object counts and byte sizes so a
caller can confirm that a staged upload is *complete* before promoting it -- the
failure mode that matters here is a transfer that dies partway, not a bit flip.
Bit-level integrity is already enforced underneath: ``gcloud storage`` validates
a CRC32C for every object end-to-end, and both the CRT S3 client and S3 itself
checksum every (multipart) upload. A cross-backend whole-object checksum
comparison is intentionally not attempted: S3 ETags are the MD5 only for
single-part uploads (multipart ETags depend on the part size), and GCS omits
``md5_hash`` for composite objects, so there is no uniform digest to compare.
"""

import json
import os
import shlex
import shutil
import subprocess
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig, create_transfer_manager

from pudl import logging_helpers

logger = logging_helpers.get_logger(__name__)

# Server-side copy tuning. The transfer manager's defaults (8 MiB parts, 10-way
# concurrency) turn each ~4 GiB transaction file into ~500 serial UploadPartCopy
# calls, so promoting the full dataset took minutes. Large parts + high
# concurrency benchmarked ~9x faster (2+ GB/s vs ~250 MB/s) for same-bucket
# server-side copies.
S3_COPY_MULTIPART_THRESHOLD = 512 * 1024**2
S3_COPY_MULTIPART_CHUNKSIZE = 512 * 1024**2
S3_COPY_MAX_CONCURRENCY = 128

# Batch size limit for the S3 DeleteObjects API.
S3_DELETE_BATCH = 1000


def _run_cli(
    args: list[str],
    *,
    env_overrides: dict[str, str] | None = None,
    check: bool = True,
) -> str:
    """Run a command line tool and return its stdout.

    *env_overrides* are merged on top of the current environment. With
    ``check=True`` a non-zero exit raises :class:`RuntimeError` with the captured
    output; with ``check=False`` the stdout is returned regardless of exit code
    (for commands whose "nothing matched" case is not a real error).
    """
    env = {**os.environ, **env_overrides} if env_overrides else None
    logger.info(f"Running: {shlex.join(args)}")
    result = subprocess.run(  # noqa: S603
        args,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if result.stderr.strip():
        logger.debug(f"{args[0]} stderr:\n{result.stderr}")
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {result.returncode}): {shlex.join(args)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


class ObjectStore(ABC):
    """Transfer files to, from, and within a single storage backend.

    One subclass per URI scheme, chosen by :meth:`for_uri`.
    """

    @classmethod
    def for_uri(
        cls, uri: str, storage_options: dict[str, Any] | None = None
    ) -> "ObjectStore":
        """Return an :class:`ObjectStore` capable of operating on *uri*.

        *storage_options* mirrors the per-target config in the FERC EQR
        deployment YAML. Only ``requester_pays`` is consulted (for GCS); other
        keys are ignored.
        """
        storage_options = storage_options or {}
        parsed = urlparse(uri)
        scheme = parsed.scheme
        if scheme == "s3":
            region = os.environ.get("AWS_REGION") or os.environ.get(
                "AWS_DEFAULT_REGION"
            )
            return S3ObjectStore(bucket=parsed.netloc, region=region)
        if scheme in {"gs", "gcs"}:
            billing_project = None
            if storage_options.get("requester_pays"):
                billing_project = os.environ.get("GCP_BILLING_PROJECT")
            return GcloudStorageObjectStore(billing_project=billing_project)
        if scheme in {"", "file"}:
            return LocalObjectStore()
        raise ValueError(
            f"No ObjectStore implementation for URI scheme {scheme!r}: {uri}"
        )

    @abstractmethod
    def upload_files(self, sources: Iterable[Path], dest_prefix: str) -> None:
        """Copy every file in *sources* into *dest_prefix*, keeping file names."""

    @abstractmethod
    def object_sizes(self, prefix: str) -> dict[str, int]:
        """Map ``path_relative_to_prefix`` -> ``size_bytes`` for objects under *prefix*.

        Returns an empty mapping if nothing is present at *prefix*.
        """

    @abstractmethod
    def sync(self, source_prefix: str, dest_prefix: str) -> None:
        """Make *dest_prefix* an exact copy of *source_prefix*, deleting extras.

        Server-side within a cloud backend -- no data touches the local machine.
        The caller must ensure *source_prefix* is non-empty.
        """

    @abstractmethod
    def move(self, source_prefix: str, dest_prefix: str) -> None:
        """Move every object under *source_prefix* beneath *dest_prefix*.

        Server-side within a cloud backend. *source_prefix* is emptied.
        """

    @abstractmethod
    def remove(self, prefix: str) -> None:
        """Recursively delete *prefix*; a no-op if it does not exist."""


class LocalObjectStore(ObjectStore):
    """Filesystem implementation backed by :mod:`shutil`, for tests and local dev."""

    @staticmethod
    def _path(uri: str) -> Path:
        parsed = urlparse(uri)
        return Path(parsed.path) if parsed.scheme == "file" else Path(uri)

    def upload_files(self, sources: Iterable[Path], dest_prefix: str) -> None:
        """Copy *sources* into the *dest_prefix* directory."""
        dest = self._path(dest_prefix)
        dest.mkdir(parents=True, exist_ok=True)
        for source in sources:
            shutil.copy2(source, dest / Path(source).name)

    def object_sizes(self, prefix: str) -> dict[str, int]:
        """Walk the *prefix* directory and return relative path -> size in bytes."""
        base = self._path(prefix)
        if not base.exists():
            return {}
        return {
            str(path.relative_to(base)): path.stat().st_size
            for path in sorted(base.rglob("*"))
            if path.is_file()
        }

    def sync(self, source_prefix: str, dest_prefix: str) -> None:
        """Replace the *dest_prefix* directory with a copy of *source_prefix*."""
        dest = self._path(dest_prefix)
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(self._path(source_prefix), dest)

    def move(self, source_prefix: str, dest_prefix: str) -> None:
        """Move every file under *source_prefix* into *dest_prefix*."""
        source = self._path(source_prefix)
        dest = self._path(dest_prefix)
        for path in sorted(source.rglob("*")):
            if not path.is_file():
                continue
            target = dest / path.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(target))

    def remove(self, prefix: str) -> None:
        """Recursively delete the *prefix* directory if it exists."""
        path = self._path(prefix)
        if path.exists():
            shutil.rmtree(path)


def _split_s3(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``."""
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


class S3ObjectStore(ObjectStore):
    """Amazon S3 implementation using :mod:`boto3`.

    Bulk local-to-S3 uploads go through the AWS Common Runtime transfer client
    (``preferred_transfer_client="crt"``) for throughput; listing, server-side
    copy, and delete use ordinary :mod:`boto3` calls. (``s5cmd`` was tried here
    first, but its object listing is unreliable in the moment right after a
    write, which broke the stage-then-promote flow.)

    The bucket's region is detected once via ``get_bucket_location`` unless
    *region* is given, so a misconfigured ``AWS_REGION`` does not matter.
    """

    def __init__(self, bucket: str, region: str | None = None):
        """Record the target *bucket* and, optionally, its *region*."""
        self.bucket = bucket
        self._region = region
        self._client_cache: Any = None

    @property
    def _client(self) -> Any:
        """A boto3 S3 client bound to the bucket's region (built on first use)."""
        if self._client_cache is None:
            self._client_cache = boto3.client(
                "s3", region_name=self._region or self._detect_region()
            )
        return self._client_cache

    def _detect_region(self) -> str:
        """Resolve the bucket's region (``get_bucket_location`` needs no redirect)."""
        location = (
            boto3.client("s3", region_name="us-east-1")
            .get_bucket_location(Bucket=self.bucket)
            .get("LocationConstraint")
        )
        return {None: "us-east-1", "EU": "eu-west-1"}.get(location) or location

    def _keys_and_sizes(self, prefix: str) -> dict[str, int]:
        """Return ``{full_key: size}`` for every object under an ``s3://`` *prefix*."""
        bucket, key_prefix = _split_s3(prefix.rstrip("/"))
        key_prefix = f"{key_prefix}/"
        keys: dict[str, int] = {}
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=key_prefix
        ):
            for obj in page.get("Contents", []):
                if obj["Key"] != key_prefix:  # skip a prefix placeholder object
                    keys[obj["Key"]] = obj["Size"]
        return keys

    def object_sizes(self, prefix: str) -> dict[str, int]:
        """Map ``path_relative_to_prefix`` -> ``size_bytes`` via ``list_objects_v2``."""
        _, key_prefix = _split_s3(prefix.rstrip("/"))
        key_prefix = f"{key_prefix}/"
        return {
            key.removeprefix(key_prefix): size
            for key, size in self._keys_and_sizes(prefix).items()
        }

    def upload_files(self, sources: Iterable[Path], dest_prefix: str) -> None:
        """Upload *sources* into *dest_prefix* via the CRT transfer manager."""
        sources = [Path(source) for source in sources]
        if not sources:
            return
        bucket, key_prefix = _split_s3(dest_prefix.rstrip("/"))
        logger.info(f"Uploading {len(sources)} file(s) to {dest_prefix}")
        config = TransferConfig(preferred_transfer_client="crt")
        with create_transfer_manager(self._client, config) as manager:
            futures = [
                manager.upload(str(source), bucket, f"{key_prefix}/{source.name}")
                for source in sources
            ]
            for future in futures:
                future.result()

    def _copy(
        self, source_prefix: str, dest_prefix: str, relative_keys: Iterable[str]
    ) -> None:
        """Server-side copy the given prefix-relative keys from source to dest."""
        relative_keys = list(relative_keys)
        if not relative_keys:
            return
        src_bucket, src_key = _split_s3(source_prefix.rstrip("/"))
        dst_bucket, dst_key = _split_s3(dest_prefix.rstrip("/"))
        config = TransferConfig(
            multipart_threshold=S3_COPY_MULTIPART_THRESHOLD,
            multipart_chunksize=S3_COPY_MULTIPART_CHUNKSIZE,
            max_concurrency=S3_COPY_MAX_CONCURRENCY,
        )
        with create_transfer_manager(self._client, config) as manager:
            futures = [
                manager.copy(
                    {"Bucket": src_bucket, "Key": f"{src_key}/{relative}"},
                    dst_bucket,
                    f"{dst_key}/{relative}",
                )
                for relative in relative_keys
            ]
            for future in futures:
                future.result()

    def _delete(self, bucket: str, keys: Iterable[str]) -> None:
        """Delete *keys* from *bucket* in batches; a no-op for an empty iterable."""
        keys = list(keys)
        for start in range(0, len(keys), S3_DELETE_BATCH):
            self._client.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": [
                        {"Key": key} for key in keys[start : start + S3_DELETE_BATCH]
                    ],
                    "Quiet": True,
                },
            )

    def sync(self, source_prefix: str, dest_prefix: str) -> None:
        """Server-side mirror *source_prefix* onto *dest_prefix*, deleting extras."""
        source = self.object_sizes(source_prefix)
        dest = self.object_sizes(dest_prefix)
        self._copy(
            source_prefix,
            dest_prefix,
            [rel for rel, size in source.items() if dest.get(rel) != size],
        )
        dst_bucket, dst_key = _split_s3(dest_prefix.rstrip("/"))
        self._delete(
            dst_bucket,
            [f"{dst_key}/{rel}" for rel in dest if rel not in source],
        )

    def move(self, source_prefix: str, dest_prefix: str) -> None:
        """Server-side move everything under *source_prefix* beneath *dest_prefix*."""
        relative_keys = list(self.object_sizes(source_prefix))
        self._copy(source_prefix, dest_prefix, relative_keys)
        src_bucket, src_key = _split_s3(source_prefix.rstrip("/"))
        self._delete(src_bucket, [f"{src_key}/{rel}" for rel in relative_keys])

    def remove(self, prefix: str) -> None:
        """Recursively delete *prefix*; a no-op if it does not exist."""
        bucket, _ = _split_s3(prefix.rstrip("/"))
        self._delete(bucket, self._keys_and_sizes(prefix))


class GcloudStorageObjectStore(ObjectStore):
    """Google Cloud Storage implementation using :command:`gcloud storage`.

    When the target bucket has requester-pays enabled, *billing_project* must be
    set; it is passed as ``--billing-project`` on every call.
    """

    def __init__(self, billing_project: str | None = None):
        """Store the GCP project to bill for requester-pays buckets."""
        self.billing_project = billing_project

    def _gcloud_storage(self, args: list[str], *, check: bool = True) -> str:
        cmd = ["gcloud", "storage", *args]
        if self.billing_project:
            cmd.append(f"--billing-project={self.billing_project}")
        return _run_cli(cmd, check=check)

    def upload_files(self, sources: Iterable[Path], dest_prefix: str) -> None:
        """Upload *sources* into *dest_prefix* with a single ``gcloud storage cp``."""
        source_args = [str(source) for source in sources]
        if not source_args:
            return
        self._gcloud_storage(["cp", *source_args, f"{dest_prefix.rstrip('/')}/"])

    def object_sizes(self, prefix: str) -> dict[str, int]:
        """Parse ``gcloud storage objects list --format=json`` into rel path -> size.

        Returns an empty mapping for a missing/empty prefix (``objects list``
        prints ``[]`` and exits zero in that case).
        """
        base = prefix.rstrip("/")
        output = self._gcloud_storage(
            ["objects", "list", f"{base}/**", "--format=json"]
        )
        sizes: dict[str, int] = {}
        for record in json.loads(output):
            # storage_url carries a "#generation" suffix; strip it and the prefix.
            url = record["storage_url"].split("#", 1)[0]
            relative = url.removeprefix(f"{base}/")
            # Skip "directory placeholder" objects (zero-byte, name ends in "/").
            if relative.endswith("/"):
                continue
            sizes[relative] = int(record["size"])
        return sizes

    def sync(self, source_prefix: str, dest_prefix: str) -> None:
        """Server-side mirror *source_prefix* onto *dest_prefix*, deleting extras."""
        self._gcloud_storage(
            [
                "rsync",
                "--recursive",
                "--delete-unmatched-destination-objects",
                source_prefix.rstrip("/"),
                dest_prefix.rstrip("/"),
            ]
        )

    def move(self, source_prefix: str, dest_prefix: str) -> None:
        """Server-side move everything under *source_prefix* beneath *dest_prefix*.

        ``gcloud storage mv`` recurses through a wildcard source on its own and
        rejects an explicit ``--recursive`` flag, unlike ``cp``/``rm``/``rsync``.
        """
        self._gcloud_storage(
            [
                "mv",
                f"{source_prefix.rstrip('/')}/*",
                f"{dest_prefix.rstrip('/')}/",
            ]
        )

    def remove(self, prefix: str) -> None:
        """Best-effort recursive delete of *prefix* (ignores "nothing matched")."""
        self._gcloud_storage(
            ["rm", "--recursive", f"{prefix.rstrip('/')}/**"], check=False
        )
