"""Parallel bulk file transfer to, from, and within object storage.

PUDL publishes large collections of Parquet files to public buckets on Google
Cloud Storage and Amazon S3. Moving that data through a Python object-storage
abstraction (``fsspec`` / ``UPath``) is slow: transfers run one file at a time
with no multipart concurrency. This module shells out to purpose-built command
line tools instead -- :command:`s5cmd` for S3 and :command:`gcloud storage` for
GCS -- which parallelize aggressively and saturate the available bandwidth. For
local filesystem targets (used in tests and local development) it falls back to
:mod:`shutil`.

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
Bit-level integrity is already enforced by the transfer tools themselves:
``gcloud storage`` validates a CRC32C for every object end-to-end and fails the
command on a mismatch, and ``s5cmd`` multipart uploads carry a per-part checksum
that S3 rejects on corruption. A cross-backend whole-object checksum comparison
is intentionally not attempted: S3 ETags are the MD5 only for single-part
uploads (multipart ETags depend on the part size), and GCS omits ``md5_hash``
for composite objects, so there is no uniform digest to compare.
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

from pudl import logging_helpers

logger = logging_helpers.get_logger(__name__)

# Number of object-level transfers s5cmd runs in parallel. Set explicitly so our
# behavior does not shift if the tool's built-in default changes.
S5CMD_NUM_WORKERS = 256


def _run_cli(
    args: list[str],
    *,
    env_overrides: dict[str, str] | None = None,
    stdin: str | None = None,
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
        input=stdin,
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
        scheme = urlparse(uri).scheme
        if scheme == "s3":
            region = os.environ.get("AWS_REGION") or os.environ.get(
                "AWS_DEFAULT_REGION"
            )
            return S5cmdObjectStore(region=region)
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


class S5cmdObjectStore(ObjectStore):
    """Amazon S3 implementation using :command:`s5cmd`.

    *region* is exported as ``AWS_REGION`` for every call. ``s5cmd ls`` raises
    ``BucketRegionError`` when the region is wrong or unset, so it must match the
    bucket. Credentials are read by ``s5cmd`` itself from ``~/.aws/credentials``
    or the standard ``AWS_*`` environment variables.
    """

    def __init__(self, region: str | None = None):
        """Store the AWS region to use for every ``s5cmd`` invocation."""
        self.region = region

    def _s5cmd(self, args: list[str], *, stdin: str | None = None, check: bool = True):
        env_overrides = {"AWS_REGION": self.region} if self.region else None
        return _run_cli(
            ["s5cmd", "--numworkers", str(S5CMD_NUM_WORKERS), *args],
            env_overrides=env_overrides,
            stdin=stdin,
            check=check,
        )

    def upload_files(self, sources: Iterable[Path], dest_prefix: str) -> None:
        """Upload *sources* into *dest_prefix* via a batched ``s5cmd run`` script."""
        dest = dest_prefix.rstrip("/")
        commands = "".join(
            f"cp {shlex.quote(str(source))} "
            f"{shlex.quote(f'{dest}/{Path(source).name}')}\n"
            for source in sources
        )
        if not commands:
            return
        self._s5cmd(["run"], stdin=commands)

    def object_sizes(self, prefix: str) -> dict[str, int]:
        """Parse ``s5cmd --json ls`` output into relative path -> size in bytes."""
        base = prefix.rstrip("/")
        output = self._s5cmd(["--json", "ls", f"{base}/*"], check=False)
        sizes: dict[str, int] = {}
        for line in output.splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if "error" in record:
                if "no object found" in record["error"]:
                    return {}
                raise RuntimeError(f"s5cmd ls failed for {prefix}: {record['error']}")
            if record.get("type") == "file":
                sizes[record["key"].removeprefix(f"{base}/")] = int(record["size"])
        return sizes

    def sync(self, source_prefix: str, dest_prefix: str) -> None:
        """Server-side mirror *source_prefix* onto *dest_prefix*, deleting extras."""
        self._s5cmd(
            [
                "sync",
                "--delete",
                f"{source_prefix.rstrip('/')}/*",
                f"{dest_prefix.rstrip('/')}/",
            ]
        )

    def move(self, source_prefix: str, dest_prefix: str) -> None:
        """Server-side move everything under *source_prefix* beneath *dest_prefix*."""
        self._s5cmd(
            ["mv", f"{source_prefix.rstrip('/')}/*", f"{dest_prefix.rstrip('/')}/"]
        )

    def remove(self, prefix: str) -> None:
        """Best-effort recursive delete of *prefix* (ignores "nothing matched")."""
        self._s5cmd(["rm", f"{prefix.rstrip('/')}/*"], check=False)


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
