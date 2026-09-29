"""Fast bulk uploads to, and server-side copies within, Amazon S3.

``s3fs`` is fine for listing, deleting and small transfers, but it is several times
slower than :mod:`boto3` for the two operations that move the FERC EQR outputs
(~120 GiB, mostly 3-4 GiB Parquet files):

* Uploads: ``s3fs`` keeps at most 10 HTTP connections open by default, which limits
  throughput on the long-RTT GCP -> AWS path. The CRT transfer client in ``boto3``
  saturates it.
* Server-side copies: ``s3fs`` copies each object with a single serial stream of
  50 MiB ``UploadPartCopy`` requests (~70 MB/s per object, whatever the block
  size), while the ``boto3`` transfer manager copies the parts of each object in
  parallel. On a 4-file benchmark it was ~6x faster (1.3 GB/s vs ~0.2 GB/s).

Everything else -- listing, deletion, verification, and all non-S3 targets --
stays on ``fsspec``/``UPath``. All ``uri`` arguments are full ``s3://bucket/key``
URIs; callers pass the whole batch so one transfer manager is shared by all of
its files.
"""

import functools
from collections.abc import Iterable
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig, create_transfer_manager

MiB = 1024**2

# Large parts + wide concurrency keep the cross-cloud pipe full; the 8 MiB default
# spends most of the transfer waiting on per-part round trips.
UPLOAD_PART_SIZE = 64 * MiB
UPLOAD_CONCURRENCY = 64

# The default 8 MiB parts turn each ~4 GiB object into ~500 serial copy calls.
COPY_PART_SIZE = 512 * MiB
COPY_CONCURRENCY = 128


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an s3:// URI: {uri}")
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


@functools.cache
def _client(bucket: str):
    """Return a boto3 S3 client bound to *bucket*'s region.

    The region is looked up with ``get_bucket_location`` (which needs no
    redirect), so a missing or wrong ``AWS_REGION`` does not matter.
    """
    location = (
        boto3.client("s3", region_name="us-east-1")
        .get_bucket_location(Bucket=bucket)
        .get("LocationConstraint")
    )
    # The API reports buckets in us-east-1 as having no location constraint.
    return boto3.client("s3", region_name=location or "us-east-1")


def upload_files(files: Iterable[tuple[Path, str]]) -> None:
    """Upload each ``(local_path, s3_uri)`` pair with the CRT transfer client.

    Blocks until every upload has finished; raises the first failure.
    """
    files = list(files)
    if not files:
        return
    bucket, _ = split_s3_uri(files[0][1])
    config = TransferConfig(
        preferred_transfer_client="crt",
        multipart_threshold=UPLOAD_PART_SIZE,
        multipart_chunksize=UPLOAD_PART_SIZE,
        max_concurrency=UPLOAD_CONCURRENCY,
    )
    with create_transfer_manager(_client(bucket), config) as manager:
        futures = []
        for path, uri in files:
            dest_bucket, key = split_s3_uri(uri)
            if dest_bucket != bucket:
                raise ValueError(f"Expected all uploads to target {bucket}: {uri}")
            futures.append(manager.upload(str(path), bucket, key))
        for future in futures:
            future.result()


def copy_objects(objects: Iterable[tuple[str, str]]) -> None:
    """Server-side copy each ``(source_uri, dest_uri)`` pair within S3.

    No data touches the local machine. Blocks until every copy has finished; raises
    the first failure.
    """
    objects = list(objects)
    if not objects:
        return
    dest_bucket, _ = split_s3_uri(objects[0][1])
    config = TransferConfig(
        multipart_threshold=COPY_PART_SIZE,
        multipart_chunksize=COPY_PART_SIZE,
        max_concurrency=COPY_CONCURRENCY,
    )
    with create_transfer_manager(_client(dest_bucket), config) as manager:
        futures = []
        for source_uri, dest_uri in objects:
            source_bucket, source_key = split_s3_uri(source_uri)
            bucket, key = split_s3_uri(dest_uri)
            if bucket != dest_bucket:
                raise ValueError(
                    f"Expected all copies to target {dest_bucket}: {dest_uri}"
                )
            futures.append(
                manager.copy({"Bucket": source_bucket, "Key": source_key}, bucket, key)
            )
        for future in futures:
            future.result()
