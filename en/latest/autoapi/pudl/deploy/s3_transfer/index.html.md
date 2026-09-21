# pudl.deploy.s3_transfer

Fast bulk uploads to, and server-side copies within, Amazon S3.

`s3fs` is fine for listing, deleting and small transfers, but it is several times
slower than `boto3` for the two operations that move the FERC EQR outputs
(~120 GiB, mostly 3-4 GiB Parquet files):

* Uploads: `s3fs` keeps at most 10 HTTP connections open by default, which limits
  throughput on the long-RTT GCP -> AWS path. The CRT transfer client in `boto3`
  saturates it.
* Server-side copies: `s3fs` copies each object with a single serial stream of
  50 MiB `UploadPartCopy` requests (~70 MB/s per object, whatever the block
  size), while the `boto3` transfer manager copies the parts of each object in
  parallel. On a 4-file benchmark it was ~6x faster (1.3 GB/s vs ~0.2 GB/s).

Everything else – listing, deletion, verification, and all non-S3 targets –
stays on `fsspec`/`UPath`. All `uri` arguments are full `s3://bucket/key`
URIs; callers pass the whole batch so one transfer manager is shared by all of
its files.

## Attributes

| [`MiB`](#pudl.deploy.s3_transfer.MiB)                |    |
|---------------------------------------------------------------------|----|
| [`UPLOAD_PART_SIZE`](#pudl.deploy.s3_transfer.UPLOAD_PART_SIZE)   |    |
| [`UPLOAD_CONCURRENCY`](#pudl.deploy.s3_transfer.UPLOAD_CONCURRENCY) |    |
| [`COPY_PART_SIZE`](#pudl.deploy.s3_transfer.COPY_PART_SIZE)     |    |
| [`COPY_CONCURRENCY`](#pudl.deploy.s3_transfer.COPY_CONCURRENCY)   |    |

## Functions

| [`split_s3_uri`](#pudl.deploy.s3_transfer.split_s3_uri)(→ tuple[str, str])   | Split an `s3://bucket/key` URI into `(bucket, key)`.                  |
|------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| [`_client`](#pudl.deploy.s3_transfer._client)(bucket)                   | Return a boto3 S3 client bound to *bucket*'s region.                  |
| [`upload_files`](#pudl.deploy.s3_transfer.upload_files)(→ None)              | Upload each `(local_path, s3_uri)` pair with the CRT transfer client. |
| [`copy_objects`](#pudl.deploy.s3_transfer.copy_objects)(→ None)              | Server-side copy each `(source_uri, dest_uri)` pair within S3.        |

## Module Contents

### pudl.deploy.s3_transfer.MiB *= 1048576*

### pudl.deploy.s3_transfer.UPLOAD_PART_SIZE *= 67108864*

### pudl.deploy.s3_transfer.UPLOAD_CONCURRENCY *= 64*

### pudl.deploy.s3_transfer.COPY_PART_SIZE *= 536870912*

### pudl.deploy.s3_transfer.COPY_CONCURRENCY *= 128*

### pudl.deploy.s3_transfer.split_s3_uri(uri: [str](https://docs.python.org/3/builtins/stdtypes.html#str)) → [tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]

Split an `s3://bucket/key` URI into `(bucket, key)`.

### pudl.deploy.s3_transfer.\_client(bucket: [str](https://docs.python.org/3/builtins/stdtypes.html#str))

Return a boto3 S3 client bound to *bucket*’s region.

The region is looked up with `get_bucket_location` (which needs no
redirect), so a missing or wrong `AWS_REGION` does not matter.

### pudl.deploy.s3_transfer.upload_files(files: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]]) → [None](https://docs.python.org/3/builtins/constants.html#None)

Upload each `(local_path, s3_uri)` pair with the CRT transfer client.

Blocks until every upload has finished; raises the first failure.

### pudl.deploy.s3_transfer.copy_objects(objects: [collections.abc.Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable)[[tuple](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)]]) → [None](https://docs.python.org/3/builtins/constants.html#None)

Server-side copy each `(source_uri, dest_uri)` pair within S3.

No data touches the local machine. Blocks until every copy has finished; raises
the first failure.
