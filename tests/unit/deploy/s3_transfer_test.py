"""Unit tests for :mod:`pudl.deploy.s3_transfer`."""

from pathlib import Path

import pytest

from pudl.deploy import s3_transfer


@pytest.fixture
def manager(mocker):
    """Patch out boto3: return the mock transfer manager used as a context manager."""
    mocker.patch.object(s3_transfer, "_client", return_value=mocker.sentinel.client)
    mgr = mocker.MagicMock()
    mgr.__enter__.return_value = mgr
    create = mocker.patch.object(
        s3_transfer, "create_transfer_manager", return_value=mgr
    )
    mgr.create = create
    return mgr


def test_split_s3_uri():
    assert s3_transfer.split_s3_uri("s3://bucket/a/b.parquet") == (
        "bucket",
        "a/b.parquet",
    )
    with pytest.raises(ValueError, match="Not an s3:// URI"):
        s3_transfer.split_s3_uri("gs://bucket/a")


def test_upload_files_uses_crt_and_waits_for_every_upload(manager):
    files = [
        (Path("/x/a.parquet"), "s3://b/p/a.parquet"),
        (Path("/x/b.parquet"), "s3://b/p/b.parquet"),
    ]
    s3_transfer.upload_files(files)

    client, config = manager.create.call_args.args
    assert client is not None
    assert config.preferred_transfer_client == "crt"
    assert config.max_concurrency == s3_transfer.UPLOAD_CONCURRENCY
    assert [c.args for c in manager.upload.call_args_list] == [
        ("/x/a.parquet", "b", "p/a.parquet"),
        ("/x/b.parquet", "b", "p/b.parquet"),
    ]
    assert manager.upload.return_value.result.call_count == 2


def test_upload_failure_propagates(manager):
    manager.upload.return_value.result.side_effect = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        s3_transfer.upload_files([(Path("/x/a"), "s3://b/a")])


def test_copy_objects_uses_large_multipart_parts(manager):
    s3_transfer.copy_objects([("s3://b/src/a", "s3://b/dst/a")])

    _, config = manager.create.call_args.args
    assert config.multipart_chunksize == s3_transfer.COPY_PART_SIZE
    assert config.max_concurrency == s3_transfer.COPY_CONCURRENCY
    manager.copy.assert_called_once_with({"Bucket": "b", "Key": "src/a"}, "b", "dst/a")


@pytest.mark.parametrize("fn", [s3_transfer.upload_files, s3_transfer.copy_objects])
def test_empty_batches_are_noops(manager, fn):
    fn([])
    manager.create.assert_not_called()


def test_mixed_destination_buckets_are_rejected(manager):
    with pytest.raises(ValueError, match="target b1"):
        s3_transfer.upload_files(
            [(Path("/x/a"), "s3://b1/a"), (Path("/x/b"), "s3://b2/b")]
        )
