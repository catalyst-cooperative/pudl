"""Unit tests for :mod:`pudl.deploy.object_store`."""

import json
from pathlib import Path

import pytest

from pudl.deploy import object_store
from pudl.deploy.object_store import (
    GcloudStorageObjectStore,
    LocalObjectStore,
    ObjectStore,
    S3ObjectStore,
)


class _DoneFuture:
    """Stand-in for a transfer-manager future that has already completed."""

    def result(self):
        return None


class _FakeS3Client:
    """A tiny in-memory S3 for exercising :class:`S3ObjectStore` without a network."""

    def __init__(self, objects=None, location="us-west-2"):
        # {(bucket, key): size}
        self.objects = dict(objects or {})
        self._location = location

    def get_bucket_location(self, Bucket):  # noqa: N803 (boto3 kwarg name)
        return {"LocationConstraint": self._location}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        client = self

        class _Paginator:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                yield {
                    "Contents": [
                        {"Key": key, "Size": size}
                        for (bucket, key), size in client.objects.items()
                        if bucket == Bucket and key.startswith(Prefix)
                    ]
                }

        return _Paginator()

    def delete_objects(self, Bucket, Delete):  # noqa: N803
        for obj in Delete["Objects"]:
            self.objects.pop((Bucket, obj["Key"]), None)


class _FakeTransferManager:
    """Applies uploads/copies straight to the backing :class:`_FakeS3Client`."""

    def __init__(self, client):
        self.client = client

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def upload(self, filename, bucket, key):
        self.client.objects[(bucket, key)] = Path(filename).stat().st_size
        return _DoneFuture()

    def copy(self, copy_source, bucket, key):
        size = self.client.objects[(copy_source["Bucket"], copy_source["Key"])]
        self.client.objects[(bucket, key)] = size
        return _DoneFuture()


@pytest.fixture
def source_files(tmp_path):
    """Create a couple of local files to upload and return their paths."""
    src = tmp_path / "src"
    src.mkdir()
    paths = []
    for name, size in (("2013q3.parquet", 10), ("2013q4.parquet", 20)):
        path = src / name
        path.write_bytes(b"x" * size)
        paths.append(path)
    return paths


class TestForUri:
    """``ObjectStore.for_uri`` dispatches on the URI scheme."""

    def test_s3_uses_boto3_with_bucket_and_region_from_env(self, mocker):
        mocker.patch.dict(
            object_store.os.environ,
            {"AWS_REGION": "", "AWS_DEFAULT_REGION": "us-west-2"},
            clear=False,
        )
        store = ObjectStore.for_uri("s3://pudl.catalyst.coop/ferceqr")
        assert isinstance(store, S3ObjectStore)
        assert store.bucket == "pudl.catalyst.coop"
        assert store._region == "us-west-2"

    def test_gs_requester_pays_pulls_billing_project(self, mocker):
        mocker.patch.dict(
            object_store.os.environ, {"GCP_BILLING_PROJECT": "my-project"}, clear=False
        )
        store = ObjectStore.for_uri(
            "gs://pudl.catalyst.coop/ferceqr", storage_options={"requester_pays": True}
        )
        assert isinstance(store, GcloudStorageObjectStore)
        assert store.billing_project == "my-project"

    def test_gs_without_requester_pays_has_no_billing_project(self):
        store = ObjectStore.for_uri("gs://pudl.catalyst.coop/ferceqr")
        assert isinstance(store, GcloudStorageObjectStore)
        assert store.billing_project is None

    @pytest.mark.parametrize("uri", ["/srv/pudl/deploy", "file:///srv/pudl/deploy"])
    def test_local_paths(self, uri):
        assert isinstance(ObjectStore.for_uri(uri), LocalObjectStore)

    def test_unknown_scheme_raises(self):
        with pytest.raises(ValueError, match="No ObjectStore implementation"):
            ObjectStore.for_uri("ftp://example.com/x")


class TestLocalObjectStore:
    """The filesystem implementation round-trips real files."""

    def test_upload_and_object_sizes(self, tmp_path, source_files):
        store = LocalObjectStore()
        dest = tmp_path / "dest" / "data"
        store.upload_files(source_files, str(dest))
        assert store.object_sizes(str(dest)) == {
            "2013q3.parquet": 10,
            "2013q4.parquet": 20,
        }

    def test_object_sizes_missing_prefix_is_empty(self, tmp_path):
        assert LocalObjectStore().object_sizes(str(tmp_path / "nope")) == {}

    def test_sync_replaces_destination(self, tmp_path, source_files):
        store = LocalObjectStore()
        src = tmp_path / "live"
        store.upload_files(source_files, str(src))
        dest = tmp_path / "previous"
        (dest).mkdir()
        (dest / "stale.parquet").write_bytes(b"old")
        store.sync(str(src), str(dest))
        assert set(store.object_sizes(str(dest))) == {
            "2013q3.parquet",
            "2013q4.parquet",
        }

    def test_move_empties_source_and_preserves_layout(self, tmp_path, source_files):
        store = LocalObjectStore()
        staging = tmp_path / "staging" / "data" / "core_ferceqr__transactions"
        store.upload_files(source_files, str(staging))
        final = tmp_path / "final"
        store.move(str(tmp_path / "staging" / "data"), str(final))
        assert store.object_sizes(str(final)) == {
            "core_ferceqr__transactions/2013q3.parquet": 10,
            "core_ferceqr__transactions/2013q4.parquet": 20,
        }
        assert store.object_sizes(str(tmp_path / "staging" / "data")) == {}

    def test_remove_is_idempotent(self, tmp_path, source_files):
        store = LocalObjectStore()
        target = tmp_path / "staging"
        store.upload_files(source_files, str(target))
        store.remove(str(target))
        store.remove(str(target))
        assert not target.exists()


class TestS3ObjectStore:
    """The S3 implementation drives boto3 list / copy / delete correctly."""

    @pytest.fixture
    def store(self, mocker):
        """An S3ObjectStore wired to an in-memory fake client and transfer manager."""
        client = _FakeS3Client()
        mocker.patch.object(
            object_store,
            "create_transfer_manager",
            side_effect=lambda client, config: _FakeTransferManager(client),
        )
        s3 = S3ObjectStore("bucket", region="us-west-2")
        s3._client_cache = client
        return s3

    def test_detect_region_defaults_none_to_us_east_1(self, mocker):
        mocker.patch.object(
            object_store.boto3, "client", return_value=_FakeS3Client(location=None)
        )
        assert S3ObjectStore("bucket")._detect_region() == "us-east-1"

    def test_upload_files_places_objects_under_prefix(self, store, source_files):
        store.upload_files(source_files, "s3://bucket/._staging/data")
        assert store.object_sizes("s3://bucket/._staging/data") == {
            "2013q3.parquet": 10,
            "2013q4.parquet": 20,
        }

    def test_upload_files_no_sources_is_noop(self, mocker):
        create_manager = mocker.patch.object(object_store, "create_transfer_manager")
        s3 = S3ObjectStore("bucket", region="us-west-2")
        s3._client_cache = _FakeS3Client()
        s3.upload_files([], "s3://bucket/x")
        create_manager.assert_not_called()

    def test_object_sizes_strips_prefix_and_placeholder(self, store):
        store._client_cache.objects = {
            ("bucket", "stg/"): 0,
            ("bucket", "stg/core_ferceqr__contracts/2013q3.parquet"): 123,
        }
        assert store.object_sizes("s3://bucket/stg") == {
            "core_ferceqr__contracts/2013q3.parquet": 123
        }

    def test_object_sizes_empty_prefix_returns_empty(self, store):
        assert store.object_sizes("s3://bucket/stg") == {}

    def test_move_copies_then_empties_source(self, store):
        store._client_cache.objects = {
            ("bucket", "._staging/data/t/a.parquet"): 5,
            ("bucket", "._staging/data/t/b.parquet"): 7,
        }
        store.move("s3://bucket/._staging/data", "s3://bucket/ferceqr")
        assert store.object_sizes("s3://bucket/ferceqr") == {
            "t/a.parquet": 5,
            "t/b.parquet": 7,
        }
        assert store.object_sizes("s3://bucket/._staging/data") == {}

    def test_sync_copies_changed_and_deletes_extras(self, store):
        store._client_cache.objects = {
            ("bucket", "src/keep.parquet"): 10,
            ("bucket", "src/changed.parquet"): 20,
            ("bucket", "dst/keep.parquet"): 10,
            ("bucket", "dst/changed.parquet"): 99,
            ("bucket", "dst/stale.parquet"): 3,
        }
        store.sync("s3://bucket/src", "s3://bucket/dst")
        assert store.object_sizes("s3://bucket/dst") == {
            "keep.parquet": 10,
            "changed.parquet": 20,
        }

    def test_remove_empties_the_prefix(self, store):
        store._client_cache.objects = {
            ("bucket", "._staging/a"): 1,
            ("bucket", "._staging/b/c"): 2,
            ("bucket", "keep/d"): 3,
        }
        store.remove("s3://bucket/._staging")
        assert store._client_cache.objects == {("bucket", "keep/d"): 3}

    def test_remove_missing_prefix_is_noop(self, store):
        store.remove("s3://bucket/nothing-here")


class TestGcloudStorageObjectStore:
    """The GCS implementation builds the expected ``gcloud storage`` command lines."""

    @pytest.fixture
    def run_cli(self, mocker):
        return mocker.patch.object(object_store, "_run_cli", return_value="")

    def test_upload_files_single_cp_invocation(self, run_cli, source_files):
        GcloudStorageObjectStore().upload_files(
            source_files, "gs://bucket/._staging/data"
        )
        cmd = run_cli.call_args.args[0]
        assert cmd[:3] == ["gcloud", "storage", "cp"]
        assert cmd[-1] == "gs://bucket/._staging/data/"
        assert cmd[3:-1] == [str(p) for p in source_files]

    def test_billing_project_flag_added(self, run_cli, source_files):
        GcloudStorageObjectStore(billing_project="proj").upload_files(
            source_files, "gs://bucket/x"
        )
        assert "--billing-project=proj" in run_cli.call_args.args[0]

    def test_object_sizes_parses_json(self, run_cli):
        run_cli.return_value = json.dumps(
            [
                {
                    "storage_url": (
                        "gs://bucket/stg/core_ferceqr__contracts/2013q3.parquet#171"
                    ),
                    "size": 123,
                },
                {
                    "storage_url": "gs://bucket/stg/ferceqr_parquet_datapackage.json#5",
                    "size": 7,
                },
                {"storage_url": "gs://bucket/stg/subdir/#1", "size": 0},
            ]
        )
        sizes = GcloudStorageObjectStore().object_sizes("gs://bucket/stg")
        assert sizes == {
            "core_ferceqr__contracts/2013q3.parquet": 123,
            "ferceqr_parquet_datapackage.json": 7,
        }
        cmd = run_cli.call_args.args[0]
        assert cmd[2:5] == ["objects", "list", "gs://bucket/stg/**"]
        assert "--format=json" in cmd

    def test_object_sizes_empty_prefix_returns_empty(self, run_cli):
        run_cli.return_value = "[]"
        assert GcloudStorageObjectStore().object_sizes("gs://bucket/stg") == {}

    def test_move_uses_wildcard_source_without_recursive_flag(self, run_cli):
        # `gcloud storage mv` recurses on its own and rejects --recursive.
        GcloudStorageObjectStore().move(
            "gs://bucket/._staging/data", "gs://bucket/ferceqr"
        )
        cmd = run_cli.call_args.args[0]
        assert cmd[2:] == [
            "mv",
            "gs://bucket/._staging/data/*",
            "gs://bucket/ferceqr/",
        ]

    def test_sync_delete_unmatched(self, run_cli):
        GcloudStorageObjectStore().sync("gs://bucket/ferceqr", "gs://bucket/._prev")
        cmd = run_cli.call_args.args[0]
        assert "rsync" in cmd and "--delete-unmatched-destination-objects" in cmd


class TestRunCli:
    """``_run_cli`` surfaces failures and honors ``check``."""

    def test_raises_on_nonzero_when_checked(self):
        with pytest.raises(RuntimeError, match="Command failed"):
            object_store._run_cli(["false"])

    def test_returns_stdout_when_not_checked(self):
        out = object_store._run_cli(["sh", "-c", "echo hi; exit 3"], check=False)
        assert out.strip() == "hi"

    def test_env_overrides_are_merged(self):
        out = object_store._run_cli(
            ["sh", "-c", "echo $PUDL_OBJ_STORE_TEST"],
            env_overrides={"PUDL_OBJ_STORE_TEST": "value"},
        )
        assert out.strip() == "value"
