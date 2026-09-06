"""Unit tests for :mod:`pudl.deploy.object_store`."""

import json

import pytest

from pudl.deploy import object_store
from pudl.deploy.object_store import (
    GcloudStorageObjectStore,
    LocalObjectStore,
    ObjectStore,
    S5cmdObjectStore,
)


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

    def test_s3_uses_s5cmd_with_region_from_env(self, mocker):
        mocker.patch.dict(
            object_store.os.environ,
            {"AWS_REGION": "", "AWS_DEFAULT_REGION": "us-west-2"},
            clear=False,
        )
        store = ObjectStore.for_uri("s3://pudl.catalyst.coop/ferceqr")
        assert isinstance(store, S5cmdObjectStore)
        assert store.region == "us-west-2"

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


class TestS5cmdObjectStore:
    """The S3 implementation builds the expected ``s5cmd`` command lines."""

    @pytest.fixture
    def run_cli(self, mocker):
        return mocker.patch.object(object_store, "_run_cli", return_value="")

    def test_upload_files_batches_into_run_script(self, run_cli, source_files):
        S5cmdObjectStore(region="us-west-2").upload_files(
            source_files, "s3://bucket/._staging/data"
        )
        args, kwargs = run_cli.call_args
        assert args[0][:1] == ["s5cmd"]
        assert args[0][-1] == "run"
        assert kwargs["env_overrides"] == {"AWS_REGION": "us-west-2"}
        stdin_lines = kwargs["stdin"].splitlines()
        assert stdin_lines == [
            f"cp {source_files[0]} s3://bucket/._staging/data/2013q3.parquet",
            f"cp {source_files[1]} s3://bucket/._staging/data/2013q4.parquet",
        ]

    def test_upload_files_no_sources_is_noop(self, run_cli):
        S5cmdObjectStore().upload_files([], "s3://bucket/x")
        run_cli.assert_not_called()

    def test_object_sizes_parses_json(self, run_cli):
        run_cli.return_value = (
            json.dumps(
                {
                    "key": "s3://bucket/stg/core_ferceqr__transactions/2013q3.parquet",
                    "type": "file",
                    "size": 123,
                }
            )
            + "\n"
            + json.dumps({"key": "s3://bucket/stg/x/", "type": "directory"})
            + "\n"
        )
        sizes = S5cmdObjectStore().object_sizes("s3://bucket/stg")
        assert sizes == {"core_ferceqr__transactions/2013q3.parquet": 123}

    def test_object_sizes_empty_prefix_returns_empty(self, run_cli):
        run_cli.return_value = json.dumps(
            {"operation": "ls", "error": "no object found"}
        )
        assert S5cmdObjectStore().object_sizes("s3://bucket/stg") == {}

    def test_object_sizes_other_error_raises(self, run_cli):
        run_cli.return_value = json.dumps(
            {"operation": "ls", "error": "BucketRegionError: incorrect region"}
        )
        with pytest.raises(RuntimeError, match="incorrect region"):
            S5cmdObjectStore().object_sizes("s3://bucket/stg")

    def test_move_uses_server_side_wildcard(self, run_cli):
        S5cmdObjectStore().move("s3://bucket/._staging/data", "s3://bucket/ferceqr")
        assert run_cli.call_args.args[0][-2:] == [
            "s3://bucket/._staging/data/*",
            "s3://bucket/ferceqr/",
        ]

    def test_sync_passes_delete_flag(self, run_cli):
        S5cmdObjectStore().sync("s3://bucket/ferceqr", "s3://bucket/._prev")
        cmd = run_cli.call_args.args[0]
        assert "sync" in cmd and "--delete" in cmd
        assert cmd[-2:] == ["s3://bucket/ferceqr/*", "s3://bucket/._prev/"]

    def test_remove_does_not_check(self, run_cli):
        S5cmdObjectStore().remove("s3://bucket/._staging")
        assert run_cli.call_args.kwargs["check"] is False


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
