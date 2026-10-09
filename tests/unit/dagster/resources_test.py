"""Unit tests for Dagster resources."""

import hashlib
import json
import os
import zipfile
from pathlib import Path

import pytest
import yaml
from dagster import build_init_resource_context
from requests import HTTPError
from upath import UPath

from pudl.dagster import resources as dagster_resources
from pudl.dagster.resources import (
    FercEqrArchiveResource,
    FercEqrDeploymentResource,
    FercEqrDeploymentTargetConfig,
    ZulipNotificationResource,
)
from pudl.workspace.datastore import ChecksumMismatchError, Datastore


@pytest.fixture
def inert_datastore(tmp_path) -> Datastore:
    """A real Datastore with only a local cache layer, so it never hits the network.

    Constructing a Datastore does no I/O, and these tests all patch the descriptor
    lookup, so nothing is ever fetched.
    """
    return Datastore(local_cache_path=tmp_path / "input", cloud_cache_path=None)


@pytest.fixture
def stub_descriptor(mocker):
    """Stub the ferceqr datapackage descriptor, verifying any file successfully.

    Lets these tests build throwaway archives that have no counterpart in the
    published Zenodo metadata. The stub hashes whatever file it's handed and reports
    it as a match, so the digest it returns is real rather than a hardcoded constant.
    Whether the comparison against the published hash is correct is the job of the
    ``validate_file_checksum`` tests in ``tests/unit/workspace/datastore_test.py``;
    what's under test here is that the resource invokes it on the right file.
    """

    def verify(name: str, path: str | Path) -> str:
        del name  # Only the file's contents matter to the stub.
        with Path(path).open("rb") as f:
            return hashlib.file_digest(f, "md5").hexdigest()  # noqa: S324

    descriptor = mocker.MagicMock()
    descriptor.validate_file_checksum.side_effect = verify
    mocker.patch.object(
        Datastore, "get_datapackage_descriptor", return_value=descriptor
    )
    return descriptor


def _quarter_archive(tmp_path) -> UPath:
    """Create an archive directory holding one minimal quarterly zip."""
    archive_dir = UPath(tmp_path / "archive")
    archive_dir.mkdir()
    with zipfile.ZipFile(tmp_path / "archive" / "ferceqr-2024q1.zip", "w") as outer:
        outer.writestr("filing1.zip", b"payload")
    return archive_dir


def test_open_quarter_archive_streams_to_disk_without_reading_into_memory(
    tmp_path, mocker, inert_datastore, stub_descriptor
):
    """The archive is fetched with the filesystem's streaming get_file, not read_bytes.

    Quarterly archives are 3-4 GB and many partition runs download at once, so
    holding a whole archive in memory before writing it out risks exhausting RAM.
    """
    archive_dir = _quarter_archive(tmp_path)
    read_bytes = mocker.patch.object(
        type(archive_dir), "read_bytes", side_effect=AssertionError("read into memory")
    )
    get_file = mocker.spy(archive_dir.fs, "get_file")
    resource = FercEqrArchiveResource(datastore=inert_datastore, path=str(archive_dir))

    with resource.open_quarter_archive("2024q1") as archive:
        assert archive.namelist() == ["filing1.zip"]
        assert archive.read("filing1.zip") == b"payload"
        assert archive.filename is not None
        local_zip = Path(archive.filename)

    get_file.assert_called_once()
    read_bytes.assert_not_called()
    stub_descriptor.validate_file_checksum.assert_called_once()
    # The local copy lives in a temporary directory that is removed on exit.
    assert not local_zip.exists()


def test_open_quarter_archive_checksums_the_local_copy(
    tmp_path, inert_datastore, stub_descriptor
):
    """Verification runs against the downloaded file, keyed by the archive's own name.

    FERC EQR is too large for Zenodo and lives only on GCS, so the md5 recorded in the
    metadata-only Zenodo archive is the sole integrity check available. It has to be
    applied to the local copy on disk, since a 3-4 GB archive can't be hashed in
    memory.
    """
    archive_dir = _quarter_archive(tmp_path)
    resource = FercEqrArchiveResource(datastore=inert_datastore, path=str(archive_dir))

    with resource.open_quarter_archive("2024q1"):
        name, verified_path = stub_descriptor.validate_file_checksum.call_args[0]
        assert name == "ferceqr-2024q1.zip"
        # The temp copy, not the source archive, and it's still present to be hashed.
        assert Path(verified_path).exists()
        assert Path(verified_path) != Path(archive_dir / "ferceqr-2024q1.zip")


def test_open_quarter_archive_raises_on_checksum_mismatch(
    tmp_path, inert_datastore, stub_descriptor
):
    """A mismatch fails the extraction rather than yielding unverified bytes."""
    archive_dir = _quarter_archive(tmp_path)
    stub_descriptor.validate_file_checksum.side_effect = ChecksumMismatchError(
        "mismatch"
    )
    resource = FercEqrArchiveResource(datastore=inert_datastore, path=str(archive_dir))

    with (
        pytest.raises(ChecksumMismatchError),
        resource.open_quarter_archive("2024q1"),
    ):
        pytest.fail("Archive should not be yielded when verification fails.")


def test_ferceqr_deployment_targets_resource_loads_config_values(tmp_path):
    """Deployment targets resource should expose configured target paths and options."""
    resource = FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(
                path="gs://my-bucket",
                storage_options={
                    "project": "my-billing-project",
                    "requester_pays": True,
                },
            ),
            FercEqrDeploymentTargetConfig(path="s3://my-bucket"),
        ],
    )

    assert len(resource.deployment_targets) == 2
    assert resource.deployment_targets[0].path == "gs://my-bucket"
    assert (
        resource.deployment_targets[0].storage_options["project"]
        == "my-billing-project"
    )
    assert resource.deployment_targets[0].storage_options["requester_pays"] is True
    assert resource.deployment_targets[1].path == "s3://my-bucket"
    assert resource.deployment_targets[1].storage_options == {}


@pytest.mark.parametrize(
    "path_fn",
    [
        lambda p: str(p),
        lambda p: p.as_uri(),
    ],
    ids=["filesystem_path", "file_uri"],
)
def test_ferceqr_deployment_target_config_accepts_local_path_formats(tmp_path, path_fn):
    """Deployment targets should accept local directories as str or file:// URI."""
    deploy_path = tmp_path / "deploy"
    deploy_path.mkdir()
    target = FercEqrDeploymentTargetConfig(path=path_fn(deploy_path))

    assert target.path == path_fn(deploy_path)


@pytest.mark.parametrize(
    "path",
    [
        "https://example.com/ferceqr",
        "s3://",
        "gs://",
        "file://relative/path",
        "   ",
        "relative/path",
    ],
)
def test_ferceqr_deployment_target_config_rejects_invalid_paths(path):
    """Deployment targets should reject unsupported URL schemes and empty bucket URLs."""
    with pytest.raises(ValueError):
        FercEqrDeploymentTargetConfig(path=path)


def test_ferceqr_deployment_target_config_rejects_missing_local_directory(tmp_path):
    """Deployment targets should reject local directories that do not exist."""
    with pytest.raises(ValueError, match="does not exist"):
        FercEqrDeploymentTargetConfig(path=str(tmp_path / "missing"))


def test_ferceqr_deployment_target_config_rejects_local_file(tmp_path):
    """Deployment targets should reject local paths that are not directories."""
    local_file = tmp_path / "deploy.txt"
    local_file.write_text("not a directory")

    with pytest.raises(ValueError, match="is not a directory"):
        FercEqrDeploymentTargetConfig(path=str(local_file))


def test_ferceqr_deployment_target_config_rejects_unwritable_directory(
    tmp_path, monkeypatch
):
    """Deployment targets should reject local directories that are not writable."""
    deploy_path = tmp_path / "deploy"
    deploy_path.mkdir()

    def fake_access(path, mode):
        if Path(path) == deploy_path and mode == os.W_OK:
            return False
        return os.access(path, mode)

    monkeypatch.setattr("pudl.dagster.resources.os.access", fake_access)

    with pytest.raises(ValueError, match="is not writable"):
        FercEqrDeploymentTargetConfig(path=str(deploy_path))


def test_ferceqr_deployment_resource_defaults_to_no_targets(monkeypatch, tmp_path):
    """Resource with no explicit targets or config path should skip deployment."""
    monkeypatch.delenv("PUDL_FERCEQR_DEPLOYMENT_CONFIG_PATH", raising=False)
    resource = FercEqrDeploymentResource()

    configured_targets = resource.configured_targets()

    assert configured_targets == []


def test_ferceqr_deployment_resource_loads_override_config_path(tmp_path):
    """Resource should support overriding deployment targets with an alternate YAML file."""
    deployment_config_path = tmp_path / "ferceqr_targets.yml"
    deploy_path = tmp_path / "deploy"
    deploy_path.mkdir()
    deployment_config_path.write_text(
        yaml.safe_dump(
            {
                "deployment_targets": [
                    {
                        "path": str(deploy_path),
                        "storage_options": {},
                    }
                ]
            }
        )
    )

    resource = FercEqrDeploymentResource(
        deployment_config_path=str(deployment_config_path),
    )

    targets = resource.resolved_targets()

    assert targets == [UPath(deploy_path)]


def test_ferceqr_deployment_resource_appends_build_id(monkeypatch):
    """Targets can opt into build-scoped destinations by appending BUILD_ID."""
    monkeypatch.setenv("BUILD_ID", "build-123")
    resource = FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(
                path="gs://test.catalyst.coop",
                append_build_id=True,
            )
        ]
    )

    assert resource.resolved_targets() == [
        UPath("gs://test.catalyst.coop") / "build-123"
    ]


def test_ferceqr_deployment_resource_requires_build_id_for_appended_targets(
    monkeypatch,
):
    """Build-scoped targets should fail if BUILD_ID is unavailable at runtime."""
    monkeypatch.delenv("BUILD_ID", raising=False)
    resource = FercEqrDeploymentResource(
        deployment_targets=[
            FercEqrDeploymentTargetConfig(
                path="gs://test.catalyst.coop",
                append_build_id=True,
            )
        ]
    )

    with pytest.raises(ValueError, match="BUILD_ID must be set"):
        resource.resolved_targets()


@pytest.fixture
def zulip_resource() -> ZulipNotificationResource:
    return ZulipNotificationResource.from_resource_context(
        build_init_resource_context(
            config={
                "base_url": "https://zulip.example.com",
                "bot_email": "bot@example.com",
                "api_key": "test-key",  # pragma: allowlist secret
                "timeout_seconds": 9,  # non-standard timeout to test that it propagates
            }
        )
    )


def test_zulip_notification_resource_sends_stream_message_successfully(
    mocker, zulip_resource
):
    """Zulip resource should post expected payload and return API response."""
    response = mocker.Mock()
    response.json.return_value = {"result": "success", "id": 123}
    post = mocker.patch.object(
        dagster_resources.requests,
        "post",
        return_value=response,
    )

    payload = zulip_resource.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-ferceqr",
        content="hello",
    )

    assert payload == {"result": "success", "id": 123}
    post.assert_called_once_with(
        "https://zulip.example.com/api/v1/messages",
        auth=("bot@example.com", "test-key"),
        data={
            "type": "stream",
            "to": "pudl-deployments",
            "topic": "build-deploy-ferceqr",
            "content": "hello",
        },
        timeout=9,
    )
    response.raise_for_status.assert_called_once_with()


@pytest.mark.parametrize(
    "error,expected_msg",
    [
        (HTTPError("bad gateway"), "bad gateway"),
        (HTTPError("Connection refused"), "Connection refused"),
    ],
    ids=["raise_for_status", "post_side_effect"],
)
def test_zulip_notification_resource_returns_error_on_request_exception(
    mocker, zulip_resource, error, expected_msg
):
    """Zulip resource should return error dict on any requests.RequestException, not raise."""
    if error.args[0] == "bad gateway":
        # Simulate raise_for_status failure
        response = mocker.Mock()
        response.raise_for_status.side_effect = error
        mocker.patch.object(dagster_resources.requests, "post", return_value=response)
    else:
        # Simulate post itself failing
        mocker.patch.object(dagster_resources.requests, "post", side_effect=error)

    payload = zulip_resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert expected_msg in payload["msg"]


def test_zulip_notification_resource_handles_bad_json(mocker, zulip_resource):
    """Zulip resource should handle non-JSON 200 response without crashing."""
    response = mocker.Mock()
    response.json.side_effect = json.JSONDecodeError("bad json", "", 0)
    mocker.patch.object(dagster_resources.requests, "post", return_value=response)

    payload = zulip_resource.send_stream_message(
        stream="stream", topic="topic", content="body"
    )

    assert payload["result"] == "error"
    assert "invalid JSON" in payload["msg"]
    assert "response_text" in payload


def test_zulip_notification_resource_uploads_and_sends_message(
    mocker, tmp_path, zulip_resource
):
    """Zulip resource should upload file and append link to message content."""
    # Create a temp file to "upload"
    csv_file = tmp_path / "status.csv"
    csv_file.write_text("table,row_count\ncore_ferceqr__contracts,42\n")

    # Mock the upload POST
    upload_response = mocker.Mock()
    upload_response.json.return_value = {
        "result": "success",
        "url": "/user_uploads/1/abc/status.csv",
        "filename": "status.csv",
    }

    # Mock the message POST
    msg_response = mocker.Mock()
    msg_response.json.return_value = {"result": "success", "id": 456}

    mocked_post = mocker.patch.object(
        dagster_resources.requests,
        "post",
        side_effect=[upload_response, msg_response],
    )

    payload = zulip_resource.send_stream_message(
        stream="pudl-deployments",
        topic="build-deploy-ferceqr",
        content="Build failed!",
        file_path=str(csv_file),
    )

    assert payload == {"result": "success", "id": 456}

    # First call: upload
    assert (
        mocked_post.call_args_list[0]
        .args[0]
        .startswith("https://zulip.example.com/api/v1/user_uploads")
    )
    assert "file" in mocked_post.call_args_list[0].kwargs["files"]

    # Second call: message with appended download link
    second_call = mocked_post.call_args_list[1]
    assert second_call.args[0] == "https://zulip.example.com/api/v1/messages"
    msg_data = second_call.kwargs["data"]
    assert msg_data["content"].endswith(
        "[status.csv](https://zulip.example.com/user_uploads/1/abc/status.csv)"
    )
