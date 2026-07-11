"""Test distribution logic for ETL outputs."""

import hashlib
import threading
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from pydantic import ValidationError
from upath import UPath

from pudl.deploy.pudl import (
    DeploymentPlan,
    DeploymentType,
    DeployStage,
    StageResult,
    StageStatus,
    build_deploy_zulip_message,
    clear_deployment_path,
    dispatch_github_workflow,
    download_build_outputs,
    get_build_from_tag,
    get_deployment_type_from_tag,
    new_deploy_stage_results,
    prepare_outputs_for_distribution,
    run_stage,
    send_zulip_message,
    trigger_zenodo_release,
    update_git_branch,
    update_pudl_viewer,
    upload_outputs,
)


def test_download_build_outputs_copies_raw_files(tmp_path):
    """download_build_outputs should copy every file from build_path to local_path."""
    build_path = tmp_path / "2026-07-04-0600-abc123456-main"
    build_path.mkdir()
    (build_path / "pudl.sqlite").write_text("db content")
    nested = build_path / "parquet"
    nested.mkdir()
    (nested / "table1.parquet").write_text("p1")

    local_path = tmp_path / "local"
    local_path.mkdir()

    download_build_outputs(local_path, UPath(build_path))

    assert (local_path / "pudl.sqlite").read_text() == "db content"
    assert (local_path / "parquet" / "table1.parquet").read_text() == "p1"


def test_prepare_outputs_for_distribution(tmp_path):
    """Test complete output preparation workflow.

    Operates directly on ``output_dir`` -- ``prepare_outputs_for_distribution``
    assumes ``download_build_outputs`` has already populated it and no longer does
    any downloading itself. ``build_path`` is only used to derive the build ID, so
    it doesn't need to exist on disk.
    """
    output_dir = tmp_path / "output"
    build_id = "2026-07-04-0600-abc123456-main"
    build_path = UPath(tmp_path / build_id)
    output_dir.mkdir()
    parquet_dirs = [
        output_dir / "parquet",
        output_dir / "ferc1_dbf",
        output_dir / "ferc2_dbf",
        output_dir / "ferc6_dbf",
        output_dir / "ferc60_dbf",
        output_dir / "ferc1_xbrl",
        output_dir / "ferc2_xbrl",
        output_dir / "ferc6_xbrl",
        output_dir / "ferc60_xbrl",
        output_dir / "ferc714_xbrl",
    ]

    (output_dir / "pudl.sqlite").write_text("db content")
    (output_dir / "ferc1.sqlite").write_text("ferc db")
    (output_dir / "pudl_dbt_tests.duckdb").write_text("test db")
    (output_dir / f"{build_id}.log").write_text("build log")
    (output_dir / f"{build_id}-deploy-2026-07-04-0700.log").write_text("deploy log")
    (output_dir / "success").write_text("")

    for parquet_dir in parquet_dirs:
        parquet_dir.mkdir()
        (parquet_dir / "table1.parquet").write_text("p1")
        (parquet_dir / "table2.parquet").write_text("p2")
        (parquet_dir / "datapackage.json").write_text("{}")

    prepare_outputs_for_distribution(output_dir, build_path)

    # SQLite files compressed and originals removed
    assert (output_dir / "pudl.sqlite.zip").exists()
    assert (output_dir / "ferc1.sqlite.zip").exists()
    assert not (output_dir / "pudl.sqlite").exists()
    assert not (output_dir / "ferc1.sqlite").exists()

    # Parquet files moved to root
    assert (output_dir / "table1.parquet").exists()
    assert (output_dir / "table2.parquet").exists()
    assert (output_dir / "pudl_parquet_datapackage.json").exists()

    # Parquet archive created
    assert (output_dir / "pudl_parquet.zip").exists()
    for parquet_zip in [
        output_dir / "pudl_parquet.zip",
        output_dir / "ferc1_dbf.zip",
        output_dir / "ferc2_dbf.zip",
        output_dir / "ferc6_dbf.zip",
        output_dir / "ferc60_dbf.zip",
        output_dir / "ferc1_xbrl.zip",
        output_dir / "ferc2_xbrl.zip",
        output_dir / "ferc6_xbrl.zip",
        output_dir / "ferc60_xbrl.zip",
        output_dir / "ferc714_xbrl.zip",
    ]:
        with zipfile.ZipFile(parquet_zip) as zf:
            names = zf.namelist()
            assert "table1.parquet" in names
            assert "table2.parquet" in names
            assert "datapackage.json" in names

    assert not (output_dir / "pudl_dbt_tests.duckdb").exists()
    assert not (output_dir / "parquet").exists()

    # A marker file named after (and containing) the build ID gives distributed
    # outputs provenance, now that the build log's filename is no longer
    # distributed. It can't be empty -- Zenodo rejects zero-byte uploads.
    marker = output_dir / build_id
    assert marker.exists()
    assert marker.stat().st_size > 0
    assert marker.read_text() == build_id

    # Build/deploy logs and the "success" sentinel must never be distributed. Logs
    # live alongside the real outputs under builds.catalyst.coop (so a
    # ``fs.get(..., recursive=True)`` pulls them down too), but they can contain
    # stack traces or other details we don't want to expose publicly. The
    # "success" sentinel is internal build-completion plumbing with no meaning
    # for consumers of the distributed outputs.
    assert list(output_dir.glob("*.log")) == []
    assert not (output_dir / "success").exists()


def test_upload_outputs_nightly(tmp_path):
    """Test upload to nightly paths (production)."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "pudl.sqlite.zip").write_text("db")
    (source_dir / "table1.parquet").write_text("p1")

    path_suffixes = ["nightly", "eel-hole"]

    with (
        patch("pudl.deploy.pudl.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.deploy.pudl.s3fs.S3FileSystem") as mock_s3_cls,
    ):
        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs.exists.return_value = True
        mock_s3.exists.return_value = True
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, path_suffixes)

        # Suffixes now upload concurrently, so call order isn't guaranteed -- assert
        # on the set of paths touched rather than positional call order.
        assert mock_gcs.put.call_count == 2
        assert {c.args[1] for c in mock_gcs.put.call_args_list} == {
            "gs://pudl.catalyst.coop/nightly/",
            "gs://pudl.catalyst.coop/eel-hole/",
        }

        assert mock_s3.put.call_count == 2
        assert {c.args[1] for c in mock_s3.put.call_args_list} == {
            "s3://pudl.catalyst.coop/nightly/",
            "s3://pudl.catalyst.coop/eel-hole/",
        }

        # Both filesystems should have been cleared before the corresponding put().
        # `call`/dict objects aren't hashable, so compare extracted paths and
        # kwargs separately rather than putting the raw calls in a set.
        assert all(c.kwargs == {"recursive": True} for c in mock_gcs.rm.call_args_list)
        assert {c.args[0] for c in mock_gcs.rm.call_args_list} == {
            "gs://pudl.catalyst.coop/nightly/",
            "gs://pudl.catalyst.coop/eel-hole/",
        }
        assert all(c.kwargs == {"recursive": True} for c in mock_s3.rm.call_args_list)
        assert {c.args[0] for c in mock_s3.rm.call_args_list} == {
            "s3://pudl.catalyst.coop/nightly/",
            "s3://pudl.catalyst.coop/eel-hole/",
        }


def test_upload_outputs_runs_targets_concurrently(tmp_path):
    """The 4 (suffix x destination) uploads should run concurrently, not serially.

    A ``threading.Barrier(4, timeout=5)`` is a rendezvous point: each thread that
    calls ``barrier.wait()`` blocks until exactly 4 threads have called it, at
    which point all 4 are released at (approximately) the same instant. Here, the
    mocked ``put()`` calls ``barrier.wait()`` instead of actually uploading, so the
    test only passes if all 4 uploads are genuinely in flight at once, each
    blocked on the same barrier.

    If ``upload_outputs`` actually ran uploads one at a time, only 1 of the 4
    required threads would ever reach the barrier at any given moment -- the other
    3 wouldn't have started yet -- so ``wait()`` would never see its 4th caller and
    would block until the 5 second timeout elapses, raising
    ``threading.BrokenBarrierError`` and failing the test. This makes the test a
    deterministic proof of real concurrency (it fails fast and reliably if the
    implementation is secretly serial) rather than a flaky wall-clock timing
    comparison (e.g. asserting total elapsed time is less than N seconds), which
    can pass or fail depending on unrelated system load.
    """
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "table1.parquet").write_text("p1")

    barrier = threading.Barrier(4, timeout=5)

    def _blocking_put(*args, **kwargs):
        barrier.wait()

    with (
        patch("pudl.deploy.pudl.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.deploy.pudl.s3fs.S3FileSystem") as mock_s3_cls,
    ):
        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs.exists.return_value = False
        mock_s3.exists.return_value = False
        mock_gcs.put.side_effect = _blocking_put
        mock_s3.put.side_effect = _blocking_put
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, ["nightly", "eel-hole"])

        assert mock_gcs.put.call_count == 2
        assert mock_s3.put.call_count == 2


def test_upload_outputs_skips_clearing_immutable_suffix(tmp_path):
    """A suffix listed as immutable should never be cleared before upload.

    The permanent path must not exist yet (see
    ``test_upload_outputs_raises_if_permanent_path_already_exists`` for the case
    where it does), so ``exists()`` is mocked to return ``False`` only for that
    path -- the rolling "stable" path returns ``True`` and should still be cleared.
    """
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "table1.parquet").write_text("p1")

    def _exists(path):
        # The permanent version path doesn't exist yet; the rolling "stable" path
        # already does and should get cleared.
        return "v2026.7.0" not in path

    with (
        patch("pudl.deploy.pudl.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.deploy.pudl.s3fs.S3FileSystem") as mock_s3_cls,
    ):
        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs.exists.side_effect = _exists
        mock_s3.exists.side_effect = _exists
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(
            source_dir,
            ["v2026.7.0", "stable"],
            immutable_suffixes=frozenset({"v2026.7.0"}),
        )

        # "stable" is a rolling path and should be cleared; "v2026.7.0" is the
        # permanent, hold-protected version path and must not be.
        assert mock_gcs.rm.call_args_list == [
            call("gs://pudl.catalyst.coop/stable/", recursive=True),
        ]
        assert mock_s3.rm.call_args_list == [
            call("s3://pudl.catalyst.coop/stable/", recursive=True),
        ]


def test_upload_outputs_raises_if_permanent_path_already_exists(tmp_path):
    """Deploying to a permanent version path that already has content is invalid.

    Regression test for the case this is meant to prevent: re-deploying the same
    stable version tag a second time, which would otherwise silently mix old and
    new files together instead of being rejected outright.
    """
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "table1.parquet").write_text("p1")

    with (
        patch("pudl.deploy.pudl.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.deploy.pudl.s3fs.S3FileSystem") as mock_s3_cls,
    ):
        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs.exists.return_value = True
        mock_s3.exists.return_value = True
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        with pytest.raises(RuntimeError, match="already has content"):
            upload_outputs(
                source_dir,
                ["v2026.7.0", "stable"],
                immutable_suffixes=frozenset({"v2026.7.0"}),
            )

        # Should fail fast, before ever attempting to upload anything.
        mock_gcs.put.assert_not_called()
        mock_s3.put.assert_not_called()


def test_clear_deployment_path_skips_nonexistent_path():
    """clear_deployment_path should not call rm() if the path doesn't exist."""
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False

    clear_deployment_path(mock_fs, "gs://pudl.catalyst.coop/nightly/")

    mock_fs.rm.assert_not_called()


def test_upload_outputs_empty_directory(tmp_path):
    """Test that uploading from empty directory raises error."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    with pytest.raises(ValueError, match="Source directory is empty"):
        upload_outputs(source_dir, ["nightly"])


def test_upload_outputs_nonexistent_directory(tmp_path):
    """Test that uploading from non-existent directory raises error."""
    source_dir = tmp_path / "nonexistent"
    with pytest.raises(ValueError, match="Source directory does not exist"):
        upload_outputs(source_dir, ["nightly"])


def test_update_git_branch():
    """Test git branch update merges tag and pushes."""
    nightly_tag = "nightly-2026-02-09"
    stable_tag = "v2026.2.9"
    with patch("pudl.deploy.pudl.subprocess.run") as mock_run:
        mock_run.retudeploymentvalue = MagicMock(returncode=0)
        update_git_branch(
            tag="nightly-2026-02-09",
            branch="nightly",
            environment="production",
            github_token="github_token",  # noqa: S106
        )

        with pytest.raises(
            RuntimeError,
            match=f"Git tag, {nightly_tag}, does not match deployment branch, stable.",
        ):
            update_git_branch(
                tag=nightly_tag,
                branch="stable",
                environment="production",
                github_token="github_token",  # noqa: S106
            )
        with pytest.raises(
            RuntimeError,
            match=f"Git tag, {stable_tag}, does not match deployment branch, nightly.",
        ):
            update_git_branch(
                tag=stable_tag,
                branch="nightly",
                environment="staging",
                github_token="github_token",  # noqa: S106
            )

        kwargs = {"check": True, "capture_output": True, "text": True}
        assert mock_run.call_count == 8
        mock_run.assert_has_calls(
            [
                call(["git", "config", "user.email", "pudl@catalyst.coop"], **kwargs),
                call(["git", "config", "user.name", "pudlbot"], **kwargs),
                call(
                    [
                        "git",
                        "remote",
                        "set-url",
                        "origin",
                        "https://pudlbot"  # Combine strings to avoid secret checkers
                        + ":github_token@github.com/catalyst-cooperative/pudl.git",
                    ],
                    **kwargs,
                ),
                call(
                    ["git", "fetch", "--force", "--tags", "origin", nightly_tag],
                    **kwargs,
                ),
                call(["git", "fetch", "origin", "nightly:nightly"], **kwargs),
                call(["git", "checkout", "nightly"], **kwargs),
                call(["git", "merge", "--ff-only", "nightly-2026-02-09"], **kwargs),
                call(["git", "push", "-u", "origin", "nightly"], **kwargs),
            ]
        )


def test_update_git_branch_staging():
    """Test git branch update skips push when staging."""
    with patch("pudl.deploy.pudl.subprocess.run") as mock_run:
        mock_run.retudeploymentvalue = MagicMock(returncode=0)

        update_git_branch(
            tag="nightly-2026-02-09",
            branch="nightly",
            environment="staging",
            github_token="github_token",  # noqa: S106
        )

        kwargs = {"check": True, "capture_output": True, "text": True}
        assert mock_run.call_count == 7
        mock_run.assert_has_calls(
            [
                call(["git", "config", "user.email", "pudl@catalyst.coop"], **kwargs),
                call(["git", "config", "user.name", "pudlbot"], **kwargs),
                call(
                    [
                        "git",
                        "remote",
                        "set-url",
                        "origin",
                        "https://pudlbot"  # Combine strings to avoid secret checkers
                        + ":github_token@github.com/catalyst-cooperative/pudl.git",
                    ],
                    **kwargs,
                ),
                call(
                    [
                        "git",
                        "fetch",
                        "--force",
                        "--tags",
                        "origin",
                        "nightly-2026-02-09",
                    ],
                    **kwargs,
                ),
                call(["git", "fetch", "origin", "nightly:nightly"], **kwargs),
                call(["git", "checkout", "nightly"], **kwargs),
                call(["git", "merge", "--ff-only", "nightly-2026-02-09"], **kwargs),
            ]
        )


@pytest.mark.parametrize(
    "inputs,expected_json",
    [
        (None, {"ref": "main"}),
        (
            {"env": "sandbox", "publish": "publish"},
            {"ref": "main", "inputs": {"env": "sandbox", "publish": "publish"}},
        ),
    ],
)
def test_dispatch_github_workflow_posts_expected_payload(inputs, expected_json):
    """dispatch_github_workflow should POST a bare ref, or one with inputs attached."""
    with patch("pudl.deploy.pudl.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=204)

        dispatch_github_workflow(
            repo="catalyst-cooperative/eel-hole",
            workflow_file="build-deploy.yml",
            ref="main",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            inputs=inputs,
        )

        mock_post.return_value.raise_for_status.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == (
            "https://api.github.com/repos/catalyst-cooperative/eel-hole"
            "/actions/workflows/build-deploy.yml/dispatches"
        )
        assert kwargs["headers"]["Authorization"] == "Bearer fake-token"
        assert kwargs["json"] == expected_json


def test_dispatch_github_workflow_raises_on_http_error():
    """A failed dispatch should propagate the HTTP error."""
    with patch("pudl.deploy.pudl.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=422)
        mock_post.return_value.raise_for_status.side_effect = requests.HTTPError(
            "422 Client Error"
        )

        with pytest.raises(requests.HTTPError):
            dispatch_github_workflow(
                repo="catalyst-cooperative/pudl",
                workflow_file="zenodo-data-release.yml",
                ref="main",
                token="fake-token",  # noqa: S106  # pragma: allowlist secret
            )


@pytest.mark.parametrize(
    "deploy_type,build_ref,source_suffix,expected_env,expected_publish",
    [
        (DeploymentType.NIGHTLY, "nightly-2026-07-05", "nightly", "sandbox", "publish"),
        (DeploymentType.STABLE, "v2026.7.0", "v2026.7.0", "production", "no-publish"),
    ],
)
def test_trigger_zenodo_release_dispatches_expected_inputs(
    deploy_type, build_ref, source_suffix, expected_env, expected_publish
):
    """Nightly releases publish to sandbox; stable releases draft to production."""
    with patch("pudl.deploy.pudl.dispatch_github_workflow") as mock_dispatch:
        trigger_zenodo_release(
            build_ref=build_ref,
            deploy_type=deploy_type,
            source_suffix=source_suffix,
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
        )

        mock_dispatch.assert_called_once_with(
            repo="catalyst-cooperative/pudl",
            workflow_file="zenodo-data-release.yml",
            ref=build_ref,
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            inputs={
                "env": expected_env,
                "source_dir": f"s3://pudl.catalyst.coop/{source_suffix}",
                "ignore_regex": r"^.*\.parquet$",
                "publish": expected_publish,
            },
        )


@pytest.mark.parametrize(
    "environment,expected_workflow_file",
    [
        ("staging", "build-deploy-staging.yml"),
        ("production", "build-deploy.yml"),
    ],
)
def test_update_pudl_viewer_selects_workflow_by_environment(
    environment, expected_workflow_file
):
    """Staging and production should dispatch different Eel Hole workflows."""
    with patch("pudl.deploy.pudl.dispatch_github_workflow") as mock_dispatch:
        update_pudl_viewer(
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            environment=environment,
        )

        mock_dispatch.assert_called_once_with(
            repo="catalyst-cooperative/eel-hole",
            workflow_file=expected_workflow_file,
            ref="main",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
        )


@pytest.mark.parametrize(
    "create_builds,build_successful",
    [
        (True, True),
        (True, False),
        (False, True),
    ],
)
def test_get_build_from_tag(
    tmp_path: Path, create_builds: bool, build_successful: bool
):
    """Test getting build path from git tag."""
    example_tag = "example_tag"
    expected_hash = hashlib.sha1(b"Fake data to hash").hexdigest()[0:9]  # noqa: S324
    other_hash = hashlib.sha1(b"More fake data to hash").hexdigest()[0:9]  # noqa: S324

    # Create build directories in tmp_path
    if create_builds:
        for build_name, most_recent_build in [
            (f"2026-02-04-1230-{expected_hash}-main", True),
            (f"2026-02-04-0530-{expected_hash}-main", False),
            (f"2026-01-01-0000-{expected_hash}-main", False),
            (f"2026-01-01-1200-{other_hash}-main", True),
            (f"2025-12-31-1200-{other_hash}-main", False),
        ]:
            build_path = tmp_path / build_name
            build_path.mkdir()
            if build_successful:
                (build_path / "success").touch()
            if most_recent_build and (expected_hash in build_name):
                expected_path = build_path

    # Setup mocks and run tests
    with (
        patch("pudl.deploy.pudl._run") as run_mock,
        patch("pudl.deploy.pudl.UPath") as build_path_mock,
    ):
        run_mock.return_value = expected_hash
        build_path_mock.return_value = tmp_path

        if not create_builds:
            with pytest.raises(
                RuntimeError, match=r"Can't find a build associated with tag:.+"
            ):
                get_build_from_tag(example_tag)
        elif not build_successful:
            with pytest.raises(
                RuntimeError, match="Can't find 'success' file in build directory!"
            ):
                get_build_from_tag(example_tag)
        else:
            assert get_build_from_tag(example_tag) == expected_path


@pytest.mark.parametrize(
    "git_tag,expected_type",
    [
        ("v2026.7.0", DeploymentType.STABLE),
        ("v2026.7.10", DeploymentType.STABLE),
        ("nightly-2026-07-05", DeploymentType.NIGHTLY),
        ("branch-2026-07-05-0600-abc123456-my-branch", DeploymentType.BRANCH),
        (
            "branch-2026-07-05-0600-abc123456-my-branch-with-dashes",
            DeploymentType.BRANCH,
        ),
    ],
)
def test_get_deployment_type_from_tag(git_tag, expected_type):
    """Each supported tag shape should map to the right deployment type."""
    assert get_deployment_type_from_tag(git_tag) == expected_type


@pytest.mark.parametrize(
    "git_tag",
    ["not-a-real-tag", "2026-07-05", "v2026", "nightly-2026-07"],
)
def test_get_deployment_type_from_tag_rejects_unrecognized_tags(git_tag):
    """Tags that don't match any known shape should raise."""
    with pytest.raises(RuntimeError, match="does not look like"):
        get_deployment_type_from_tag(git_tag)


@pytest.mark.parametrize(
    "git_tag,environment,expected_deploy_type,expected_suffixes,"
    "expected_zenodo_suffix,expected_immutable_suffixes,expect_eel_hole,"
    "expect_git,expect_zenodo,expect_hold",
    [
        (
            "nightly-2026-07-05",
            "production",
            DeploymentType.NIGHTLY,
            ["nightly", "eel-hole"],
            "nightly",
            frozenset(),
            True,
            True,
            True,
            False,
        ),
        (
            "nightly-2026-07-05",
            "staging",
            DeploymentType.NIGHTLY,
            ["staging/nightly", "staging/eel-hole"],
            "staging/nightly",
            frozenset(),
            True,
            True,
            True,
            False,
        ),
        (
            "v2026.7.0",
            "production",
            DeploymentType.STABLE,
            ["v2026.7.0", "stable"],
            "v2026.7.0",
            frozenset({"v2026.7.0"}),
            False,
            True,
            True,
            True,
        ),
        (
            "v2026.7.0",
            "staging",
            DeploymentType.STABLE,
            ["staging/v2026.7.0", "staging/stable"],
            "staging/v2026.7.0",
            frozenset(),
            False,
            True,
            True,
            False,
        ),
        (
            "branch-2026-07-05-0600-abc123456-my-branch",
            "staging",
            DeploymentType.BRANCH,
            ["staging/nightly", "staging/eel-hole"],
            "staging/nightly",
            frozenset(),
            False,
            False,
            False,
            False,
        ),
    ],
)
def test_deployment_plan(
    git_tag,
    environment,
    expected_deploy_type,
    expected_suffixes,
    expected_zenodo_suffix,
    expected_immutable_suffixes,
    expect_eel_hole,
    expect_git,
    expect_zenodo,
    expect_hold,
):
    """The deployment plan should be the single source of truth for every
    git_tag/environment decision -- the deploy type it implies, path suffixes, the
    immutable (never cleared) suffix, and which stages run.
    """
    plan = DeploymentPlan(git_tag=git_tag, environment=environment)

    assert plan.deploy_type == expected_deploy_type
    assert plan.path_suffixes == expected_suffixes
    assert plan.zenodo_source_suffix == expected_zenodo_suffix
    assert plan.immutable_suffixes == expected_immutable_suffixes
    assert plan.redeploy_eel_hole == expect_eel_hole
    assert plan.update_git_branch == expect_git
    assert plan.trigger_zenodo_release == expect_zenodo
    assert plan.gcs_temporary_hold == expect_hold


def test_deployment_plan_rejects_branch_deploy_to_production():
    """A branch deployment must never be allowed to target production."""
    with pytest.raises(ValidationError, match="Branch deployments can only target"):
        DeploymentPlan(
            git_tag="branch-2026-07-05-0600-abc123456-my-branch",
            environment="production",
        )


def test_deployment_plan_rejects_unrecognized_tag():
    """A git_tag that doesn't match any known deploy-type pattern should raise.

    ``deploy_type`` is derived from ``git_tag`` (see ``get_deployment_type_from_tag``)
    and accessed during validation, so an unparseable tag fails fast at construction
    time rather than later when some property happens to be accessed.
    """
    with pytest.raises(RuntimeError, match="does not look like"):
        DeploymentPlan(git_tag="not-a-real-tag", environment="staging")


def test_run_stage_records_success():
    """A successful stage is recorded with StageStatus.SUCCESS; others stay skipped."""
    stage_results = new_deploy_stage_results()

    run_stage(
        stage_fn=lambda: None,
        stage_name=DeployStage.UPLOAD_OUTPUTS,
        stage_results=stage_results,
    )

    assert stage_results[DeployStage.UPLOAD_OUTPUTS].status == StageStatus.SUCCESS
    assert stage_results[DeployStage.UPLOAD_OUTPUTS].duration_seconds >= 0
    assert stage_results[DeployStage.TRIGGER_ZENODO_RELEASE].status == (
        StageStatus.SKIPPED
    )
    assert stage_results[DeployStage.TRIGGER_ZENODO_RELEASE].duration_seconds == 0.0


def test_run_stage_records_failure_and_reraises():
    """A failing stage should record StageStatus.FAILURE and re-raise the exception."""
    stage_results = new_deploy_stage_results()

    def _boom():
        raise ValueError("kaboom")

    with pytest.raises(ValueError, match="kaboom"):
        run_stage(
            stage_fn=_boom,
            stage_name=DeployStage.UPLOAD_OUTPUTS,
            stage_results=stage_results,
        )

    assert stage_results[DeployStage.UPLOAD_OUTPUTS].status == StageStatus.FAILURE


def test_run_stage_does_not_raise_on_failure_when_not_fail_hard():
    """A fail_hard=False stage failure should be recorded but not propagate."""
    stage_results = new_deploy_stage_results()

    def _boom():
        raise ValueError("kaboom")

    # This deliberately triggers an expected logger.exception() call; patch the
    # logger so the traceback doesn't clutter test output.
    with patch("pudl.deploy.pudl.logger"):
        run_stage(
            stage_fn=_boom,
            stage_name=DeployStage.TRIGGER_ZENODO_RELEASE,
            stage_results=stage_results,
            fail_hard=False,
        )

    assert stage_results[DeployStage.TRIGGER_ZENODO_RELEASE].status == (
        StageStatus.FAILURE
    )


def test_build_deploy_zulip_message_reports_failure():
    """A failed stage should flip the message header to a failure state."""
    stage_results = new_deploy_stage_results()
    stage_results[DeployStage.UPLOAD_OUTPUTS] = StageResult(
        status=StageStatus.FAILURE, duration_seconds=5
    )

    message = build_deploy_zulip_message(
        build_id="2026-07-05-0600-abc123456-main",
        git_tag="nightly-2026-07-05",
        stage_results=stage_results,
        total_duration_seconds=5,
        deploy_logfile_name="2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log",
    )

    assert ":x: PUDL Deployment Failed" in message


def test_send_zulip_message_swallows_request_errors():
    """A Zulip API failure should be logged, not raised."""
    with (
        patch("pudl.deploy.pudl.requests.post") as mock_post,
        # This deliberately triggers an expected logger.warning() call; patch the
        # logger so the traceback doesn't clutter test output.
        patch("pudl.deploy.pudl.logger"),
    ):
        mock_post.side_effect = requests.RequestException("network error")

        # should not raise
        send_zulip_message("hello", api_key="fake-key")  # pragma: allowlist secret
