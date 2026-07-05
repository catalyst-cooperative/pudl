"""Test distribution logic for ETL outputs."""

import hashlib
import threading
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from upath import UPath

from pudl.deploy.pudl import (
    DeploymentType,
    StageResult,
    build_deploy_logfile_links,
    build_deploy_zulip_message,
    build_deployment_plan,
    clear_deployment_path,
    dispatch_github_workflow,
    format_stage_duration,
    get_build_from_tag,
    get_deployment_type_from_tag,
    new_deploy_stage_results,
    prepare_outputs_for_distribution,
    run_best_effort_stage,
    run_stage,
    send_zulip_message,
    stage_emoji,
    trigger_zenodo_release,
    update_git_branch,
    update_pudl_viewer,
    upload_outputs,
)


def test_prepare_outputs_for_distribution(tmp_path):
    """Test complete output preparation workflow."""
    output_dir = tmp_path / "output"
    build_id = "2026-07-04-0600-abc123456-main"
    build_path = tmp_path / build_id
    output_dir.mkdir()
    build_path.mkdir()
    parquet_dirs = [
        build_path / "parquet",
        build_path / "ferc1_dbf",
        build_path / "ferc2_dbf",
        build_path / "ferc6_dbf",
        build_path / "ferc60_dbf",
        build_path / "ferc1_xbrl",
        build_path / "ferc2_xbrl",
        build_path / "ferc6_xbrl",
        build_path / "ferc60_xbrl",
        build_path / "ferc714_xbrl",
    ]

    (build_path / "pudl.sqlite").write_text("db content")
    (build_path / "ferc1.sqlite").write_text("ferc db")
    (build_path / "pudl_dbt_tests.duckdb").write_text("test db")

    for parquet_dir in parquet_dirs:
        parquet_dir.mkdir()
        (parquet_dir / "table1.parquet").write_text("p1")
        (parquet_dir / "table2.parquet").write_text("p2")
        (parquet_dir / "datapackage.json").write_text("{}")

    prepare_outputs_for_distribution(output_dir, UPath(build_path))

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

    # An empty marker file named after the build ID gives distributed outputs
    # provenance, now that the build log's filename is no longer distributed.
    marker = output_dir / build_id
    assert marker.exists()
    assert marker.stat().st_size == 0


def test_prepare_outputs_for_distribution_excludes_internal_files(tmp_path):
    """Build/deploy logs and the "success" sentinel must never be distributed.

    Build and deploy logs live alongside the real outputs under
    builds.catalyst.coop (so a ``fs.get(..., recursive=True)`` pulls them down
    too), but they can contain stack traces or other details we don't want to
    expose publicly. The "success" sentinel is internal build-completion
    plumbing with no meaning for consumers of the distributed outputs.
    """
    output_dir = tmp_path / "output"
    build_path = tmp_path / "build_path"
    output_dir.mkdir()
    build_path.mkdir()

    (build_path / "pudl.sqlite").write_text("db content")
    (build_path / "pudl_dbt_tests.duckdb").write_text("test db")
    (build_path / "2026-07-04-0600-abc123456-main.log").write_text("build log")
    (
        build_path / "2026-07-04-0600-abc123456-main-deploy-2026-07-04-0700.log"
    ).write_text("deploy log")
    (build_path / "success").write_text("")

    for name in ["parquet", "ferc1_dbf", "ferc2_dbf", "ferc6_dbf", "ferc60_dbf"]:
        parquet_dir = build_path / name
        parquet_dir.mkdir()
        (parquet_dir / "table1.parquet").write_text("p1")
        (parquet_dir / "datapackage.json").write_text("{}")
    for xbrl_name in [
        "ferc1_xbrl",
        "ferc2_xbrl",
        "ferc6_xbrl",
        "ferc60_xbrl",
        "ferc714_xbrl",
    ]:
        xbrl_dir = build_path / xbrl_name
        xbrl_dir.mkdir()
        (xbrl_dir / "table1.parquet").write_text("p1")
        (xbrl_dir / "datapackage.json").write_text("{}")

    prepare_outputs_for_distribution(output_dir, UPath(build_path))

    assert list(output_dir.glob("*.log")) == []
    assert not (output_dir / "success").exists()
    # Everything else should still have made it through.
    assert (output_dir / "pudl.sqlite.zip").exists()


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

    Uses a barrier that all 4 ``put()`` calls must reach together. If the uploads
    actually ran one at a time, only one call would ever be in flight and the
    barrier would time out instead of releasing.
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
    """A suffix listed as immutable should never be cleared before upload."""
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


def test_dispatch_github_workflow_without_inputs():
    """dispatch_github_workflow should POST a bare ref when no inputs are given."""
    with patch("pudl.deploy.pudl.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=204)

        dispatch_github_workflow(
            repo="catalyst-cooperative/eel-hole",
            workflow_file="build-deploy.yml",
            ref="main",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
        )

        mock_post.return_value.raise_for_status.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == (
            "https://api.github.com/repos/catalyst-cooperative/eel-hole"
            "/actions/workflows/build-deploy.yml/dispatches"
        )
        assert kwargs["headers"]["Authorization"] == "Bearer fake-token"
        assert kwargs["json"] == {"ref": "main"}


def test_dispatch_github_workflow_with_inputs():
    """dispatch_github_workflow should include workflow_dispatch inputs when given."""
    with patch("pudl.deploy.pudl.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=204)

        dispatch_github_workflow(
            repo="catalyst-cooperative/pudl",
            workflow_file="zenodo-data-release.yml",
            ref="nightly-2026-07-05",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            inputs={"env": "sandbox", "publish": "publish"},
        )

        _args, kwargs = mock_post.call_args
        assert kwargs["json"] == {
            "ref": "nightly-2026-07-05",
            "inputs": {"env": "sandbox", "publish": "publish"},
        }


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


def test_trigger_zenodo_release_nightly_dispatches_sandbox_publish():
    """Nightly/branch releases should publish a sandbox Zenodo deposition."""
    with patch("pudl.deploy.pudl.dispatch_github_workflow") as mock_dispatch:
        trigger_zenodo_release(
            build_ref="nightly-2026-07-05",
            deploy_type=DeploymentType.NIGHTLY,
            source_suffix="nightly",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
        )

        mock_dispatch.assert_called_once_with(
            repo="catalyst-cooperative/pudl",
            workflow_file="zenodo-data-release.yml",
            ref="nightly-2026-07-05",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            inputs={
                "env": "sandbox",
                "source_dir": "s3://pudl.catalyst.coop/nightly",
                "ignore_regex": r"^.*\.parquet$",
                "publish": "publish",
            },
        )


def test_trigger_zenodo_release_stable_dispatches_production_no_publish():
    """Stable releases should leave a production Zenodo deposition as a draft."""
    with patch("pudl.deploy.pudl.dispatch_github_workflow") as mock_dispatch:
        trigger_zenodo_release(
            build_ref="v2026.7.0",
            deploy_type=DeploymentType.STABLE,
            source_suffix="v2026.7.0",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
        )

        mock_dispatch.assert_called_once_with(
            repo="catalyst-cooperative/pudl",
            workflow_file="zenodo-data-release.yml",
            ref="v2026.7.0",
            token="fake-token",  # noqa: S106  # pragma: allowlist secret
            inputs={
                "env": "production",
                "source_dir": "s3://pudl.catalyst.coop/v2026.7.0",
                "ignore_regex": r"^.*\.parquet$",
                "publish": "no-publish",
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
        ("branch-my-branch-2026-07-05", DeploymentType.BRANCH),
        ("branch-my-branch-with-dashes-2026-07-05", DeploymentType.BRANCH),
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
    "deploy_type,git_tag,environment,expected_suffixes,expected_zenodo_suffix,"
    "expected_immutable_suffixes,expect_eel_hole,expect_git,expect_zenodo,expect_hold",
    [
        (
            DeploymentType.NIGHTLY,
            "nightly-2026-07-05",
            "production",
            ["nightly", "eel-hole"],
            "nightly",
            frozenset(),
            True,
            True,
            True,
            False,
        ),
        (
            DeploymentType.NIGHTLY,
            "nightly-2026-07-05",
            "staging",
            ["staging/nightly", "staging/eel-hole"],
            "staging/nightly",
            frozenset(),
            True,
            True,
            True,
            False,
        ),
        (
            DeploymentType.STABLE,
            "v2026.7.0",
            "production",
            ["v2026.7.0", "stable"],
            "v2026.7.0",
            frozenset({"v2026.7.0"}),
            False,
            True,
            True,
            True,
        ),
        (
            DeploymentType.STABLE,
            "v2026.7.0",
            "staging",
            ["staging/v2026.7.0", "staging/stable"],
            "staging/v2026.7.0",
            frozenset(),
            False,
            True,
            True,
            False,
        ),
        (
            DeploymentType.BRANCH,
            "branch-my-branch-2026-07-05",
            "staging",
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
def test_build_deployment_plan(
    deploy_type,
    git_tag,
    environment,
    expected_suffixes,
    expected_zenodo_suffix,
    expected_immutable_suffixes,
    expect_eel_hole,
    expect_git,
    expect_zenodo,
    expect_hold,
):
    """The deployment plan should be the single source of truth for every
    deploy_type/environment decision -- path suffixes, the immutable (never
    cleared) suffix, and which stages run.
    """
    plan = build_deployment_plan(
        deploy_type=deploy_type, git_tag=git_tag, environment=environment
    )

    assert plan.path_suffixes == expected_suffixes
    assert plan.zenodo_source_suffix == expected_zenodo_suffix
    assert plan.immutable_suffixes == expected_immutable_suffixes
    assert plan.redeploy_eel_hole == expect_eel_hole
    assert plan.update_git_branch == expect_git
    assert plan.trigger_zenodo_release == expect_zenodo
    assert plan.gcs_temporary_hold == expect_hold


@pytest.mark.parametrize(
    "elapsed_seconds,expected",
    [
        (0, "00:00:00"),
        (59, "00:00:59"),
        (60, "00:01:00"),
        (3661, "01:01:01"),
        (7325, "02:02:05"),
    ],
)
def test_format_stage_duration(elapsed_seconds, expected):
    """Durations should be formatted as zero-padded HH:MM:SS."""
    assert format_stage_duration(elapsed_seconds) == expected


@pytest.mark.parametrize(
    "status,expected_emoji",
    [
        ("success", ":check:"),
        ("failure", ":x:"),
        ("skipped", ":ghost:"),
    ],
)
def test_stage_emoji(status, expected_emoji):
    """Each stage status should map to its corresponding Zulip emoji."""
    assert stage_emoji(status) == expected_emoji


def test_run_stage_records_success():
    """A successful stage should be recorded with 'success' and its duration."""
    stage_results = new_deploy_stage_results()

    run_stage(
        stage_fn=lambda: None,
        stage_name="Upload outputs",
        stage_results=stage_results,
    )

    assert stage_results["Upload outputs"].status == "success"
    assert stage_results["Upload outputs"].duration_seconds >= 0


def test_run_stage_records_failure_and_reraises():
    """A failing stage should record 'failure' and re-raise the exception."""
    stage_results = new_deploy_stage_results()

    def _boom():
        raise ValueError("kaboom")

    with pytest.raises(ValueError, match="kaboom"):
        run_stage(
            stage_fn=_boom, stage_name="Upload outputs", stage_results=stage_results
        )

    assert stage_results["Upload outputs"].status == "failure"


def test_run_stage_leaves_other_stages_skipped():
    """Stages never run should keep their default skipped status."""
    stage_results = new_deploy_stage_results()

    run_stage(
        stage_fn=lambda: None,
        stage_name="Upload outputs",
        stage_results=stage_results,
    )

    assert stage_results["Trigger Zenodo Release"].status == "skipped"
    assert stage_results["Trigger Zenodo Release"].duration_seconds == 0.0


def test_run_best_effort_stage_does_not_raise_on_failure():
    """A best-effort stage failure should be recorded but not propagate."""
    stage_results = new_deploy_stage_results()

    def _boom():
        raise ValueError("kaboom")

    # This deliberately triggers an expected logger.exception() call; patch the
    # logger so the traceback doesn't clutter test output.
    with patch("pudl.deploy.pudl.logger"):
        run_best_effort_stage(
            stage_fn=_boom,
            stage_name="Trigger Zenodo Release",
            stage_results=stage_results,
        )

    assert stage_results["Trigger Zenodo Release"].status == "failure"


def test_build_deploy_zulip_message_reports_success_and_all_stage_rows():
    """The Zulip message should include every tracked stage, in order."""
    stage_results = new_deploy_stage_results()
    stage_results["Prepare outputs"] = StageResult(
        status="success", duration_seconds=10
    )
    stage_results["Upload outputs"] = StageResult(status="success", duration_seconds=20)
    # "Redeploy Eel Hole", "Update Git Branch", "Trigger Zenodo Release", and
    # "GCS Temporary Hold" are left at their default skipped status.

    message = build_deploy_zulip_message(
        build_id="2026-07-05-0600-abc123456-main",
        git_tag="nightly-2026-07-05",
        stage_results=stage_results,
        total_duration_seconds=30,
        deploy_logfile_name="2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log",
    )

    assert ":check: PUDL Deployment Succeeded" in message
    assert "2026-07-05-0600-abc123456-main" in message
    assert "nightly-2026-07-05" in message
    assert "| Prepare outputs | :check: |" in message
    assert "| Upload outputs | :check: |" in message
    assert "| Redeploy Eel Hole | :ghost: |" in message
    assert "| Update Git Branch | :ghost: |" in message
    assert "| Trigger Zenodo Release | :ghost: |" in message
    assert "| GCS Temporary Hold | :ghost: |" in message
    assert "## Review PUDL Deploy Logs" in message
    assert "2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log" in message


def test_build_deploy_zulip_message_reports_failure():
    """A failed stage should flip the message header to a failure state."""
    stage_results = new_deploy_stage_results()
    stage_results["Upload outputs"] = StageResult(status="failure", duration_seconds=5)

    message = build_deploy_zulip_message(
        build_id="2026-07-05-0600-abc123456-main",
        git_tag="nightly-2026-07-05",
        stage_results=stage_results,
        total_duration_seconds=5,
        deploy_logfile_name="2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log",
    )

    assert ":x: PUDL Deployment Failed" in message


def test_build_deploy_logfile_links_includes_batch_job_console_link_when_known():
    """The console job link should appear when a batch job name is provided."""
    message = build_deploy_logfile_links(
        build_id="2026-07-05-0600-abc123456-main",
        deploy_logfile_name="2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log",
        batch_job_name="deploy-outputs-12345-1",
    )

    assert "## Review PUDL Deploy Logs" in message
    assert (
        "gs://builds.catalyst.coop/2026-07-05-0600-abc123456-main/"
        "2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log" in message
    )
    assert (
        "https://storage.cloud.google.com/builds.catalyst.coop/"
        "2026-07-05-0600-abc123456-main/"
        "2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log" in message
    )
    assert (
        "https://console.cloud.google.com/batch/jobsDetail/regions/us-east1/"
        "jobs/deploy-outputs-12345-1/logs?project=catalyst-cooperative-pudl" in message
    )
    assert (
        "https://console.cloud.google.com/storage/browser/builds.catalyst.coop/"
        "2026-07-05-0600-abc123456-main" in message
    )


def test_build_deploy_logfile_links_omits_console_link_when_job_name_unknown():
    """No batch job name should mean no (broken) console job link."""
    message = build_deploy_logfile_links(
        build_id="2026-07-05-0600-abc123456-main",
        deploy_logfile_name="2026-07-05-0600-abc123456-main-deploy-2026-07-05-0700.log",
        batch_job_name=None,
    )

    assert "jobsDetail" not in message
    assert "## Review PUDL Deploy Logs" in message
    assert "gs://builds.catalyst.coop" in message


def test_send_zulip_message_posts_expected_payload():
    """send_zulip_message should POST the message to the Zulip stream API."""
    with patch("pudl.deploy.pudl.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)

        send_zulip_message("hello", api_key="fake-key")  # pragma: allowlist secret

        assert mock_post.call_count == 1
        _args, kwargs = mock_post.call_args
        assert kwargs["auth"] == (
            "build-status-bot@catalyst-cooperative.zulipchat.com",
            "fake-key",  # pragma: allowlist secret
        )
        assert kwargs["data"]["content"] == "hello"
        assert kwargs["data"]["to"] == "pudl-deployments"


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
