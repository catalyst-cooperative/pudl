"""Test distribution logic for ETL outputs."""

import hashlib
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from upath import UPath

from pudl.deploy.pudl import (
    get_build_from_tag,
    prepare_outputs_for_distribution,
    update_git_branch,
    upload_outputs,
)


def test_prepare_outputs_for_distribution(tmp_path):
    """Test complete output preparation workflow."""
    output_dir = tmp_path / "output"
    build_path = tmp_path / "build_path"
    output_dir.mkdir()
    build_path.mkdir()
    parquet_dir = build_path / "parquet"
    parquet_dir.mkdir()

    (build_path / "pudl.sqlite").write_text("db content")
    (build_path / "ferc1.sqlite").write_text("ferc db")
    (build_path / "pudl_dbt_tests.duckdb").write_text("test db")
    (parquet_dir / "table1.parquet").write_text("p1")
    (parquet_dir / "table2.parquet").write_text("p2")
    (parquet_dir / "pudl_parquet_datapackage.json").write_text("{}")

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
    with zipfile.ZipFile(output_dir / "pudl_parquet.zip") as zf:
        names = zf.namelist()
        assert "table1.parquet" in names
        assert "table2.parquet" in names
        assert "pudl_parquet_datapackage.json" in names

    assert not (output_dir / "pudl_dbt_tests.duckdb").exists()
    assert not (output_dir / "parquet").exists()


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
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, path_suffixes)

        assert mock_gcs.put.call_count == 2
        assert (
            mock_gcs.put.call_args_list[0][0][1] == "gs://pudl.catalyst.coop/nightly/"
        )
        assert (
            mock_gcs.put.call_args_list[1][0][1] == "gs://pudl.catalyst.coop/eel-hole/"
        )

        assert mock_s3.put.call_count == 2
        assert mock_s3.put.call_args_list[0][0][1] == "s3://pudl.catalyst.coop/nightly/"
        assert (
            mock_s3.put.call_args_list[1][0][1] == "s3://pudl.catalyst.coop/eel-hole/"
        )


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
        update_git_branch(tag="nightly-2026-02-09", branch="nightly", staging=False)

        with pytest.raises(
            RuntimeError,
            match=f"Git tag, {nightly_tag}, does not match deployment branch, stable.",
        ):
            update_git_branch(tag=nightly_tag, branch="stable", staging=False)
        with pytest.raises(
            RuntimeError,
            match=f"Git tag, {stable_tag}, does not match deployment branch, nightly.",
        ):
            update_git_branch(tag=stable_tag, branch="nightly", staging=False)

        kwargs = {"check": True, "capture_output": True, "text": True}
        assert mock_run.call_count == 3
        mock_run.assert_has_calls(
            [
                call(["git", "checkout", "nightly"], **kwargs),
                call(["git", "merge", "--ff-only", "nightly-2026-02-09"], **kwargs),
                call(["git", "push", "-u", "origin", "nightly"], **kwargs),
            ]
        )


def test_update_git_branch_staging():
    """Test git branch update skips push when staging."""
    with patch("pudl.deploy.pudl.subprocess.run") as mock_run:
        mock_run.retudeploymentvalue = MagicMock(returncode=0)

        update_git_branch(tag="nightly-2026-02-09", branch="nightly", staging=True)

        kwargs = {"check": True, "capture_output": True, "text": True}
        assert mock_run.call_count == 2
        mock_run.assert_has_calls(
            [
                call(["git", "checkout", "nightly"], **kwargs),
                call(["git", "merge", "--ff-only", "nightly-2026-02-09"], **kwargs),
            ]
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
