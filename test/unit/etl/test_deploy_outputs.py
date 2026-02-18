"""Test distribution logic for ETL outputs."""

import zipfile
from unittest.mock import MagicMock, call, patch

import pytest

from pudl.etl.deploy_outputs import (
    prepare_outputs_for_distribution,
    update_git_branch,
    upload_outputs,
)


def test_prepare_outputs_for_distribution(tmp_path):
    """Test complete output preparation workflow."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    parquet_dir = output_dir / "parquet"
    parquet_dir.mkdir()

    (output_dir / "pudl.sqlite").write_text("db content")
    (output_dir / "ferc1.sqlite").write_text("ferc db")
    (output_dir / "pudl_dbt_tests.duckdb").write_text("test db")
    (parquet_dir / "table1.parquet").write_text("p1")
    (parquet_dir / "table2.parquet").write_text("p2")
    (parquet_dir / "pudl_parquet_datapackage.json").write_text("{}")

    prepare_outputs_for_distribution(output_dir)

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
    assert not parquet_dir.exists()


def test_upload_outputs_nightly(tmp_path):
    """Test upload to nightly paths (production)."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "pudl.sqlite.zip").write_text("db")
    (source_dir / "table1.parquet").write_text("p1")

    path_suffixes = ["nightly", "eel-hole"]

    with (
        patch("pudl.etl.deploy_outputs.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.etl.deploy_outputs.s3fs.S3FileSystem") as mock_s3_cls,
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
    with patch("pudl.etl.deploy_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        update_git_branch(tag="nightly-2026-02-09", branch="nightly", staging=False)

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
    with patch("pudl.etl.deploy_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        update_git_branch(tag="nightly-2026-02-09", branch="nightly", staging=True)

        kwargs = {"check": True, "capture_output": True, "text": True}
        assert mock_run.call_count == 2
        mock_run.assert_has_calls(
            [
                call(["git", "checkout", "nightly"], **kwargs),
                call(["git", "merge", "--ff-only", "nightly-2026-02-09"], **kwargs),
            ]
        )
