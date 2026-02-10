"""Test distribution logic for ETL outputs."""

import zipfile
from unittest.mock import MagicMock, patch

import pytest

from pudl.etl.distribute_outputs import (
    prepare_outputs_for_distribution,
    set_gcs_temporary_hold,
    trigger_zenodo_release,
    update_cloud_run_service,
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

    # Test DB removed
    assert not (output_dir / "pudl_dbt_tests.duckdb").exists()

    # Parquet directory removed
    assert not parquet_dir.exists()


def test_upload_outputs_nightly(tmp_path):
    """Test upload to nightly paths (production and staging)."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "pudl.sqlite.zip").write_text("db")
    (source_dir / "table1.parquet").write_text("p1")

    path_suffixes = ["nightly", "eel-hole"]

    with (
        patch("pudl.etl.distribute_outputs.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.etl.distribute_outputs.s3fs.S3FileSystem") as mock_s3_cls,
    ):
        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, path_suffixes, staging=False)

        # Verify GCS put called for each path suffix
        assert mock_gcs.put.call_count == 2
        # Verify S3 put called for each path suffix
        assert mock_s3.put.call_count == 2

        # Verify correct paths were used
        assert (
            mock_gcs.put.call_args_list[0][0][1] == "gs://pudl.catalyst.coop/nightly/"
        )
        assert (
            mock_gcs.put.call_args_list[1][0][1] == "gs://pudl.catalyst.coop/eel-hole/"
        )
        assert mock_s3.put.call_args_list[0][0][1] == "s3://pudl.catalyst.coop/nightly/"
        assert (
            mock_s3.put.call_args_list[1][0][1] == "s3://pudl.catalyst.coop/eel-hole/"
        )


def test_upload_outputs_staging(tmp_path):
    """Test upload to staging paths."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    (source_dir / "pudl.sqlite.zip").write_text("db")

    path_suffixes = ["nightly"]

    with (
        patch("pudl.etl.distribute_outputs.gcsfs.GCSFileSystem") as mock_gcs_cls,
        patch("pudl.etl.distribute_outputs.s3fs.S3FileSystem") as mock_s3_cls,
    ):
        mock_gcs = MagicMock()
        mock_s3 = MagicMock()
        mock_gcs_cls.return_value = mock_gcs
        mock_s3_cls.return_value = mock_s3

        upload_outputs(source_dir, path_suffixes, staging=True)

        # Check that staging/ prefix is used in the path
        gcs_call_args = mock_gcs.put.call_args_list[0][0]
        assert "staging/nightly/" in gcs_call_args[1]


def test_upload_outputs_empty_directory(tmp_path):
    """Test that uploading from empty directory raises error."""
    source_dir = tmp_path / "output"
    source_dir.mkdir()
    # Don't create any files - directory is empty

    with pytest.raises(ValueError, match="Source directory is empty"):
        upload_outputs(source_dir, ["nightly"], staging=False)


def test_upload_outputs_nonexistent_directory(tmp_path):
    """Test that uploading from non-existent directory raises error."""
    source_dir = tmp_path / "nonexistent"
    # Don't create the directory

    with pytest.raises(ValueError, match="Source directory does not exist"):
        upload_outputs(source_dir, ["nightly"], staging=False)


def test_update_git_branch():
    """Test git branch update merges tag and pushes."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        update_git_branch(tag="nightly-2025-02-05", branch="nightly")

        # Verify exactly 3 git commands called
        calls = mock_run.call_args_list
        assert len(calls) == 3

        # Verify git checkout command
        assert calls[0][0][0] == ["git", "checkout", "nightly"]
        assert calls[0][1]["check"] is True
        assert calls[0][1]["capture_output"] is True
        assert calls[0][1]["text"] is True

        # Verify git merge command with --ff-only
        assert calls[1][0][0] == ["git", "merge", "--ff-only", "nightly-2025-02-05"]
        assert calls[1][1]["check"] is True
        assert calls[1][1]["capture_output"] is True
        assert calls[1][1]["text"] is True

        # Verify git push command
        assert calls[2][0][0] == ["git", "push", "-u", "origin", "nightly"]
        assert calls[2][1]["check"] is True
        assert calls[2][1]["capture_output"] is True
        assert calls[2][1]["text"] is True


def test_trigger_zenodo_release():
    """Test Zenodo release workflow trigger."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        trigger_zenodo_release(
            env="sandbox",
            source_dir="s3://pudl.catalyst.coop/nightly/",
            ignore_regex=r"(^.*\.parquet$|^pudl_parquet_datapackage\.json$)",
            publish=True,
        )

        # Verify gh workflow dispatch called
        calls = mock_run.call_args_list
        assert len(calls) == 1
        cmd = " ".join(calls[0][0][0])
        assert "gh workflow run" in cmd
        assert "zenodo-data-release.yml" in cmd
        assert "sandbox" in cmd
        assert "publish" in cmd


def test_trigger_zenodo_release_no_publish():
    """Test Zenodo release with publish=False."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        trigger_zenodo_release(
            env="production",
            source_dir="s3://pudl.catalyst.coop/stable/",
            ignore_regex=r"(^.*\.parquet$)",
            publish=False,
        )

        # Verify no-publish flag used
        cmd = " ".join(mock_run.call_args_list[0][0][0])
        assert "no-publish" in cmd
        assert "production" in cmd


def test_trigger_zenodo_release_invalid_env():
    """Test Zenodo release with invalid environment."""
    with pytest.raises(ValueError, match="Invalid Zenodo environment"):
        trigger_zenodo_release(
            env="invalid",
            source_dir="s3://pudl.catalyst.coop/nightly/",
            ignore_regex=r".*",
            publish=True,
        )


def test_update_cloud_run_service():
    """Test Cloud Run service update."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        update_cloud_run_service("pudl-viewer")

        # Verify gcloud run services update called
        calls = mock_run.call_args_list
        assert len(calls) == 1
        cmd = " ".join(calls[0][0][0])
        assert "gcloud run services update" in cmd
        assert "pudl-viewer" in cmd


def test_set_gcs_temporary_hold():
    """Test GCS temporary hold for versioned releases."""
    with patch("pudl.etl.distribute_outputs.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)

        set_gcs_temporary_hold("gs://pudl.catalyst.coop/v2025.2.3/")

        # Verify gcloud storage objects update called
        calls = mock_run.call_args_list
        assert len(calls) == 1
        cmd = " ".join(calls[0][0][0])
        assert "gcloud storage" in cmd
        assert "temporary-hold" in cmd
        assert "v2025.2.3" in cmd
