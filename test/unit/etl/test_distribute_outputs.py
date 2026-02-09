"""Test distribution logic for ETL outputs."""

import zipfile
from unittest.mock import MagicMock, patch

import pytest

from pudl.etl.distribute_outputs import prepare_outputs_for_distribution, upload_outputs


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
