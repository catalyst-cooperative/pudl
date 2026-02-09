"""Test distribution logic for ETL outputs."""

import zipfile

from pudl.etl.distribute_outputs import prepare_outputs_for_distribution


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
