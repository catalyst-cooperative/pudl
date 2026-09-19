"""Tests for converting FERC DBF databases into Parquet."""

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

import pudl
from pudl.extract.dbf import convert_db_into_parquet


def test_convert_db_into_parquet_writes_zstd(tmp_path: Path) -> None:
    """The Parquet files converted from a DuckDB database use the codec set centrally in ``pudl``."""
    db_path = tmp_path / "ferc.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE f1_test AS SELECT range AS a FROM range(10)")

    convert_db_into_parquet(db_path, tmp_path / "parquet")

    row_group = pq.read_metadata(tmp_path / "parquet" / "f1_test.parquet").row_group(0)
    assert row_group.column(0).compression == pudl.PARQUET_COMPRESSION.upper()
