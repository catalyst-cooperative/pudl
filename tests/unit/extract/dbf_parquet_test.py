"""Tests for converting FERC DBF databases into Parquet."""

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

import pudl
from pudl.extract.dbf import convert_db_into_parquet


class _RecordingConnection:
    """A DuckDB connection that records the statements it executes."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection
        self.statements: list[str] = []

    def __enter__(self) -> _RecordingConnection:
        return self

    def __exit__(self, *exc_info) -> None:
        self._connection.close()

    def sql(self, query: str):
        return self._connection.sql(query)

    def execute(self, query: str):
        self.statements.append(query)
        return self._connection.execute(query)


def _make_db(path: Path) -> None:
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE TABLE f1_test AS SELECT range AS a FROM range(10)")


def test_convert_db_into_parquet_codec(tmp_path: Path) -> None:
    """The Parquet files converted from a DuckDB database use the shared codec."""
    db_path = tmp_path / "ferc.duckdb"
    _make_db(db_path)

    convert_db_into_parquet(db_path, tmp_path / "parquet")

    row_group = pq.read_metadata(tmp_path / "parquet" / "f1_test.parquet").row_group(0)
    assert row_group.column(0).compression == pudl.PARQUET_COMPRESSION.upper()


def test_convert_db_into_parquet_compression_level(tmp_path: Path, mocker) -> None:
    """The conversion asks DuckDB for the codec and level set centrally in ``pudl``.

    The level isn't recorded in the Parquet files, so this checks the ``COPY``
    statements that DuckDB is given.
    """
    mocker.patch("pudl.PARQUET_COMPRESSION_LEVEL", 7)
    db_path = tmp_path / "ferc.duckdb"
    _make_db(db_path)
    connections: list[_RecordingConnection] = []
    real_connect = duckdb.connect

    def recording_connect(*args, **kwargs) -> _RecordingConnection:
        connections.append(_RecordingConnection(real_connect(*args, **kwargs)))
        return connections[-1]

    mocker.patch.object(duckdb, "connect", side_effect=recording_connect)

    convert_db_into_parquet(db_path, tmp_path / "parquet")

    (copy_statement,) = [s for c in connections for s in c.statements]
    assert f"COMPRESSION {pudl.PARQUET_COMPRESSION}," in copy_statement
    assert "COMPRESSION_LEVEL 7" in copy_statement
