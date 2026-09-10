"""Check that pudl.sqlite, pudl.duckdb, and the Parquet outputs agree.

Every table defined in :data:`PUDL_PACKAGE` should exist in both the SQLite and
DuckDB databases, neither database should contain any extra tables, and each
table should have the same columns and the same number of rows in SQLite,
DuckDB, and its source Parquet file under ``$PUDL_OUTPUT/parquet/``.
"""

import duckdb
import pytest

from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

# Obtained the same way the pudl_sqlite / pudl_duckdb assets pick which tables to
# write (see pudl.dagster.assets._find_sql_asset_keys), so the two lists hopefully
# stay in sync... also we're going to delete SQLite soon so YOLO.
EXPECTED_TABLES: list[str] = [
    table.name for table in PUDL_PACKAGE.to_sql().sorted_tables
]


def _scalar(
    conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None
) -> int:
    result = (conn.execute(sql, params) if params else conn.execute(sql)).fetchone()
    assert result is not None
    return result[0]


def _tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return {name for (name,) in rows}


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchall()
    return {name for (name,) in rows}


def _parquet_columns(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> set[str]:
    rows = conn.execute(
        "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet(?))",
        [parquet_path],
    ).fetchall()
    return {name for (name,) in rows}


def test_expected_tables_present(
    pudl_sqlite_connection: duckdb.DuckDBPyConnection,
    pudl_duckdb_connection: duckdb.DuckDBPyConnection,
):
    """Every table in PUDL_PACKAGE exists in both databases."""
    expected = set(EXPECTED_TABLES)
    missing_sqlite = expected - _tables(pudl_sqlite_connection)
    missing_duckdb = expected - _tables(pudl_duckdb_connection)
    assert not missing_sqlite, (
        f"Tables missing from pudl.sqlite: {sorted(missing_sqlite)}"
    )
    assert not missing_duckdb, (
        f"Tables missing from pudl.duckdb: {sorted(missing_duckdb)}"
    )


def test_no_extra_tables(
    pudl_sqlite_connection: duckdb.DuckDBPyConnection,
    pudl_duckdb_connection: duckdb.DuckDBPyConnection,
):
    """Neither database contains tables that aren't defined in PUDL_PACKAGE."""
    expected = set(EXPECTED_TABLES)
    extra_sqlite = _tables(pudl_sqlite_connection) - expected
    extra_duckdb = _tables(pudl_duckdb_connection) - expected
    assert not extra_sqlite, f"Unexpected tables in pudl.sqlite: {sorted(extra_sqlite)}"
    assert not extra_duckdb, f"Unexpected tables in pudl.duckdb: {sorted(extra_duckdb)}"


@pytest.mark.parametrize("table_name", EXPECTED_TABLES)
def test_columns_match(
    table_name: str,
    pudl_sqlite_connection: duckdb.DuckDBPyConnection,
    pudl_duckdb_connection: duckdb.DuckDBPyConnection,
    pudl_test_paths: PudlPaths,
):
    """The set of columns agrees across SQLite, DuckDB, and the source Parquet file."""
    cols_sqlite = _table_columns(pudl_sqlite_connection, table_name)
    cols_duckdb = _table_columns(pudl_duckdb_connection, table_name)

    parquet_path = pudl_test_paths.parquet_path(table_name)
    assert parquet_path.exists(), f"Missing Parquet file: {parquet_path}"
    cols_parquet = _parquet_columns(pudl_duckdb_connection, str(parquet_path))

    assert cols_sqlite == cols_duckdb == cols_parquet, (
        f"Column mismatch for {table_name}: "
        f"sqlite-only={sorted(cols_sqlite - cols_duckdb - cols_parquet)}, "
        f"duckdb-only={sorted(cols_duckdb - cols_sqlite - cols_parquet)}, "
        f"parquet-only={sorted(cols_parquet - cols_sqlite - cols_duckdb)}"
    )


@pytest.mark.parametrize("table_name", EXPECTED_TABLES)
def test_row_counts_match(
    table_name: str,
    pudl_sqlite_connection: duckdb.DuckDBPyConnection,
    pudl_duckdb_connection: duckdb.DuckDBPyConnection,
    pudl_test_paths: PudlPaths,
):
    """Row counts agree across SQLite, DuckDB, and the source Parquet file."""
    count_sql = f'SELECT COUNT(*) FROM "{table_name}"'  # noqa: S608
    n_sqlite = _scalar(pudl_sqlite_connection, count_sql)
    n_duckdb = _scalar(pudl_duckdb_connection, count_sql)

    parquet_path = pudl_test_paths.parquet_path(table_name)
    assert parquet_path.exists(), f"Missing Parquet file: {parquet_path}"
    n_parquet = _scalar(
        pudl_duckdb_connection,
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(parquet_path)],
    )

    assert n_sqlite == n_duckdb == n_parquet, (
        f"Row count mismatch for {table_name}: "
        f"sqlite={n_sqlite}, duckdb={n_duckdb}, parquet={n_parquet}"
    )
