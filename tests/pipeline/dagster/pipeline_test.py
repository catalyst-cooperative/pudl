"""Integration tests verifying the Dagster pipeline produces expected outputs."""

import duckdb
import pytest

from pudl.helpers import get_parquet_table

EXPECTED_TABLES = (
    "core_pudl__entity_plants_pudl",
    "core_pudl__entity_utilities_pudl",
)


@pytest.mark.order(2)
@pytest.mark.usefixtures("prebuilt_outputs")
def test_pudl_parquet_outputs():
    """Verify that key PUDL tables exist and are populated in the Parquet outputs.

    Foreign key validation lives in a separate data-validation test so the nightly
    build can report it independently from the rest of the integration suite.
    """
    for table_name in EXPECTED_TABLES:
        df = get_parquet_table(table_name)
        assert not df.empty, f"Expected {table_name} to contain data."


@pytest.mark.order(2)
def test_pudl_sqlite_connection(pudl_sqlite_connection: duckdb.DuckDBPyConnection):
    """Verify that key PUDL tables exist and are populated in pudl.sqlite."""
    table_names = {
        row[0]
        for row in pudl_sqlite_connection.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    for table_name in EXPECTED_TABLES:
        assert table_name in table_names
        first_row_exists = (
            pudl_sqlite_connection.execute(
                f'SELECT 1 FROM "{table_name}" LIMIT 1'  # noqa: S608
            ).fetchone()
            is not None
        )
        assert first_row_exists, f"Expected {table_name} to contain data."


@pytest.mark.order(2)
def test_pudl_duckdb_connection(pudl_duckdb_connection: duckdb.DuckDBPyConnection):
    """Verify that key PUDL tables exist, are populated, and are documented.

    DuckDB (unlike SQLite) actually persists the table/column ``comment``s that
    Resource.to_sql()/Field.to_sql() attach -- see pudl.metadata.classes -- so this
    also checks that those descriptions really landed in the built database.
    """
    table_names = {
        row[0]
        for row in pudl_duckdb_connection.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    for table_name in EXPECTED_TABLES:
        assert table_name in table_names
        first_row_exists = (
            pudl_duckdb_connection.execute(
                f'SELECT 1 FROM "{table_name}" LIMIT 1'  # noqa: S608
            ).fetchone()
            is not None
        )
        assert first_row_exists, f"Expected {table_name} to contain data."

        table_comment_row = pudl_duckdb_connection.execute(
            "SELECT comment FROM duckdb_tables() WHERE table_name = ?", [table_name]
        ).fetchone()
        assert table_comment_row is not None, f"Expected {table_name} to exist."
        assert table_comment_row[0], f"Expected {table_name} to have a table comment."

        column_comments = [
            row[0]
            for row in pudl_duckdb_connection.execute(
                "SELECT comment FROM duckdb_columns() WHERE table_name = ?",
                [table_name],
            ).fetchall()
        ]
        assert column_comments, f"Expected {table_name} to have columns."
        assert all(column_comments), (
            f"Expected every column in {table_name} to have a comment."
        )
