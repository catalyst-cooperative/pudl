"""Test the pudl_sqlite/pudl_duckdb assets that rebuild databases from Parquet."""

import sqlite3
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.dagster.assets.output.databases import (
    TableWriteErrorInfo,
    TableWriteReport,
    _copy_table,
    _has_integer_rowid_alias_pk,
    _validate_primary_key,
    _write_pudl_duckdb,
    _write_pudl_sqlite,
)
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths

DB_TYPES = ["sqlite", "duckdb"]
"""The two database formats every cross-format test is parametrized over."""


def _db_path(db_type: str, paths: PudlPaths) -> Path:
    """Where the writer for ``db_type`` puts its output file."""
    return (
        paths.sqlite_path("pudl") if db_type == "sqlite" else paths.duckdb_path("pudl")
    )


def _write_db(
    db_type: str, table_names: Sequence[str], paths: PudlPaths
) -> TableWriteReport:
    """Dispatch to the writer for ``db_type`` so parametrized tests stay terse."""
    writer = _write_pudl_sqlite if db_type == "sqlite" else _write_pudl_duckdb
    return writer(table_names, paths)


@contextmanager
def _destination(
    db_type: str, paths: PudlPaths, package: Package
) -> Generator[tuple[duckdb.DuckDBPyConnection, Callable[[str], str]]]:
    """Create ``package``'s empty schema and open the connection ``_copy_table`` uses.

    Mirrors the setup inside ``_write_pudl_sqlite`` / ``_write_pudl_duckdb`` so
    ``_copy_table`` can be exercised in isolation: lay down the schema through a
    throwaway SQLAlchemy engine, then yield the DuckDB connection the inserts run
    through (in-memory with the SQLite file attached, or the DuckDB file directly) and
    the function that quotes a destination table reference, closing the connection on
    exit.
    """
    db_path = _db_path(db_type, paths)
    engine = sa.create_engine(f"{db_type}:///{db_path}")
    if db_type == "sqlite":
        metadata = package.to_sql(
            dialect="sqlite", check_types=False, check_values=False
        )
    else:
        engine.dialect.max_identifier_length = 255
        metadata = package.to_sql(dialect="duckdb", include_foreign_keys=False)
    metadata.create_all(engine)
    engine.dispose()

    if db_type == "sqlite":
        cm = duckdb.connect()
        prefix = "pudl_sqlite."
    else:
        cm = duckdb.connect(str(db_path))
        prefix = ""
    with cm as conn:
        if db_type == "sqlite":
            conn.execute("LOAD sqlite")
            conn.execute(f"ATTACH '{db_path}' AS pudl_sqlite (TYPE sqlite)")
        yield conn, lambda table: f'{prefix}"{table}"'


@pytest.fixture
def test_pkg() -> Package:
    """A small metadata package covering the schema shapes the tests care about."""
    utility = Resource(
        name="utility",
        description="Utility (single-column integer PK -- a SQLite ROWID alias)",
        schema={
            "fields": [
                {
                    "name": "utility_id_eia",
                    "type": "integer",
                    "description": "utility_id_eia",
                },
                {
                    "name": "utility_name_eia",
                    "type": "string",
                    "constraints": {"required": True, "pattern": "^[A-Za-z ]+$"},
                    "description": "utility_name_eia",
                },
            ],
            "primary_key": ["utility_id_eia"],
        },
    )
    plant = Resource(
        name="plant",
        description="Plant (foreign key to utility)",
        schema={
            "fields": [
                {
                    "name": "plant_id_eia",
                    "type": "integer",
                    "description": "plant_id_eia",
                },
                {
                    "name": "plant_name_eia",
                    "type": "string",
                    "constraints": {"required": True},
                    "description": "plant_name_eia",
                },
                {
                    "name": "utility_id_eia",
                    "type": "integer",
                    "description": "utility_id_eia",
                },
            ],
            "primary_key": ["plant_id_eia"],
            "foreign_keys": [
                {
                    "fields": ["utility_id_eia"],
                    "reference": {
                        "resource": "utility",
                        "fields": ["utility_id_eia"],
                    },
                }
            ],
        },
    )
    boiler_generator_assn = Resource(
        name="boiler_generator_assn",
        description="Boiler-generator association (composite PK)",
        schema={
            "fields": [
                {
                    "name": "plant_id_eia",
                    "type": "integer",
                    "description": "plant_id_eia",
                },
                {
                    "name": "generator_id",
                    "type": "integer",
                    "description": "generator_id",
                },
            ],
            "primary_key": ["plant_id_eia", "generator_id"],
        },
    )
    fuel_type = Resource(
        name="fuel_type",
        description="Fuel type code (single-column non-integer PK)",
        schema={
            "fields": [
                {
                    "name": "fuel_type_code",
                    "type": "string",
                    "description": "fuel_type_code",
                }
            ],
            "primary_key": ["fuel_type_code"],
        },
    )
    # Two resources sharing an enum-constrained field of the same name, to check the
    # DuckDB dialect's native ENUM type is created once and reused, not re-declared.
    status_schema = {
        "fields": [
            {"name": "id", "type": "integer", "description": "id"},
            {
                "name": "operational_status_code",
                "type": "string",
                "constraints": {"required": True, "enum": ["existing", "retired"]},
                "description": "operational_status_code",
            },
        ],
        "primary_key": ["id"],
    }
    generator_status = Resource(
        name="generator_status", schema=status_schema, description="Generator status"
    )
    boiler_status = Resource(
        name="boiler_status", schema=status_schema, description="Boiler status"
    )

    return Package(
        name="eia860",
        resources=[
            plant,
            utility,
            boiler_generator_assn,
            fuel_type,
            generator_status,
            boiler_status,
        ],
    )


@pytest.fixture
def paths(tmp_path: Path) -> PudlPaths:
    """A PudlPaths with sibling pudl_input/pudl_output dirs, and a parquet/ subdir."""
    pudl_output = tmp_path / "output"
    (pudl_output / "parquet").mkdir(parents=True)
    return PudlPaths(pudl_input=tmp_path / "input", pudl_output=pudl_output)


def _write_parquet(paths: PudlPaths, table_name: str, df: pd.DataFrame) -> None:
    df.to_parquet(paths.parquet_path(table_name))


def _row_count(db_type: str, db_path: Path, table_name: str) -> int:
    """Row count of ``table_name``, read back through a SQLAlchemy engine."""
    engine = sa.create_engine(f"{db_type}:///{db_path}")
    try:
        with engine.connect() as conn:
            return conn.exec_driver_sql(
                f'SELECT count(*) FROM "{table_name}"'  # noqa: S608
            ).scalar_one()
    finally:
        engine.dispose()


################################################################################
# Primary-key pre-write check (SQLite's ROWID-alias nonsense)
################################################################################


@pytest.mark.parametrize(
    ("resource_name", "is_rowid_alias"),
    [
        ("utility", True),  # single-column integer PK
        ("boiler_generator_assn", False),  # composite PK
        ("fuel_type", False),  # single-column non-integer PK
    ],
)
def test_has_integer_rowid_alias_pk(
    test_pkg: Package, resource_name: str, is_rowid_alias: bool
):
    """Only a single-column integer/year PK is treated by SQLite as a ROWID alias."""
    resource = test_pkg.get_resource(resource_name)
    assert _has_integer_rowid_alias_pk(resource) is is_rowid_alias


def test_validate_primary_key_wraps_check_primary_key_errors(
    paths: PudlPaths, test_pkg: Package
):
    """Errors from resource.check_primary_key() are wrapped in a ValueError.

    check_primary_key() behavior is tested in tests/unit/metadata/metadata_test.py, this
    only checks that the error is properly wrapped/attributed to the right table.
    """
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1, 1], "utility_name_eia": ["A", "B"]}),
    )
    with pytest.raises(ValueError, match="utility"):
        _validate_primary_key(test_pkg.get_resource("utility"), paths)


def test_validate_primary_key_passes_valid_data(paths: PudlPaths, test_pkg: Package):
    """No exception is raised when resource.check_primary_key() finds nothing wrong."""
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1, 2], "utility_name_eia": ["A", "B"]}),
    )
    _validate_primary_key(test_pkg.get_resource("utility"), paths)


@pytest.mark.parametrize("resource_name", ["boiler_generator_assn", "fuel_type"])
def test_validate_primary_key_skips_non_rowid_alias_pk(
    paths: PudlPaths, test_pkg: Package, resource_name: str
):
    """PKs SQLite enforces correctly are skipped without even reading the Parquet.

    This exercises _validate_primary_key's own gating logic
    (_has_integer_rowid_alias_pk), not resource.check_primary_key()'s -- no Parquet
    file is written for these resources, so if the skip logic broke, scanning a
    nonexistent file would raise instead of silently passing.
    """
    _validate_primary_key(test_pkg.get_resource(resource_name), paths)


def test_sqlite_integer_pk_is_a_rowid_alias(tmp_path: Path):
    """A single-column ``INTEGER`` PK is a ROWID alias: a NULL insert is silently
    replaced with an auto-assigned rowid instead of raising, despite ``NOT NULL``.

    This is the quirk _has_integer_rowid_alias_pk / _validate_primary_key guard
    against.
    """
    con = sqlite3.connect(tmp_path / "rowid.sqlite")
    try:
        con.execute("CREATE TABLE t (i INTEGER NOT NULL, PRIMARY KEY (i))")
        con.execute("INSERT INTO t (i) VALUES (NULL)")
        assert con.execute("SELECT i FROM t").fetchone() == (1,)
    finally:
        con.close()


def test_sqlite_bigint_pk_is_not_a_rowid_alias(tmp_path: Path):
    """A ``BIGINT`` PK has INTEGER affinity but is *not* a ROWID alias, so SQLite
    enforces its ``NOT NULL`` constraint like any other column.

    Since PUDL now emits BIGINT for its ``integer`` fields (see
    test_pudl_sqlite_schema_uses_bigint_for_integer_fields), the
    _has_integer_rowid_alias_pk / _validate_primary_key guard is currently
    belt-and-suspenders rather than load-bearing.
    """
    con = sqlite3.connect(tmp_path / "rowid.sqlite")
    try:
        con.execute("CREATE TABLE t (i BIGINT NOT NULL, PRIMARY KEY (i))")
        with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
            con.execute("INSERT INTO t (i) VALUES (NULL)")
    finally:
        con.close()


def test_pudl_sqlite_schema_uses_bigint_for_integer_fields(test_pkg: Package):
    """PUDL's SQLite DDL declares ``integer`` primary-key columns as BIGINT."""
    metadata = test_pkg.to_sql(dialect="sqlite", check_types=False, check_values=False)
    column = metadata.tables["utility"].columns["utility_id_eia"]
    ddl_type = column.type.compile(sa.create_engine("sqlite://").dialect)
    assert ddl_type == "BIGINT"


################################################################################
# Per-table failure reporting
################################################################################


def test_table_write_error_str_includes_debugging_context():
    """The error string identifies the table, exception type, and message."""
    error = TableWriteErrorInfo("utility", ValueError("bad primary key"))
    assert str(error) == "utility: ValueError: bad primary key"


@pytest.mark.parametrize(
    ("report", "expected_substrings"),
    [
        (
            TableWriteReport(
                db_path=Path("pudl.sqlite"),
                row_counts={"plant": 1},
                errors=[TableWriteErrorInfo("utility", ValueError("bad primary key"))],
            ),
            ["1/2", "utility: ValueError: bad primary key"],
        ),
        (
            TableWriteReport(
                db_path=Path("pudl.sqlite"),
                row_counts={"utility": 1, "plant": 1},
                errors=[],
            ),
            ["Wrote all 2 table(s)."],
        ),
    ],
    ids=["with_failures", "all_success"],
)
def test_table_write_report_summary(
    report: TableWriteReport, expected_substrings: list[str]
):
    """The summary names every failed table, or says so plainly when none failed."""
    summary = report.summary()
    for substring in expected_substrings:
        assert substring in summary


################################################################################
# _copy_table, against both a real SQLite and a real DuckDB destination
################################################################################


@pytest.mark.parametrize("db_type", DB_TYPES)
def test_copy_table_returns_row_count(
    db_type: str, paths: PudlPaths, test_pkg: Package
):
    """The number of rows in the destination table after the insert is returned."""
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame(
            {"utility_id_eia": [1, 2, 3], "utility_name_eia": ["A", "B", "C"]}
        ),
    )
    with _destination(db_type, paths, test_pkg) as (conn, table_ref):
        row_count = _copy_table(
            conn,
            test_pkg.get_resource("utility"),
            paths.parquet_path("utility"),
            table_ref=table_ref("utility"),
        )
    assert row_count == 3


@pytest.mark.parametrize("db_type", DB_TYPES)
def test_copy_table_reports_constraint_violation_as_duckdb_error(
    db_type: str, paths: PudlPaths, test_pkg: Package
):
    """A NOT NULL violation surfaces as duckdb.Error -- what _WRITE_EXCEPTIONS catches."""
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": [None]}),
    )
    with (
        _destination(db_type, paths, test_pkg) as (conn, table_ref),
        pytest.raises(duckdb.Error, match="NOT NULL"),
    ):
        _copy_table(
            conn,
            test_pkg.get_resource("utility"),
            paths.parquet_path("utility"),
            table_ref=table_ref("utility"),
        )


@pytest.mark.parametrize("db_type", DB_TYPES)
def test_copy_table_missing_column_raises_binder_error(
    db_type: str, paths: PudlPaths, test_pkg: Package
):
    """A Parquet file missing a declared column raises a DuckDB binder error.

    ``duckdb.BinderException`` is a ``duckdb.Error`` subclass, so a schema mismatch
    is caught per-table by the writers rather than aborting the run.
    """
    _write_parquet(paths, "utility", pd.DataFrame({"utility_id_eia": [1]}))
    with (
        _destination(db_type, paths, test_pkg) as (conn, table_ref),
        pytest.raises(duckdb.BinderException, match="utility_name_eia"),
    ):
        _copy_table(
            conn,
            test_pkg.get_resource("utility"),
            paths.parquet_path("utility"),
            table_ref=table_ref("utility"),
        )


################################################################################
# Schema issues -- tricky things having to do with SQLite or DuckDB schemata
################################################################################


def test_duckdb_schema_shares_enum_type_across_tables(test_pkg: Package):
    """A named ENUM type shared by two tables is created exactly once.

    Regression test: the (unrelated) Field.to_duckdb_dtype() helper does this via ad
    hoc per-call ``CREATE TYPE``, which errors the second time the same field name is
    reused across tables. Going through a real SQLAlchemy engine + create_all() avoids
    that -- checked here against "generator_status" and "boiler_status", which both
    declare an "operational_status_code" enum field with identical values.
    """
    metadata = test_pkg.to_sql(dialect="duckdb", include_foreign_keys=False)
    engine = sa.create_engine("duckdb:///:memory:")
    engine.dialect.max_identifier_length = 255
    metadata.create_all(engine)  # should not raise CatalogException
    with engine.connect() as conn:
        conn.exec_driver_sql(
            'INSERT INTO "generator_status" VALUES (1, ?)', ("existing",)
        )
        conn.exec_driver_sql('INSERT INTO "boiler_status" VALUES (1, ?)', ("retired",))
        conn.commit()
        with pytest.raises(sa.exc.DBAPIError):
            conn.exec_driver_sql(
                'INSERT INTO "generator_status" VALUES (2, ?)', ("bogus",)
            )
    engine.dispose()


def test_duckdb_build_persists_enum_constraint(
    paths: PudlPaths, test_pkg: Package, mocker
):
    """An enum-constrained column lands in the built pudl.duckdb as a real ENUM.

    Runs the full _write_pudl_duckdb path (SQLAlchemy create_all + DuckDB inserts) and
    then reopens the file with the native DuckDB API -- the way downstream users do
    -- to confirm sa.Enum compiled to an enforced ENUM column rather than being
    silently dropped to VARCHAR by duckdb-engine. Also shows the enum values land in
    the sorted order FieldConstraints.enum guarantees.
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "generator_status",
        pd.DataFrame(
            {"id": [1, 2], "operational_status_code": ["retired", "existing"]}
        ),
    )

    report = _write_pudl_duckdb(["generator_status"], paths)
    assert report.errors == []
    db_path = report.db_path

    with duckdb.connect(str(db_path), read_only=True) as conn:
        row = conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'generator_status' "
            "AND column_name = 'operational_status_code'"
        ).fetchone()
    assert row is not None
    assert row[0] == "ENUM('existing', 'retired')"

    with duckdb.connect(str(db_path)) as conn, pytest.raises(duckdb.Error):
        conn.execute("INSERT INTO generator_status VALUES (3, 'bogus')")


################################################################################
# End to end, against both backends
################################################################################


@pytest.mark.parametrize("db_type", DB_TYPES)
def test_write_pudl_db_end_to_end(
    db_type: str,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """Building a database from Parquet reproduces the row counts of the inputs."""
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1, 2], "utility_name_eia": ["A", "B"]}),
    )
    _write_parquet(
        paths,
        "plant",
        pd.DataFrame(
            {
                "plant_id_eia": [1, 2],
                "plant_name_eia": ["Plant A", "Plant B"],
                "utility_id_eia": [1, 1],
            }
        ),
    )

    report = _write_db(db_type, ["utility", "plant"], paths)

    assert report.row_counts == {"utility": 2, "plant": 2}
    assert report.errors == []
    assert report.db_path.exists()
    assert _row_count(db_type, report.db_path, "utility") == 2
    assert _row_count(db_type, report.db_path, "plant") == 2


@pytest.mark.parametrize("db_type", DB_TYPES)
def test_write_pudl_db_replaces_existing_file(
    db_type: str,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """A stale file at the destination is replaced, not appended to."""
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": ["A"]}),
    )
    _db_path(db_type, paths).write_text("not a real database file")

    report = _write_db(db_type, ["utility"], paths)

    assert report.errors == []
    assert _row_count(db_type, report.db_path, "utility") == 1


@pytest.mark.parametrize("db_type", DB_TYPES)
def test_write_pudl_db_continues_past_failing_table(
    db_type: str,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """A data-quality failure in one table is recorded; the others still load."""
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    # "utility" violates NOT NULL on utility_name_eia (enforced by both backends);
    # "plant" is valid.
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": [None]}),
    )
    _write_parquet(
        paths,
        "plant",
        pd.DataFrame(
            {
                "plant_id_eia": [1],
                "plant_name_eia": ["Plant A"],
                "utility_id_eia": [1],
            }
        ),
    )

    report = _write_db(db_type, ["utility", "plant"], paths)

    assert report.row_counts == {"plant": 1}
    assert report.failed_tables == ["utility"]
    assert len(report.errors) == 1
    assert isinstance(report.errors[0].exception, duckdb.Error)
    assert _row_count(db_type, report.db_path, "plant") == 1
    assert _row_count(db_type, report.db_path, "utility") == 0
