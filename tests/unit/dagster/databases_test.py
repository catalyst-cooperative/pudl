"""Test the pudl_sqlite/pudl_duckdb assets that rebuild databases from Parquet."""

from pathlib import Path

import duckdb
import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.dagster.assets.output.databases import (
    _SQLITE_ATTACH_ALIAS,
    DUCKDB_TARGET,
    SQLITE_TARGET,
    TableWriteError,
    TableWriteReport,
    _copy_table,
    _DatabaseTarget,
    _has_integer_rowid_alias_pk,
    _validate_primary_key,
    write_pudl_duckdb,
    write_pudl_sqlite,
)
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths

# duckdb-engine's SQLAlchemy dialect inherits Postgres' 63-character identifier
# length limit, which DuckDB itself doesn't actually have -- see the matching
# constant/comment in databases.py.
_DUCKDB_MAX_IDENTIFIER_LENGTH = 255


@pytest.fixture
def test_pkg() -> Package:
    """A small metadata package covering the schema shapes the tests care about."""
    artist = Resource(
        name="artist",
        description="Artist (single-column integer PK -- a SQLite ROWID alias)",
        schema={
            "fields": [
                {"name": "artistid", "type": "integer", "description": "artistid"},
                {
                    "name": "artistname",
                    "type": "string",
                    "constraints": {"required": True, "pattern": "^[A-Za-z ]+$"},
                    "description": "artistname",
                },
            ],
            "primary_key": ["artistid"],
        },
    )
    track = Resource(
        name="track",
        description="Track (foreign key to artist)",
        schema={
            "fields": [
                {"name": "trackid", "type": "integer", "description": "trackid"},
                {
                    "name": "trackname",
                    "type": "string",
                    "constraints": {"required": True},
                    "description": "trackname",
                },
                {
                    "name": "trackartist",
                    "type": "integer",
                    "description": "trackartist",
                },
            ],
            "primary_key": ["trackid"],
            "foreign_keys": [
                {
                    "fields": ["trackartist"],
                    "reference": {"resource": "artist", "fields": ["artistid"]},
                }
            ],
        },
    )
    track_label = Resource(
        name="track_label",
        description="Track label (composite PK)",
        schema={
            "fields": [
                {"name": "trackid", "type": "integer", "description": "trackid"},
                {"name": "labelid", "type": "integer", "description": "labelid"},
            ],
            "primary_key": ["trackid", "labelid"],
        },
    )
    genre = Resource(
        name="genre",
        description="Genre (single-column non-integer PK)",
        schema={
            "fields": [{"name": "genrename", "type": "string", "description": "genre"}],
            "primary_key": ["genrename"],
        },
    )
    # Two resources sharing an enum-constrained field of the same name, to check the
    # DuckDB dialect's native ENUM type is created once and reused, not re-declared.
    status_schema = {
        "fields": [
            {"name": "id", "type": "integer", "description": "id"},
            {
                "name": "status",
                "type": "string",
                "constraints": {"required": True, "enum": ["active", "inactive"]},
                "description": "status",
            },
        ],
        "primary_key": ["id"],
    }
    status_a = Resource(name="status_a", schema=status_schema, description="Status A")
    status_b = Resource(name="status_b", schema=status_schema, description="Status B")

    return Package(
        name="music",
        resources=[track, artist, track_label, genre, status_a, status_b],
    )


@pytest.fixture
def paths(tmp_path: Path) -> PudlPaths:
    """A PudlPaths pointing at a scratch tmp_path, with a parquet/ subdir."""
    (tmp_path / "parquet").mkdir()
    return PudlPaths(pudl_input=tmp_path / "input", pudl_output=tmp_path)


def _write_parquet(paths: PudlPaths, table_name: str, df: pd.DataFrame) -> None:
    df.to_parquet(paths.parquet_path(table_name))


def _row_count(target: _DatabaseTarget, db_path: Path, table_name: str) -> int:
    """Row count of ``table_name``, read back through a SQLAlchemy engine."""
    engine = sa.create_engine(target.engine_url(db_path))
    try:
        with engine.connect() as conn:
            return conn.exec_driver_sql(
                f'SELECT count(*) FROM "{table_name}"'  # noqa: S608
            ).scalar_one()
    finally:
        engine.dispose()


################################################################################
# Primary-key pre-write check (SQLite's ROWID-alias footgun)
################################################################################


@pytest.mark.parametrize(
    ("resource_name", "is_rowid_alias"),
    [
        ("artist", True),  # single-column integer PK
        ("track_label", False),  # composite PK
        ("genre", False),  # single-column non-integer PK
    ],
)
def test_has_integer_rowid_alias_pk(
    test_pkg: Package, resource_name: str, is_rowid_alias: bool
):
    """Only a single-column integer/year PK is treated by SQLite as a ROWID alias."""
    resource = test_pkg.get_resource(resource_name)
    assert _has_integer_rowid_alias_pk(resource) is is_rowid_alias


@pytest.mark.parametrize(
    "bad_df",
    [
        pd.DataFrame({"artistid": [1, None], "artistname": ["A", "B"]}),
        pd.DataFrame({"artistid": [1, 1], "artistname": ["A", "B"]}),
    ],
    ids=["null_pk", "duplicate_pk"],
)
def test_validate_primary_key_raises(
    paths: PudlPaths, test_pkg: Package, bad_df: pd.DataFrame
):
    """A null or duplicate primary key value raises ValueError before any write."""
    _write_parquet(paths, "artist", bad_df)
    with pytest.raises(ValueError, match="artist"):
        _validate_primary_key(test_pkg.get_resource("artist"), paths)


def test_validate_primary_key_passes_valid_data(paths: PudlPaths, test_pkg: Package):
    """Valid, unique, non-null primary keys do not raise."""
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1, 2], "artistname": ["A", "B"]})
    )
    _validate_primary_key(test_pkg.get_resource("artist"), paths)


@pytest.mark.parametrize("resource_name", ["track_label", "genre"])
def test_validate_primary_key_skips_non_rowid_alias_pk(
    paths: PudlPaths, test_pkg: Package, resource_name: str
):
    """PKs SQLite enforces correctly are skipped without even reading the Parquet.

    No Parquet file is written for these resources -- if the skip logic broke,
    scanning a nonexistent file would raise instead of silently passing.
    """
    _validate_primary_key(test_pkg.get_resource(resource_name), paths)


################################################################################
# Per-table failure reporting
################################################################################


def test_table_write_error_str_includes_debugging_context():
    """The error string identifies the table, exception type, and message."""
    error = TableWriteError("artist", ValueError("bad primary key"))
    assert str(error) == "artist: ValueError: bad primary key"


@pytest.mark.parametrize(
    ("report", "expected_substrings"),
    [
        (
            TableWriteReport(
                row_counts={"track": 1},
                errors=[TableWriteError("artist", ValueError("bad primary key"))],
            ),
            ["1/2", "artist: ValueError: bad primary key"],
        ),
        (
            TableWriteReport(row_counts={"artist": 1, "track": 1}, errors=[]),
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


@pytest.fixture(params=["sqlite", "duckdb"])
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def target(backend: str) -> _DatabaseTarget:
    return SQLITE_TARGET if backend == "sqlite" else DUCKDB_TARGET


@pytest.fixture
def write_db(backend: str):
    """The public ``write_pudl_*`` entry point for the parametrized backend."""
    return write_pudl_sqlite if backend == "sqlite" else write_pudl_duckdb


@pytest.fixture
def db_path(backend: str, tmp_path: Path) -> Path:
    return tmp_path / f"pudl.{backend}"


@pytest.fixture
def db_con(target: _DatabaseTarget, db_path: Path, test_pkg: Package, mocker):
    """An open DuckDB connection writing into a freshly-created schema for ``target``.

    Mirrors ``_write_pudl_db``'s setup: build the empty schema through a throwaway
    SQLAlchemy engine, then open the DuckDB connection the inserts run through
    (``ATTACH``ing the file for the SQLite target).
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    engine = sa.create_engine(target.engine_url(db_path))
    if target.max_identifier_length is not None:
        engine.dialect.max_identifier_length = target.max_identifier_length
    target.build_metadata().create_all(engine)
    engine.dispose()

    con = duckdb.connect() if target.attach_as_sqlite else duckdb.connect(str(db_path))
    if target.attach_as_sqlite:
        con.execute("LOAD sqlite")
        con.execute(f"ATTACH '{db_path}' AS {_SQLITE_ATTACH_ALIAS} (TYPE sqlite)")
    try:
        yield con
    finally:
        con.close()


def test_copy_table_returns_row_count(
    db_con: duckdb.DuckDBPyConnection,
    target: _DatabaseTarget,
    paths: PudlPaths,
    test_pkg: Package,
):
    """The number of rows in the destination table after the insert is returned."""
    _write_parquet(
        paths,
        "artist",
        pd.DataFrame({"artistid": [1, 2, 3], "artistname": ["A", "B", "C"]}),
    )
    row_count = _copy_table(
        db_con,
        test_pkg.get_resource("artist"),
        paths.parquet_path("artist"),
        table_ref=target.table_ref("artist"),
    )
    assert row_count == 3


def test_copy_table_reports_constraint_violation_as_duckdb_error(
    db_con: duckdb.DuckDBPyConnection,
    target: _DatabaseTarget,
    paths: PudlPaths,
    test_pkg: Package,
):
    """A NOT NULL violation surfaces as duckdb.Error -- what _WRITE_EXCEPTIONS catches."""
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": [None]})
    )
    with pytest.raises(duckdb.Error, match="NOT NULL"):
        _copy_table(
            db_con,
            test_pkg.get_resource("artist"),
            paths.parquet_path("artist"),
            table_ref=target.table_ref("artist"),
        )


def test_copy_table_missing_column_raises_binder_error(
    db_con: duckdb.DuckDBPyConnection,
    target: _DatabaseTarget,
    paths: PudlPaths,
    test_pkg: Package,
):
    """A Parquet file missing a declared column raises a DuckDB binder error.

    ``duckdb.BinderException`` is a ``duckdb.Error`` subclass, so a schema mismatch
    is caught per-table by ``_write_pudl_db`` rather than aborting the run.
    """
    _write_parquet(paths, "artist", pd.DataFrame({"artistid": [1]}))
    with pytest.raises(duckdb.BinderException, match="artistname"):
        _copy_table(
            db_con,
            test_pkg.get_resource("artist"),
            paths.parquet_path("artist"),
            table_ref=target.table_ref("artist"),
        )


################################################################################
# Schema shape -- deliberate SQLite / DuckDB design choices
################################################################################


def test_sqlite_schema_omits_check_constraints(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """CHECK constraints are dropped from the SQLite schema (data is validated upstream).

    "artist".artistname has a ``^[A-Za-z ]+$`` pattern; "123" violates it but must
    still write, since the lean SQLite schema omits CHECK for insert speed.
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["123"]})
    )
    report = write_pudl_sqlite(tmp_path / "pudl.sqlite", ["artist"], paths=paths)
    assert report.row_counts == {"artist": 1}
    assert report.errors == []


def test_sqlite_foreign_keys_declared_but_not_enforced(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """The SQLite schema keeps FK definitions, but SQLite doesn't enforce them on write.

    SQLite only checks foreign keys under ``PRAGMA foreign_keys = ON`` (it defaults
    OFF and DuckDB never sets it), so a row pointing at a non-existent artist still
    writes -- while the FK definition stays visible for downstream users.
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["A"]})
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame({"trackid": [1], "trackname": ["T1"], "trackartist": [999]}),
    )
    sqlite_path = tmp_path / "pudl.sqlite"
    report = write_pudl_sqlite(sqlite_path, ["artist", "track"], paths=paths)
    assert report.row_counts == {"artist": 1, "track": 1}  # dangling FK still written

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    assert sa.inspect(engine).get_foreign_keys("track")  # ...but FK still declared
    engine.dispose()


def test_duckdb_schema_excludes_foreign_keys(test_pkg: Package):
    """The DuckDB schema has no foreign key constraints; the SQLite schema keeps them.

    "track" declares a foreign key to "artist" in the fixture package -- confirms
    ``include_foreign_keys=False`` takes effect for DuckDB but not for SQLite.
    """
    duckdb_metadata = test_pkg.to_sql(dialect="duckdb", include_foreign_keys=False)
    assert list(duckdb_metadata.tables["track"].foreign_keys) == []

    sqlite_metadata = test_pkg.to_sql()
    assert list(sqlite_metadata.tables["track"].foreign_keys) != []


def test_duckdb_schema_shares_enum_type_across_tables(test_pkg: Package):
    """A named ENUM type shared by two tables is created exactly once.

    Regression test: the (unrelated) Field.to_duckdb_dtype() helper does this via ad
    hoc per-call ``CREATE TYPE``, which errors the second time the same field name is
    reused across tables. Going through a real SQLAlchemy engine + create_all() avoids
    that -- checked here against "status_a" and "status_b", which both declare a
    "status" enum field with identical values.
    """
    metadata = test_pkg.to_sql(dialect="duckdb", include_foreign_keys=False)
    engine = sa.create_engine("duckdb:///:memory:")
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
    metadata.create_all(engine)  # should not raise CatalogException
    with engine.connect() as conn:
        conn.exec_driver_sql('INSERT INTO "status_a" VALUES (1, ?)', ("active",))
        conn.exec_driver_sql('INSERT INTO "status_b" VALUES (1, ?)', ("inactive",))
        conn.commit()
        with pytest.raises(sa.exc.DBAPIError):
            conn.exec_driver_sql('INSERT INTO "status_a" VALUES (2, ?)', ("bogus",))
    engine.dispose()


################################################################################
# End to end, against both backends
################################################################################


def test_write_pudl_db_end_to_end(
    write_db,
    target: _DatabaseTarget,
    db_path: Path,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """Building a database from Parquet reproduces the row counts of the inputs."""
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1, 2], "artistname": ["A", "B"]})
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame(
            {"trackid": [1, 2], "trackname": ["T1", "T2"], "trackartist": [1, 1]}
        ),
    )

    report = write_db(db_path, ["artist", "track"], paths=paths)

    assert report.row_counts == {"artist": 2, "track": 2}
    assert report.errors == []
    assert db_path.exists()
    assert _row_count(target, db_path, "artist") == 2
    assert _row_count(target, db_path, "track") == 2


def test_write_pudl_db_replaces_existing_file(
    write_db,
    target: _DatabaseTarget,
    db_path: Path,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """A stale file at the destination is replaced, not appended to."""
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["A"]})
    )
    db_path.write_text("not a real database file")

    report = write_db(db_path, ["artist"], paths=paths)

    assert report.errors == []
    assert _row_count(target, db_path, "artist") == 1


def test_write_pudl_db_continues_past_failing_table(
    write_db,
    target: _DatabaseTarget,
    db_path: Path,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """A data-quality failure in one table is recorded; the others still load."""
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    # "artist" violates NOT NULL on artistname (enforced by both backends); "track" is
    # valid.
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": [None]})
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame({"trackid": [1], "trackname": ["T1"], "trackartist": [1]}),
    )

    report = write_db(db_path, ["artist", "track"], paths=paths)

    assert report.row_counts == {"track": 1}
    assert report.failed_tables == ["artist"]
    assert len(report.errors) == 1
    assert isinstance(report.errors[0].exception, duckdb.Error)
    assert _row_count(target, db_path, "track") == 1
    assert _row_count(target, db_path, "artist") == 0
