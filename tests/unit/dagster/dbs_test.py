"""Test the pudl_sqlite/pudl_duckdb assets that rebuild databases from Parquet."""

from pathlib import Path

import duckdb
import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.dagster.assets.output.dbs import (
    _SQLITE_ATTACH_ALIAS,
    TableWriteError,
    TableWriteReport,
    _copy_table_to_duckdb,
    _copy_table_to_sqlite,
    _has_integer_rowid_alias_pk,
    _validate_primary_key,
    write_pudl_duckdb,
    write_pudl_sqlite,
)
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths

# duckdb-engine's SQLAlchemy dialect inherits Postgres' 63-character identifier
# length limit, which DuckDB itself doesn't actually have -- see the matching
# constant/comment in dbs.py. Applied to every duckdb SQLAlchemy engine below.
_DUCKDB_MAX_IDENTIFIER_LENGTH = 255


@pytest.fixture
def test_pkg() -> Package:
    """Create a test metadata package for the pudl_sqlite/pudl_duckdb asset tests."""
    fields = [
        {"name": "artistid", "type": "integer", "description": "artistid"},
        {
            "name": "artistname",
            "type": "string",
            "constraints": {"required": True, "pattern": "^[A-Za-z ]+$"},
            "description": "artistname",
        },
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    artist_resource = Resource(name="artist", schema=schema, description="Artist")

    fields = [
        {"name": "trackid", "type": "integer", "description": "trackid"},
        {
            "name": "trackname",
            "type": "string",
            "constraints": {"required": True},
            "description": "trackname",
        },
        {"name": "trackartist", "type": "integer", "description": "trackartist"},
    ]
    fkeys = [
        {
            "fields": ["trackartist"],
            "reference": {"resource": "artist", "fields": ["artistid"]},
        }
    ]
    schema = {"fields": fields, "primary_key": ["trackid"], "foreign_keys": fkeys}
    track_resource = Resource(name="track", schema=schema, description="Track")

    fields = [
        {"name": "trackid", "type": "integer", "description": "trackid"},
        {"name": "labelid", "type": "integer", "description": "labelid"},
    ]
    schema = {"fields": fields, "primary_key": ["trackid", "labelid"]}
    track_label_resource = Resource(
        name="track_label", schema=schema, description="Track label (composite PK)"
    )

    fields = [
        {"name": "genrename", "type": "string", "description": "genrename"},
    ]
    schema = {"fields": fields, "primary_key": ["genrename"]}
    genre_resource = Resource(
        name="genre", schema=schema, description="Genre (non-integer PK)"
    )

    # Two resources sharing an enum-constrained field of the same name, to test that
    # the DuckDB dialect's native ENUM type gets created once and reused rather than
    # erroring on the second table.
    status_fields = [
        {"name": "id", "type": "integer", "description": "id"},
        {
            "name": "status",
            "type": "string",
            "constraints": {"required": True, "enum": ["active", "inactive"]},
            "description": "status",
        },
    ]
    status_schema = {"fields": status_fields, "primary_key": ["id"]}
    status_a_resource = Resource(
        name="status_a", schema=status_schema, description="Status A"
    )
    status_b_resource = Resource(
        name="status_b", schema=status_schema, description="Status B"
    )

    return Package(
        name="music",
        resources=[
            track_resource,
            artist_resource,
            track_label_resource,
            genre_resource,
            status_a_resource,
            status_b_resource,
        ],
    )


@pytest.fixture
def paths(tmp_path: Path) -> PudlPaths:
    """A PudlPaths pointing at a scratch tmp_path, with a parquet/ subdir."""
    (tmp_path / "parquet").mkdir()
    return PudlPaths(pudl_input=tmp_path / "input", pudl_output=tmp_path)


def _write_parquet(paths: PudlPaths, table_name: str, df: pd.DataFrame) -> None:
    df.to_parquet(paths.parquet_path(table_name))


def test_validate_primary_key_raises_on_null(paths: PudlPaths, test_pkg: Package):
    """A null primary key value should raise ValueError before any write happens."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1, None], "artistname": ["A", "B"]})
    )
    with pytest.raises(ValueError, match="artist"):
        _validate_primary_key(artist, paths)


def test_validate_primary_key_raises_on_duplicate(paths: PudlPaths, test_pkg: Package):
    """A duplicate primary key value should raise ValueError before any write happens."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1, 1], "artistname": ["A", "B"]})
    )
    with pytest.raises(ValueError, match="artist"):
        _validate_primary_key(artist, paths)


def test_validate_primary_key_passes_valid_data(paths: PudlPaths, test_pkg: Package):
    """Valid, unique, non-null primary keys should not raise."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1, 2], "artistname": ["A", "B"]})
    )
    _validate_primary_key(artist, paths)  # should not raise


def test_has_integer_rowid_alias_pk_single_integer_column(test_pkg: Package):
    """A single-column integer primary key is susceptible to the ROWID alias quirk."""
    assert _has_integer_rowid_alias_pk(test_pkg.get_resource("artist"))


def test_has_integer_rowid_alias_pk_false_for_composite_key(test_pkg: Package):
    """A composite primary key is not a ROWID alias, even if all-integer."""
    assert not _has_integer_rowid_alias_pk(test_pkg.get_resource("track_label"))


def test_has_integer_rowid_alias_pk_false_for_non_integer_key(test_pkg: Package):
    """A single-column non-integer primary key is not a ROWID alias."""
    assert not _has_integer_rowid_alias_pk(test_pkg.get_resource("genre"))


def test_validate_primary_key_skips_composite_key(paths: PudlPaths, test_pkg: Package):
    """Composite primary keys should be skipped entirely, not just pass.

    No Parquet file is written for "track_label" here -- if the skip logic weren't
    working, scanning a nonexistent file would raise, not silently pass.
    """
    track_label = test_pkg.get_resource("track_label")
    _validate_primary_key(track_label, paths)  # should not raise or read any file


def test_validate_primary_key_skips_non_integer_key(
    paths: PudlPaths, test_pkg: Package
):
    """Non-integer single-column primary keys should be skipped entirely.

    No Parquet file is written for "genre" here -- if the skip logic weren't
    working, scanning a nonexistent file would raise, not silently pass.
    """
    genre = test_pkg.get_resource("genre")
    _validate_primary_key(genre, paths)  # should not raise or read any file


def test_table_write_error_str_includes_debugging_context():
    """The error string should identify the table, exception type, and message."""
    error = TableWriteError("artist", ValueError("bad primary key"))
    assert str(error) == "artist: ValueError: bad primary key"


def test_table_write_report_summary_reports_failures():
    """The summary should call out every failed table by name and reason."""
    report = TableWriteReport(
        row_counts={"track": 1},
        errors=[TableWriteError("artist", ValueError("bad primary key"))],
    )
    summary = report.summary()
    assert "1/2" in summary
    assert "artist: ValueError: bad primary key" in summary


def test_table_write_report_summary_reports_full_success():
    """The summary should say so plainly when nothing failed."""
    report = TableWriteReport(row_counts={"artist": 1, "track": 1}, errors=[])
    assert report.summary() == "Wrote all 2 table(s)."


################################################################################
# SQLite
################################################################################


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    return tmp_path / "test.sqlite"


@pytest.fixture
def sqlite_duckdb_con(sqlite_path: Path, test_pkg: Package):
    """A DuckDB connection with a freshly-created "lean" SQLite schema attached.

    Mirrors what :func:`write_pudl_sqlite` sets up: the schema is built without
    CHECK constraints (but keeps primary keys, NOT NULL, UNIQUE and foreign
    keys), and the SQLite file is attached to DuckDB under
    ``_SQLITE_ATTACH_ALIAS`` so ``_copy_table_to_sqlite`` can stream into it.
    """
    metadata = test_pkg.to_sql(
        dialect="sqlite",
        check_types=False,
        check_values=False,
    )
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    metadata.create_all(engine)
    engine.dispose()

    con = duckdb.connect()
    con.execute("LOAD sqlite")
    con.execute(f"ATTACH '{sqlite_path}' AS {_SQLITE_ATTACH_ALIAS} (TYPE sqlite)")
    try:
        yield con
    finally:
        con.close()


@pytest.fixture
def duckdb_con():
    con = duckdb.connect()
    try:
        yield con
    finally:
        con.close()


def test_copy_table_to_sqlite_not_null_violation(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_duckdb_con: duckdb.DuckDBPyConnection,
):
    """A NOT NULL violation should surface from SQLite as a duckdb.Error."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": [None]})
    )
    with pytest.raises(duckdb.Error, match="NOT NULL"):
        _copy_table_to_sqlite(sqlite_duckdb_con, artist, paths.parquet_path("artist"))


def test_copy_table_to_sqlite_duplicate_primary_key(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_duckdb_con: duckdb.DuckDBPyConnection,
):
    """A duplicate primary key should surface from SQLite as a duckdb.Error.

    Unlike the dropped CHECK constraints, PRIMARY KEY / NOT NULL / UNIQUE are kept
    in the lean schema and still enforced by SQLite as DuckDB streams rows in.
    """
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1, 1], "artistname": ["A", "B"]})
    )
    with pytest.raises(duckdb.Error, match="UNIQUE|PRIMARY KEY"):
        _copy_table_to_sqlite(sqlite_duckdb_con, artist, paths.parquet_path("artist"))


def test_copy_table_to_sqlite_no_check_constraint_enforced(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_duckdb_con: duckdb.DuckDBPyConnection,
):
    """CHECK constraints are intentionally *not* enforced on write.

    "artist".artistname has a ``^[A-Za-z ]+$`` pattern in the fixture package.
    The old sqlite3-based writer rejected "123"; the lean-schema DuckDB writer
    drops the CHECK entirely, trusting the upstream validation, and writes it.
    """
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["123"]})
    )
    row_count = _copy_table_to_sqlite(
        sqlite_duckdb_con, artist, paths.parquet_path("artist")
    )
    assert row_count == 1


def test_copy_table_to_sqlite_foreign_keys_declared_not_enforced(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_path: Path,
    sqlite_duckdb_con: duckdb.DuckDBPyConnection,
):
    """Foreign keys are in the schema but not checked on write.

    "track".trackartist references "artist".artistid in the fixture package. The
    lean schema keeps that FK, but SQLite doesn't enforce foreign keys unless
    ``PRAGMA foreign_keys = ON`` (it defaults OFF and DuckDB never sets it), so a
    row pointing at a non-existent artist still writes -- while the FK definition
    stays visible for downstream users.
    """
    track = test_pkg.get_resource("track")
    _write_parquet(
        paths,
        "track",
        pd.DataFrame({"trackid": [1], "trackname": ["T1"], "trackartist": [999]}),
    )
    row_count = _copy_table_to_sqlite(
        sqlite_duckdb_con, track, paths.parquet_path("track")
    )
    assert row_count == 1

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    assert sa.inspect(engine).get_foreign_keys("track")  # FK still declared
    engine.dispose()


def test_copy_table_to_sqlite_missing_column(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_duckdb_con: duckdb.DuckDBPyConnection,
):
    """A Parquet file missing a declared column should raise a DuckDB binder error."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(paths, "artist", pd.DataFrame({"artistid": [1]}))
    with pytest.raises(duckdb.BinderException, match="artistname"):
        _copy_table_to_sqlite(sqlite_duckdb_con, artist, paths.parquet_path("artist"))


def test_copy_table_to_sqlite_returns_row_count(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_duckdb_con: duckdb.DuckDBPyConnection,
):
    """The row count written to the attached SQLite table is returned."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths,
        "artist",
        pd.DataFrame({"artistid": [1, 2, 3], "artistname": ["A", "B", "C"]}),
    )
    row_count = _copy_table_to_sqlite(
        sqlite_duckdb_con, artist, paths.parquet_path("artist")
    )
    assert row_count == 3


def test_copy_table_to_sqlite_whole_second_datetime(
    tmp_path: Path,
    paths: PudlPaths,
):
    """Whole-second datetimes round-trip through the DuckDB -> SQLite write.

    Regression coverage for the old sqlite3 datetime adapter is no longer
    relevant (DuckDB does the write now), but a datetime whose ``microsecond``
    is 0 should still land in SQLite and read back as the same instant.
    """
    fields = [
        {"name": "eventid", "type": "integer", "description": "eventid"},
        {"name": "eventtime", "type": "datetime", "description": "eventtime"},
    ]
    schema = {"fields": fields, "primary_key": ["eventid"]}
    event_resource = Resource(name="event", schema=schema, description="Event")
    pkg = Package(name="events", resources=[event_resource])

    sqlite_path = tmp_path / "event.sqlite"
    metadata = pkg.to_sql(
        dialect="sqlite",
        check_types=False,
        check_values=False,
    )
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    metadata.create_all(engine)
    engine.dispose()

    _write_parquet(
        paths,
        "event",
        pd.DataFrame(
            {
                "eventid": [1],
                "eventtime": pd.to_datetime(["2020-01-01 00:00:00"]).astype(
                    "datetime64[us]"
                ),
            }
        ),
    )
    con = duckdb.connect()
    con.execute("LOAD sqlite")
    con.execute(f"ATTACH '{sqlite_path}' AS {_SQLITE_ATTACH_ALIAS} (TYPE sqlite)")
    try:
        row_count = _copy_table_to_sqlite(
            con, event_resource, paths.parquet_path("event")
        )
        stored = con.execute(
            f'SELECT eventtime::TIMESTAMP FROM {_SQLITE_ATTACH_ALIAS}."event"'  # noqa: S608
        ).fetchone()[0]
    finally:
        con.close()
    assert row_count == 1
    assert str(stored).startswith("2020-01-01 00:00:00")


def test_write_pudl_sqlite_end_to_end(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """write_pudl_sqlite should build a fresh SQLite DB matching the Parquet inputs."""
    mocker.patch("pudl.dagster.assets.output.dbs.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "artist",
        pd.DataFrame({"artistid": [1, 2], "artistname": ["A", "B"]}),
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame(
            {
                "trackid": [1, 2],
                "trackname": ["T1", "T2"],
                "trackartist": [1, 1],
            }
        ),
    )

    sqlite_path = tmp_path / "pudl.sqlite"
    report = write_pudl_sqlite(sqlite_path, ["artist", "track"], paths=paths)

    assert report.row_counts == {"artist": 2, "track": 2}
    assert report.errors == []
    assert sqlite_path.exists()

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    insp = sa.inspect(engine)
    # Schema creation covers every resource in the package; only the tables passed
    # in table_names actually get data written to them.
    assert {"artist", "track"} <= set(insp.get_table_names())
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT COUNT(*) FROM artist")).scalar_one() == 2
        assert conn.execute(sa.text("SELECT COUNT(*) FROM track")).scalar_one() == 2
    engine.dispose()


def test_write_pudl_sqlite_removes_existing_file(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """write_pudl_sqlite should start from a fresh file, not append to a stale one."""
    mocker.patch("pudl.dagster.assets.output.dbs.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["A"]})
    )
    _write_parquet(
        paths, "track", pd.DataFrame(columns=["trackid", "trackname", "trackartist"])
    )

    sqlite_path = tmp_path / "pudl.sqlite"
    sqlite_path.write_text("not a real sqlite file")

    write_pudl_sqlite(sqlite_path, ["artist", "track"], paths=paths)

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT COUNT(*) FROM artist")).scalar_one() == 1
    engine.dispose()


def test_write_pudl_sqlite_continues_past_failing_table(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """A data-quality failure in one table shouldn't stop the rest from loading."""
    mocker.patch("pudl.dagster.assets.output.dbs.PUDL_PACKAGE", test_pkg)
    # "artist" violates its NOT NULL constraint on artistname; "track" is valid.
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": [None]})
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame({"trackid": [1], "trackname": ["T1"], "trackartist": [1]}),
    )

    sqlite_path = tmp_path / "pudl.sqlite"
    report = write_pudl_sqlite(sqlite_path, ["artist", "track"], paths=paths)

    # The successful table still gets loaded...
    assert report.row_counts == {"track": 1}
    # ...and the failure is recorded, rather than raised or silently dropped.
    assert report.failed_tables == ["artist"]
    assert len(report.errors) == 1
    error = report.errors[0]
    assert error.table_name == "artist"
    assert isinstance(error.exception, duckdb.Error)

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT COUNT(*) FROM track")).scalar_one() == 1
        assert conn.execute(sa.text("SELECT COUNT(*) FROM artist")).scalar_one() == 0
    engine.dispose()


################################################################################
# DuckDB
################################################################################


@pytest.fixture
def duckdb_path(tmp_path: Path) -> Path:
    return tmp_path / "test.duckdb"


@pytest.fixture
def duckdb_sa_engine(duckdb_path: Path, test_pkg: Package):
    """A SQLAlchemy engine with a freshly-created, FK-free DuckDB schema."""
    metadata = test_pkg.to_sql(dialect="duckdb", include_foreign_keys=False)
    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
    metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def duckdb_sa_conn(duckdb_sa_engine: sa.Engine):
    with duckdb_sa_engine.connect() as conn:
        yield conn


def test_write_pudl_duckdb_schema_excludes_foreign_keys(test_pkg: Package):
    """The DuckDB schema should have no foreign key constraints at all.

    "track" declares a foreign key to "artist" in the fixture package -- confirms
    write_pudl_duckdb's include_foreign_keys=False actually takes effect, unlike the
    sqlite schema (built without that flag), which does have the FK.
    """
    duckdb_metadata = test_pkg.to_sql(dialect="duckdb", include_foreign_keys=False)
    assert list(duckdb_metadata.tables["track"].foreign_keys) == []

    sqlite_metadata = test_pkg.to_sql()
    assert list(sqlite_metadata.tables["track"].foreign_keys) != []


def test_duckdb_schema_shares_enum_type_across_tables(test_pkg: Package):
    """A named ENUM type shared by two tables should be created exactly once.

    Regression test: the existing (unrelated) Field.to_duckdb_dtype() helper does
    this via ad hoc per-call `CREATE TYPE`, which errors the second time the same
    field name is reused across tables. Going through a real SQLAlchemy engine +
    metadata.create_all() avoids that -- confirmed here against "status_a" and
    "status_b", which both declare a "status" enum field with identical values.
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


def test_copy_table_to_duckdb_not_null_violation(
    paths: PudlPaths, test_pkg: Package, duckdb_sa_conn: sa.Connection
):
    """A NOT NULL constraint violation should raise sqlalchemy.exc.DBAPIError."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": [None]})
    )
    with pytest.raises(sa.exc.DBAPIError, match="NOT NULL"):
        _copy_table_to_duckdb(duckdb_sa_conn, artist, paths.parquet_path("artist"))


def test_copy_table_to_duckdb_check_violation(
    paths: PudlPaths, test_pkg: Package, duckdb_sa_conn: sa.Connection
):
    """A pattern CHECK constraint violation should raise sqlalchemy.exc.DBAPIError.

    Regression test: DuckDB has no bare REGEXP keyword (unlike SQLite), so
    Field._to_sql_duckdb must emit regexp_full_match(...) instead -- if it emitted
    plain REGEXP, this CHECK constraint could never even be created, let alone
    enforced.
    """
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["123"]})
    )
    with pytest.raises(sa.exc.DBAPIError, match="CHECK constraint failed"):
        _copy_table_to_duckdb(duckdb_sa_conn, artist, paths.parquet_path("artist"))


def test_copy_table_to_duckdb_missing_column(
    paths: PudlPaths, test_pkg: Package, duckdb_sa_conn: sa.Connection
):
    """A Parquet file missing a declared column should raise a DuckDB binder error."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(paths, "artist", pd.DataFrame({"artistid": [1]}))
    with pytest.raises(sa.exc.DBAPIError, match="artistname"):
        _copy_table_to_duckdb(duckdb_sa_conn, artist, paths.parquet_path("artist"))


def test_copy_table_to_duckdb_returns_row_count(
    paths: PudlPaths, test_pkg: Package, duckdb_sa_conn: sa.Connection
):
    """The row count DuckDB's own INSERT reports should be returned directly."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths,
        "artist",
        pd.DataFrame({"artistid": [1, 2, 3], "artistname": ["A", "B", "C"]}),
    )
    row_count = _copy_table_to_duckdb(
        duckdb_sa_conn, artist, paths.parquet_path("artist")
    )
    assert row_count == 3


def test_write_pudl_duckdb_end_to_end(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """write_pudl_duckdb should build a fresh DuckDB DB matching the Parquet inputs."""
    mocker.patch("pudl.dagster.assets.output.dbs.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "artist",
        pd.DataFrame({"artistid": [1, 2], "artistname": ["A", "B"]}),
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame(
            {
                "trackid": [1, 2],
                "trackname": ["T1", "T2"],
                "trackartist": [1, 1],
            }
        ),
    )

    duckdb_path = tmp_path / "pudl.duckdb"
    report = write_pudl_duckdb(duckdb_path, ["artist", "track"], paths=paths)

    assert report.row_counts == {"artist": 2, "track": 2}
    assert report.errors == []
    assert duckdb_path.exists()

    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM artist").scalar_one() == 2
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM track").scalar_one() == 2
        # No foreign key constraint should exist between track and artist.
        insp = sa.inspect(engine)
        assert insp.get_foreign_keys("track") == []
    engine.dispose()


def test_write_pudl_duckdb_removes_existing_file(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """write_pudl_duckdb should start from a fresh file, not append to a stale one."""
    mocker.patch("pudl.dagster.assets.output.dbs.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["A"]})
    )
    _write_parquet(
        paths, "track", pd.DataFrame(columns=["trackid", "trackname", "trackartist"])
    )

    duckdb_path = tmp_path / "pudl.duckdb"
    duckdb_path.write_text("not a real duckdb file")

    write_pudl_duckdb(duckdb_path, ["artist", "track"], paths=paths)

    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM artist").scalar_one() == 1
    engine.dispose()


def test_write_pudl_duckdb_continues_past_failing_table(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """A data-quality failure in one table shouldn't stop the rest from loading."""
    mocker.patch("pudl.dagster.assets.output.dbs.PUDL_PACKAGE", test_pkg)
    # "artist" violates its own CHECK constraint; "track" is perfectly valid.
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["123"]})
    )
    _write_parquet(
        paths,
        "track",
        pd.DataFrame({"trackid": [1], "trackname": ["T1"], "trackartist": [1]}),
    )

    duckdb_path = tmp_path / "pudl.duckdb"
    report = write_pudl_duckdb(duckdb_path, ["artist", "track"], paths=paths)

    # The successful table still gets loaded...
    assert report.row_counts == {"track": 1}
    # ...and the failure is recorded, rather than raised or silently dropped.
    assert report.failed_tables == ["artist"]
    assert len(report.errors) == 1
    error = report.errors[0]
    assert error.table_name == "artist"
    assert isinstance(error.exception, sa.exc.DBAPIError)

    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM track").scalar_one() == 1
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM artist").scalar_one() == 0
    engine.dispose()
