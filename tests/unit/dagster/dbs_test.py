"""Test the pudl_sqlite asset that rebuilds pudl.sqlite from Parquet."""

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.dagster.assets.output.dbs import (
    TableWriteError,
    TableWriteReport,
    _copy_table_to_sqlite,
    _has_integer_rowid_alias_pk,
    _validate_primary_key,
    write_pudl_sqlite,
)
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths


@pytest.fixture
def test_pkg() -> Package:
    """Create a test metadata package for the pudl_sqlite asset tests."""
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

    return Package(
        name="music",
        resources=[
            track_resource,
            artist_resource,
            track_label_resource,
            genre_resource,
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


@pytest.fixture
def sqlite_path(tmp_path: Path) -> Path:
    return tmp_path / "test.sqlite"


@pytest.fixture
def sqlite_con(sqlite_path: Path, test_pkg: Package):
    """A sqlite3 connection to a freshly-created schema."""
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    test_pkg.to_sql().create_all(engine)
    engine.dispose()

    con = sqlite3.connect(sqlite_path)
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
    sqlite_con: sqlite3.Connection,
    duckdb_con: duckdb.DuckDBPyConnection,
):
    """A NOT NULL constraint violation should raise sqlite3.IntegrityError."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": [None]})
    )
    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL"):
        _copy_table_to_sqlite(
            sqlite_con, duckdb_con, artist, paths.parquet_path("artist")
        )


def test_copy_table_to_sqlite_check_violation(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_con: sqlite3.Connection,
    duckdb_con: duckdb.DuckDBPyConnection,
):
    """A REGEXP CHECK constraint violation should raise sqlite3.IntegrityError.

    Regression test: DuckDB's own statically-linked SQLite build (used by ``ATTACH
    ... TYPE sqlite``) has no REGEXP() function, unlike the system/conda-forge SQLite
    Python's sqlite3 module links against, so REGEXP-based CHECK constraints must be
    enforced by writing through sqlite3 rather than DuckDB's SQLite attachment.
    """
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["123"]})
    )
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        _copy_table_to_sqlite(
            sqlite_con, duckdb_con, artist, paths.parquet_path("artist")
        )


def test_copy_table_to_sqlite_missing_column(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_con: sqlite3.Connection,
    duckdb_con: duckdb.DuckDBPyConnection,
):
    """A Parquet file missing a declared column should raise a DuckDB binder error."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(paths, "artist", pd.DataFrame({"artistid": [1]}))
    with pytest.raises(duckdb.BinderException, match="artistname"):
        _copy_table_to_sqlite(
            sqlite_con, duckdb_con, artist, paths.parquet_path("artist")
        )


def test_copy_table_to_sqlite_replaces_not_duplicates(
    paths: PudlPaths,
    test_pkg: Package,
    sqlite_con: sqlite3.Connection,
    duckdb_con: duckdb.DuckDBPyConnection,
):
    """Writing the same table twice should replace, not duplicate, its rows."""
    artist = test_pkg.get_resource("artist")
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["A"]})
    )
    parquet_path = paths.parquet_path("artist")
    row_count = _copy_table_to_sqlite(sqlite_con, duckdb_con, artist, parquet_path)
    assert row_count == 1
    row_count = _copy_table_to_sqlite(sqlite_con, duckdb_con, artist, parquet_path)
    assert row_count == 1


def test_copy_table_to_sqlite_whole_second_datetime(
    tmp_path: Path,
    paths: PudlPaths,
    duckdb_con: duckdb.DuckDBPyConnection,
):
    """A datetime value with microsecond == 0 should satisfy the DATETIME CHECK.

    Regression test: Python's default (deprecated) sqlite3 datetime adapter
    stringifies via ``str(dt)``, which omits the fractional-seconds suffix
    whenever microsecond == 0 (e.g. "2020-01-01 00:00:00" instead of
    "2020-01-01 00:00:00.000000"). PUDL's datetime CHECK constraint only accepts
    the microsecond-suffixed form, so whole-second timestamps -- common for
    date-only concepts stored as "datetime" fields -- used to fail to insert.
    """
    fields = [
        {"name": "eventid", "type": "integer", "description": "eventid"},
        {"name": "eventtime", "type": "datetime", "description": "eventtime"},
    ]
    schema = {"fields": fields, "primary_key": ["eventid"]}
    event_resource = Resource(name="event", schema=schema, description="Event")
    pkg = Package(name="events", resources=[event_resource])

    sqlite_path = tmp_path / "event.sqlite"
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    pkg.to_sql().create_all(engine)
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
    con = sqlite3.connect(sqlite_path)
    try:
        row_count = _copy_table_to_sqlite(
            con, duckdb_con, event_resource, paths.parquet_path("event")
        )
    finally:
        con.close()
    assert row_count == 1


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
    # "artist" violates its own CHECK constraint; "track" is perfectly valid.
    _write_parquet(
        paths, "artist", pd.DataFrame({"artistid": [1], "artistname": ["123"]})
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
    assert isinstance(error.exception, sqlite3.IntegrityError)

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT COUNT(*) FROM track")).scalar_one() == 1
        assert conn.execute(sa.text("SELECT COUNT(*) FROM artist")).scalar_one() == 0
    engine.dispose()
