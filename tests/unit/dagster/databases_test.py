"""Test the pudl_sqlite/pudl_duckdb assets that rebuild databases from Parquet."""

from functools import partial
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.dagster.assets.output.databases import (
    _DUCKDB_MAX_IDENTIFIER_LENGTH,
    _SQLITE_ATTACH_ALIAS,
    DUCKDB_TARGET,
    SQLITE_TARGET,
    TableWriteError,
    TableWriteReport,
    _copy_table,
    _DatabaseTarget,
    _has_integer_rowid_alias_pk,
    _validate_primary_key,
    _write_pudl_db,
)
from pudl.metadata.classes import Package, Resource
from pudl.workspace.setup import PudlPaths


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

    resource.check_primary_key()'s own detection logic -- null vs. duplicate values,
    chunking, the polars/pandas backends -- is already covered by
    tests/unit/metadata/metadata_test.py, so it isn't re-tested here. This only
    confirms that _validate_primary_key runs that check for a ROWID-alias primary
    key and re-raises whatever it finds as a ValueError naming the resource, which is
    the part of the wrapper that's actually ours.
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


################################################################################
# Per-table failure reporting
################################################################################


def test_table_write_error_str_includes_debugging_context():
    """The error string identifies the table, exception type, and message."""
    error = TableWriteError("utility", ValueError("bad primary key"))
    assert str(error) == "utility: ValueError: bad primary key"


@pytest.mark.parametrize(
    ("report", "expected_substrings"),
    [
        (
            TableWriteReport(
                row_counts={"plant": 1},
                errors=[TableWriteError("utility", ValueError("bad primary key"))],
            ),
            ["1/2", "utility: ValueError: bad primary key"],
        ),
        (
            TableWriteReport(row_counts={"utility": 1, "plant": 1}, errors=[]),
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
def write_db(target: _DatabaseTarget):
    """``_write_pudl_db`` bound to the parametrized target."""
    return partial(_write_pudl_db, target)


@pytest.fixture
def db_path(backend: str, tmp_path: Path) -> Path:
    return tmp_path / f"pudl.{backend}"


@pytest.fixture
def db_conn(target: _DatabaseTarget, db_path: Path, test_pkg: Package, mocker):
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

    conn = duckdb.connect() if target.attach_as_sqlite else duckdb.connect(str(db_path))
    if target.attach_as_sqlite:
        conn.execute("LOAD sqlite")
        conn.execute(f"ATTACH '{db_path}' AS {_SQLITE_ATTACH_ALIAS} (TYPE sqlite)")
    try:
        yield conn
    finally:
        conn.close()


def test_copy_table_returns_row_count(
    db_conn: duckdb.DuckDBPyConnection,
    target: _DatabaseTarget,
    paths: PudlPaths,
    test_pkg: Package,
):
    """The number of rows in the destination table after the insert is returned."""
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame(
            {"utility_id_eia": [1, 2, 3], "utility_name_eia": ["A", "B", "C"]}
        ),
    )
    row_count = _copy_table(
        db_conn,
        test_pkg.get_resource("utility"),
        paths.parquet_path("utility"),
        table_ref=target.table_ref("utility"),
    )
    assert row_count == 3


def test_copy_table_reports_constraint_violation_as_duckdb_error(
    db_conn: duckdb.DuckDBPyConnection,
    target: _DatabaseTarget,
    paths: PudlPaths,
    test_pkg: Package,
):
    """A NOT NULL violation surfaces as duckdb.Error -- what _WRITE_EXCEPTIONS catches."""
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": [None]}),
    )
    with pytest.raises(duckdb.Error, match="NOT NULL"):
        _copy_table(
            db_conn,
            test_pkg.get_resource("utility"),
            paths.parquet_path("utility"),
            table_ref=target.table_ref("utility"),
        )


def test_copy_table_missing_column_raises_binder_error(
    db_conn: duckdb.DuckDBPyConnection,
    target: _DatabaseTarget,
    paths: PudlPaths,
    test_pkg: Package,
):
    """A Parquet file missing a declared column raises a DuckDB binder error.

    ``duckdb.BinderException`` is a ``duckdb.Error`` subclass, so a schema mismatch
    is caught per-table by ``_write_pudl_db`` rather than aborting the run.
    """
    _write_parquet(paths, "utility", pd.DataFrame({"utility_id_eia": [1]}))
    with pytest.raises(duckdb.BinderException, match="utility_name_eia"):
        _copy_table(
            db_conn,
            test_pkg.get_resource("utility"),
            paths.parquet_path("utility"),
            table_ref=target.table_ref("utility"),
        )


################################################################################
# Schema shape -- deliberate SQLite / DuckDB design choices
################################################################################


def test_sqlite_schema_omits_check_constraints(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """CHECK constraints are dropped from the SQLite schema (data is validated upstream).

    "utility".utility_name_eia has a ``^[A-Za-z ]+$`` pattern; "123" violates it but
    must still write, since the lean SQLite schema omits CHECK for insert speed.
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": ["123"]}),
    )
    report = _write_pudl_db(
        SQLITE_TARGET, tmp_path / "pudl.sqlite", ["utility"], paths=paths
    )
    assert report.row_counts == {"utility": 1}
    assert report.errors == []


def test_sqlite_foreign_keys_declared_but_not_enforced(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """The SQLite schema keeps FK definitions, but SQLite doesn't enforce them on write.

    SQLite only checks foreign keys under ``PRAGMA foreign_keys = ON`` (it defaults
    OFF and DuckDB never sets it), so a plant pointing at a non-existent utility
    still writes -- while the FK definition stays visible for downstream users.
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": ["A"]}),
    )
    _write_parquet(
        paths,
        "plant",
        pd.DataFrame(
            {
                "plant_id_eia": [1],
                "plant_name_eia": ["Plant A"],
                "utility_id_eia": [999],  # no such utility
            }
        ),
    )
    sqlite_path = tmp_path / "pudl.sqlite"
    report = _write_pudl_db(
        SQLITE_TARGET, sqlite_path, ["utility", "plant"], paths=paths
    )
    assert report.row_counts == {"utility": 1, "plant": 1}  # dangling FK still written

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    assert sa.inspect(engine).get_foreign_keys("plant")  # ...but FK still declared
    engine.dispose()


def test_duckdb_foreign_keys_declared_and_enforced(
    tmp_path: Path, paths: PudlPaths, test_pkg: Package, mocker
):
    """The DuckDB schema declares FK constraints and enforces them on write.

    Unlike SQLite (see test_sqlite_foreign_keys_declared_but_not_enforced above),
    DuckDB validates foreign keys against the referenced table as rows land, so a
    plant pointing at a non-existent utility fails instead of writing.
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": ["A"]}),
    )
    _write_parquet(
        paths,
        "plant",
        pd.DataFrame(
            {
                "plant_id_eia": [1],
                "plant_name_eia": ["Plant A"],
                "utility_id_eia": [999],  # no such utility
            }
        ),
    )
    duckdb_path = tmp_path / "pudl.duckdb"
    report = _write_pudl_db(
        DUCKDB_TARGET, duckdb_path, ["utility", "plant"], paths=paths
    )
    assert report.row_counts == {"utility": 1}
    assert report.failed_tables == ["plant"]
    assert isinstance(report.errors[0].exception, duckdb.Error)

    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
    assert sa.inspect(engine).get_foreign_keys("plant")  # FK declared even though
    # the row that would have violated it never landed
    engine.dispose()


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
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
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

    report = write_db(db_path, ["utility", "plant"], paths=paths)

    assert report.row_counts == {"utility": 2, "plant": 2}
    assert report.errors == []
    assert db_path.exists()
    assert _row_count(target, db_path, "utility") == 2
    assert _row_count(target, db_path, "plant") == 2


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
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": ["A"]}),
    )
    db_path.write_text("not a real database file")

    report = write_db(db_path, ["utility"], paths=paths)

    assert report.errors == []
    assert _row_count(target, db_path, "utility") == 1


def test_write_pudl_db_continues_past_failing_table(
    write_db,
    target: _DatabaseTarget,
    db_path: Path,
    paths: PudlPaths,
    test_pkg: Package,
    mocker,
):
    """A data-quality failure in one table is recorded; the others still load.

    "fuel_type" (not "plant") stands in for the succeeding table here specifically
    because it has no foreign key to "utility": under DuckDB, a child row pointing at
    a parent row that failed to write would itself fail on the FK constraint, which
    would conflate this test's "independent tables" property with FK-enforcement
    behavior (covered separately below).
    """
    mocker.patch("pudl.dagster.assets.output.databases.PUDL_PACKAGE", test_pkg)
    # "utility" violates NOT NULL on utility_name_eia (enforced by both backends);
    # "fuel_type" is valid and unrelated to "utility".
    _write_parquet(
        paths,
        "utility",
        pd.DataFrame({"utility_id_eia": [1], "utility_name_eia": [None]}),
    )
    _write_parquet(
        paths,
        "fuel_type",
        pd.DataFrame({"fuel_type_code": ["NG"]}),
    )

    report = write_db(db_path, ["utility", "fuel_type"], paths=paths)

    assert report.row_counts == {"fuel_type": 1}
    assert report.failed_tables == ["utility"]
    assert len(report.errors) == 1
    assert isinstance(report.errors[0].exception, duckdb.Error)
    assert _row_count(target, db_path, "fuel_type") == 1
    assert _row_count(target, db_path, "utility") == 0
