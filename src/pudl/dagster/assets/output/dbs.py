"""Dagster assets that rebuild pudl.sqlite and pudl.duckdb from PUDL's Parquet outputs."""

import datetime
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from sqlite3 import sqlite_version

import dagster as dg
import duckdb
import sqlalchemy as sa
from packaging import version

import pudl.logging_helpers
from pudl.helpers import get_parquet_table_polars
from pudl.metadata.classes import PUDL_PACKAGE, Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

MINIMUM_SQLITE_VERSION = "3.32.0"

# Number of rows read from Parquet and inserted into SQLite per batch. Keeps memory
# bounded for large tables without a per-row round trip.
_COPY_BATCH_SIZE = 100_000

# Exceptions treated as data-quality problems with one table, rather than a bug in
# this module: caught per table in write_pudl_sqlite()/write_pudl_duckdb() so one bad
# table doesn't abort the hundreds of others. ValueError comes from
# _validate_primary_key(); sqlite3.Error covers CHECK/NOT NULL/UNIQUE/FOREIGN KEY
# constraint violations raised by the real SQLite write; duckdb.Error covers problems
# reading a table's Parquet file (e.g. a column that doesn't match the declared
# schema).
_SQLITE_WRITE_EXCEPTIONS = (ValueError, sqlite3.Error, duckdb.Error)

# duckdb-engine's SQLAlchemy dialect subclasses postgresql's, inheriting a
# 63-character identifier length limit that DuckDB itself doesn't actually have.
_DUCKDB_MAX_IDENTIFIER_LENGTH = 255


def _adapt_datetime(dt: datetime.datetime) -> str:
    """Serialize a datetime with an explicit, always-present microseconds suffix.

    Python's default (now-deprecated) ``sqlite3`` datetime adapter stringifies via
    ``str(dt)``, which omits the fractional-seconds suffix whenever
    ``microsecond == 0`` (e.g. ``"2020-01-01 00:00:00"`` instead of
    ``"2020-01-01 00:00:00.000000"``). That silently produces two different string
    formats for the same column depending on the value, and PUDL's ``datetime``
    ``CHECK`` constraint (see ``Field._to_sql_sqlite``) only accepts the
    microsecond-suffixed form -- so whole-second timestamps failed to insert.
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


sqlite3.register_adapter(datetime.datetime, _adapt_datetime)


@dataclass
class TableWriteError:
    """Everything needed to debug one table's failed write."""

    table_name: str
    exception: Exception

    def __str__(self) -> str:
        """Render as ``table_name: ExceptionType: message`` for logs and reports."""
        return f"{self.table_name}: {type(self.exception).__name__}: {self.exception}"


@dataclass
class TableWriteReport:
    """Outcome of rebuilding a database: what wrote successfully, and what didn't.

    Tables are written independently of each other in :func:`write_pudl_sqlite`/
    :func:`write_pudl_duckdb`, so a data-quality problem in one doesn't stop the rest
    from being attempted. This report is how the caller finds out, after the fact,
    exactly which tables failed and why -- across potentially hundreds of tables in
    one run -- rather than only ever learning about the first failure encountered.
    """

    row_counts: dict[str, int] = field(default_factory=dict)
    errors: list[TableWriteError] = field(default_factory=list)

    @property
    def failed_tables(self) -> list[str]:
        """Names of tables that failed to write."""
        return [error.table_name for error in self.errors]

    def summary(self) -> str:
        """Human-readable report of every table that failed to write, and why."""
        total = len(self.row_counts) + len(self.errors)
        if not self.errors:
            return f"Wrote all {total} table(s)."
        lines = [f"Wrote {len(self.row_counts)}/{total} table(s)."]
        lines.append(f"{len(self.errors)} table(s) failed:")
        lines.extend(f"  - {error}" for error in self.errors)
        return "\n".join(lines)


################################################################################
# SQLite
################################################################################


def _check_sqlite_version() -> None:
    if version.parse(sqlite_version) < version.parse(MINIMUM_SQLITE_VERSION):
        logger.warning(
            f"Found SQLite {sqlite_version}, less than the minimum required "
            f"version {MINIMUM_SQLITE_VERSION}."
        )


def _has_integer_rowid_alias_pk(resource: Resource) -> bool:
    """Return True if resource's primary key is susceptible to SQLite's ROWID alias.

    When a table's primary key is a *single* column declared with type ``INTEGER``,
    SQLite treats that column as an alias for its internal ``rowid`` rather than as
    an ordinary constrained column. This has a surprising consequence: inserting
    ``NULL`` into that column does not raise an error, even though the column also
    has an explicit ``NOT NULL`` constraint (every PUDL primary key column does --
    see ``Resource._check_primary_key_in_fields``). Instead, SQLite silently
    substitutes the next available rowid and the insert succeeds -- a bad row
    (``NULL`` where a primary key value was expected) is quietly laundered into a
    plausible-looking row instead of failing loudly.

    This only applies to a single-column ``integer``/``year`` primary key (``year``
    also maps to ``sa.BigInteger`` -- see ``FIELD_DTYPES_SQLITE``). Composite
    (multi-column) primary keys and non-integer single-column primary keys are not
    ROWID aliases, so SQLite's own ``NOT NULL``/``UNIQUE`` enforcement on those
    columns works exactly as expected and needs no help from PUDL.
    """
    pk = resource.schema.primary_key
    if len(pk) != 1:
        return False
    field = next(f for f in resource.schema.fields if f.name == pk[0])
    return field.type in ("integer", "year")


def _validate_primary_key(resource: Resource, paths: PudlPaths) -> None:
    """Raise ValueError if resource's table violates its own primary key.

    Only checked when :func:`_has_integer_rowid_alias_pk` is True, since that's the
    only primary key shape SQLite doesn't already enforce NOT NULL/UNIQUE on
    correctly. For every other shape, SQLite's own constraint enforcement at insert
    time is already sufficient, so scanning the Parquet file here would just be
    wasted effort. (DuckDB has no equivalent quirk for any primary key shape, so
    write_pudl_duckdb doesn't need an analogous check at all.)
    """
    if not _has_integer_rowid_alias_pk(resource):
        return
    lazy_df = get_parquet_table_polars(resource.name, paths=paths)
    if errors := resource.check_primary_key(lazy_df):
        raise ValueError(
            f"{resource.name}: " + "\n".join(str(error) for error in errors)
        )


def _copy_table_to_sqlite(
    sqlite_con: sqlite3.Connection,
    duckdb_con: duckdb.DuckDBPyConnection,
    resource: Resource,
    parquet_path: Path,
) -> int:
    """Delete and reload one table's data in the destination SQLite database.

    Returns the row count written. Rows are read from Parquet via DuckDB (fast
    columnar I/O) but written through Python's ``sqlite3`` module rather than
    DuckDB's own ``ATTACH ... TYPE sqlite``: DuckDB's statically-linked SQLite build
    has no ``REGEXP()`` function, while the system/conda-forge SQLite that Python's
    ``sqlite3`` links against does, and several PUDL ``CHECK`` constraints rely on
    ``REGEXP``. Constraints (``CHECK``/``NOT NULL``/``UNIQUE``/``FOREIGN KEY``)
    declared on the destination table are still enforced by SQLite itself at insert
    time, since this is real SQLite doing the write.
    """
    # Table/column names come from PUDL_PACKAGE resource metadata, not external
    # input, so this isn't user-controlled SQL injection despite the string
    # interpolation. The parquet path is the one real variable and is bound as a
    # query parameter below.
    table_name = resource.name
    columns = resource.get_field_names()
    columns_sql = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'  # noqa: S608

    reader = duckdb_con.execute(
        f"SELECT {columns_sql} FROM read_parquet(?)",  # noqa: S608
        [str(parquet_path)],
    ).to_arrow_reader(_COPY_BATCH_SIZE)

    row_count = 0
    with sqlite_con:
        sqlite_con.execute(f'DELETE FROM "{table_name}"')  # noqa: S608
        for batch in reader:
            rows = [tuple(row.values()) for row in batch.to_pylist()]
            sqlite_con.executemany(insert_sql, rows)
            row_count += len(rows)
    return row_count


def write_pudl_sqlite(
    sqlite_path: Path,
    table_names: Sequence[str],
    paths: PudlPaths | None = None,
) -> TableWriteReport:
    """Rebuild a fresh SQLite DB at ``sqlite_path`` from the given Parquet tables.

    Each table is validated and written independently: a data-quality failure in one
    table (see :data:`_SQLITE_WRITE_EXCEPTIONS`) is caught and recorded rather than
    aborting the run, so a single call surfaces every problem across potentially
    hundreds of tables at once instead of stopping at the first one. Returns a
    :class:`TableWriteReport` describing what wrote successfully and what didn't, for
    the caller to act on (e.g. fail the Dagster run) or inspect. Separated from the
    Dagster asset wrapper so it's directly unit-testable without constructing Dagster
    execution contexts.
    """
    _check_sqlite_version()
    paths = paths or PudlPaths()
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    sqlite_path.unlink(missing_ok=True)

    # Schema/constraints come from SQLAlchemy metadata -- this is the one part that
    # needs Package.to_sql()'s full constraint detail (PK/FK/CHECK).
    metadata = PUDL_PACKAGE.to_sql()  # already filtered to create_database_schema
    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    metadata.create_all(engine)
    engine.dispose()

    report = TableWriteReport()
    duckdb_con = duckdb.connect()
    sqlite_con = sqlite3.connect(sqlite_path)
    n_tables = len(table_names)
    try:
        for n, table_name in enumerate(table_names, start=1):
            logger.info(f"Writing {n}/{n_tables} {table_name} to SQLite.")
            resource = PUDL_PACKAGE.get_resource(table_name)
            try:
                _validate_primary_key(resource, paths)
                report.row_counts[table_name] = _copy_table_to_sqlite(
                    sqlite_con, duckdb_con, resource, paths.parquet_path(table_name)
                )
            except _SQLITE_WRITE_EXCEPTIONS as exc:
                logger.error(f"Failed to write {table_name} to SQLite: {exc}")
                report.errors.append(TableWriteError(table_name, exc))
    finally:
        sqlite_con.close()
        duckdb_con.close()

    return report


def build_pudl_sqlite_asset(
    sqlite_asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Return a Dagster asset that rebuilds pudl.sqlite from Parquet outputs.

    Args:
        sqlite_asset_keys: Keys of the Parquet-writing assets whose tables should be
            included in pudl.sqlite (i.e. Resource.create_database_schema is True).
    """

    @dg.asset(
        name="pudl_sqlite",
        group_name="out_pudl",
        deps=list(sqlite_asset_keys),
        required_resource_keys={"pudl_paths"},
        description=(
            "SQLite database rebuilt from PUDL's Parquet outputs after the ETL "
            "completes. Written to $PUDL_OUTPUT/pudl.sqlite. Includes only tables "
            "whose Resource has create_database_schema=True."
        ),
    )
    def pudl_sqlite(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        pudl_paths: PudlPaths = context.resources.pudl_paths
        sqlite_path = pudl_paths.sqlite_db_path("pudl")
        table_names = [key.path[-1] for key in sqlite_asset_keys]
        report = write_pudl_sqlite(sqlite_path, table_names, paths=pudl_paths)

        metadata: dict[str, dg.MetadataValue] = {
            "path": dg.MetadataValue.path(sqlite_path),
            "table_count": dg.MetadataValue.int(len(report.row_counts)),
            "bytes": dg.MetadataValue.int(sqlite_path.stat().st_size),
        }
        if report.errors:
            metadata["failed_tables"] = dg.MetadataValue.md(
                "\n".join(f"- {error}" for error in report.errors)
            )
            raise dg.Failure(description=report.summary(), metadata=metadata)

        return dg.MaterializeResult(metadata=metadata)

    return pudl_sqlite


################################################################################
# DuckDB
################################################################################


def _copy_table_to_duckdb(
    conn: sa.Connection,
    resource: Resource,
    parquet_path: Path,
) -> int:
    """Load one table's data into an already-created, empty DuckDB table.

    Returns the row count DuckDB's own INSERT reports directly. No DELETE step is
    needed (unlike SQLite): the destination file is always freshly created, so every
    table starts empty. Uses ``conn.exec_driver_sql()`` to send SQL straight to the
    duckdb-engine DBAPI cursor -- duckdb-engine's SQLAlchemy ``Connection`` wraps the
    native ``duckdb.DuckDBPyConnection`` in a way that doesn't cleanly expose it
    directly, so this is the most native-speed path available through the engine.
    """
    # See _copy_table_to_sqlite for why the string interpolation here isn't a SQL
    # injection risk: table/column names come from PUDL_PACKAGE resource metadata.
    table_name = resource.name
    columns_sql = ", ".join(f'"{c}"' for c in resource.get_field_names())
    result = conn.exec_driver_sql(
        f'INSERT INTO "{table_name}" SELECT {columns_sql} '  # noqa: S608
        "FROM read_parquet(?)",
        (str(parquet_path),),
    )
    row = result.fetchone()
    assert row is not None  # INSERT always returns exactly one "Count" row
    return row[0]


def write_pudl_duckdb(
    duckdb_path: Path,
    table_names: Sequence[str],
    paths: PudlPaths | None = None,
) -> TableWriteReport:
    """Rebuild a fresh DuckDB file from the given Parquet tables, FK-free.

    Schema creation goes through a real SQLAlchemy engine
    (``metadata.create_all()``), which is what lets ``ENUM``-constrained fields that
    share a name across many tables get a single native ``ENUM`` type created once
    and reused, rather than erroring on re-creation. Foreign keys are intentionally
    excluded (expensive at PUDL's full data volume); primary keys and ``NOT NULL``
    are kept, since both are cheap and natively enforced by DuckDB for every key
    shape -- unlike SQLite's ROWID-alias quirk, so (unlike ``write_pudl_sqlite``) no
    pre-write primary-key scan is needed here. Per-table failures are caught and
    collected rather than aborting the run, same as ``write_pudl_sqlite``.
    """
    paths = paths or PudlPaths()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.unlink(missing_ok=True)

    metadata = PUDL_PACKAGE.to_sql(dialect="duckdb", include_foreign_keys=False)
    engine = sa.create_engine(f"duckdb:///{duckdb_path}")
    # duckdb-engine's dialect subclasses SQLAlchemy's postgresql dialect, inheriting
    # Postgres' 63-character identifier length limit even though DuckDB itself has
    # no such restriction (confirmed directly against a native duckdb connection) --
    # several PUDL table names exceed 63 characters. _DUCKDB_MAX_IDENTIFIER_LENGTH
    # comfortably covers any realistic table/column name.
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
    metadata.create_all(engine)

    report = TableWriteReport()
    n_tables = len(table_names)
    try:
        with engine.connect() as conn:
            for n, table_name in enumerate(table_names, start=1):
                logger.info(f"Writing {n}/{n_tables} {table_name} to DuckDB.")
                resource = PUDL_PACKAGE.get_resource(table_name)
                try:
                    report.row_counts[table_name] = _copy_table_to_duckdb(
                        conn, resource, paths.parquet_path(table_name)
                    )
                    conn.commit()
                except sa.exc.DBAPIError as exc:
                    conn.rollback()
                    logger.error(f"Failed to write {table_name} to DuckDB: {exc}")
                    report.errors.append(TableWriteError(table_name, exc))
    finally:
        engine.dispose()

    return report


def build_pudl_duckdb_asset(
    duckdb_asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Return a Dagster asset that rebuilds pudl.duckdb from Parquet outputs.

    Args:
        duckdb_asset_keys: Keys of the Parquet-writing assets whose tables should be
            included in pudl.duckdb (i.e. Resource.create_database_schema is True).
    """

    @dg.asset(
        name="pudl_duckdb",
        group_name="out_pudl",
        deps=list(duckdb_asset_keys),
        required_resource_keys={"pudl_paths"},
        description=(
            "DuckDB database rebuilt from PUDL's Parquet outputs after the ETL "
            "completes. Written to $PUDL_OUTPUT/pudl.duckdb. Includes only tables "
            "whose Resource has create_database_schema=True. Foreign key "
            "constraints are excluded for performance."
        ),
    )
    def pudl_duckdb(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        pudl_paths: PudlPaths = context.resources.pudl_paths
        duckdb_path = pudl_paths.duckdb_db_path("pudl")
        table_names = [key.path[-1] for key in duckdb_asset_keys]
        report = write_pudl_duckdb(duckdb_path, table_names, paths=pudl_paths)

        metadata: dict[str, dg.MetadataValue] = {
            "path": dg.MetadataValue.path(duckdb_path),
            "table_count": dg.MetadataValue.int(len(report.row_counts)),
            "bytes": dg.MetadataValue.int(duckdb_path.stat().st_size),
        }
        if report.errors:
            metadata["failed_tables"] = dg.MetadataValue.md(
                "\n".join(f"- {error}" for error in report.errors)
            )
            raise dg.Failure(description=report.summary(), metadata=metadata)

        return dg.MaterializeResult(metadata=metadata)

    return pudl_duckdb


__all__ = [
    "TableWriteError",
    "TableWriteReport",
    "build_pudl_duckdb_asset",
    "build_pudl_sqlite_asset",
    "write_pudl_duckdb",
    "write_pudl_sqlite",
]
