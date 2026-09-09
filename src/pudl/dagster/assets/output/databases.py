r"""Dagster assets that assemble pudl.sqlite and pudl.duckdb from the Parquet outputs.

Once the ETL has written every table to Parquet, :func:`build_pudl_sqlite_asset` and
:func:`build_pudl_duckdb_asset` each define a Dagster asset that rebuilds one all-in-one
relational database from those files. :func:`_write_pudl_sqlite` and
:func:`_write_pudl_duckdb` do the actual work. They are deliberately two separate
functions rather than one parametrized one: the formats differ in schema, connection
handling, and which constraints get enforced on write, and the ``pudl.sqlite`` path is
expected to be deleted wholesale once its deprecation period ends.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import dagster as dg
import duckdb
import sqlalchemy as sa

import pudl.logging_helpers
from pudl.helpers import get_parquet_table_polars
from pudl.metadata.classes import PUDL_PACKAGE, Resource
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)

_WRITE_EXCEPTIONS: tuple[type[Exception], ...] = (ValueError, duckdb.Error)
"""Exceptions treated as data-quality problems with an individual table.

These are caught per table in the writers below so one bad table doesn't abort hundreds
of others. ValueError comes from _validate_primary_key(); duckdb.Error covers both
problems reading a table's Parquet file (e.g. a column that doesn't match the declared
schema) and the NOT NULL / UNIQUE / PRIMARY KEY / CHECK violations the destination
raises back through DuckDB as the rows are streamed in.
"""

_DUCKDB_MAX_IDENTIFIER_LENGTH = 255
"""Explicit maximum length for DuckDB identifiers.

Required because duckdb-engine's SQLAlchemy dialect subclasses postgresql's, inheriting a
63-character identifier length limit that DuckDB itself doesn't actually have.
"""

SQLITE_DESCRIPTION = (
    "SQLite database assembled from PUDL's Parquet outputs after the ETL "
    "completes. Written to $PUDL_OUTPUT/pudl.sqlite. Includes only tables "
    "whose Resource has create_database_schema=True. CHECK constraints "
    "are omitted for performance (data is already validated against the "
    "full schema upstream); foreign keys are declared but, per SQLite's "
    "default, not enforced on write."
)

DUCKDB_DESCRIPTION = (
    "DuckDB database assembled from PUDL's Parquet outputs after the ETL "
    "completes. Written to $PUDL_OUTPUT/pudl.duckdb. Includes only tables "
    "whose Resource has create_database_schema=True. Foreign key "
    "constraints are excluded due to a handful of type conflicts."
)


@dataclass
class TableWriteErrorInfo:
    """A single table's failed write: which table, and the exception that stopped it."""

    table_name: str
    """Name of the table whose write failed."""
    exception: Exception
    """The exception raised while validating or writing the table."""

    def __str__(self) -> str:
        """Render as ``table_name: ExceptionType: message`` for logs and reports."""
        return f"{self.table_name}: {type(self.exception).__name__}: {self.exception}"


@dataclass
class TableWriteReport:
    """Outcome of building a database: where it landed, and any error reports."""

    db_path: Path
    """Path of the database file that was built."""
    row_counts: dict[str, int] = field(default_factory=dict)
    """Mapping of table name to number of rows written, per successfully written table."""
    errors: list[TableWriteErrorInfo] = field(default_factory=list)
    """One :class:`TableWriteErrorInfo` per failed table, in the order they failed."""

    @property
    def failed_tables(self) -> list[str]:
        """Names of the tables that failed to write.

        Returns:
            The ``table_name`` of every recorded error, in failure order.
        """
        return [error.table_name for error in self.errors]

    def summary(self) -> str:
        """Human-readable report of every table that failed to write, and why.

        Returns:
            A multi-line string: a single ``"Wrote all N table(s)."`` line when
            nothing failed, otherwise a header line plus one indented line per
            failed table.
        """
        total = len(self.row_counts) + len(self.errors)
        if not self.errors:
            return f"Wrote all {total} table(s)."
        lines = [f"Wrote {len(self.row_counts)}/{total} table(s)."]
        lines.append(f"{len(self.errors)} table(s) failed:")
        lines.extend(f"  - {error}" for error in self.errors)
        return "\n".join(lines)


def _copy_table(
    conn: duckdb.DuckDBPyConnection,
    resource: Resource,
    parquet_path: Path,
    *,
    table_ref: str,
) -> int:
    r"""Stream one table's Parquet data into ``table_ref`` via DuckDB.

    DuckDB reads the Parquet file with its columnar engine and writes the rows straight
    into ``table_ref`` -- either a table in the DuckDB file itself, or a table in an
    attached SQLite database. Column order comes from the resource metadata so the
    ``SELECT`` lines up with the destination schema regardless of the Parquet file's
    column order.

    ``PRIMARY KEY`` / ``NOT NULL`` / ``UNIQUE`` (and, for DuckDB, ``CHECK``) are
    enforced by the destination as the rows land. SQLite foreign keys are declared
    but, per SQLite's default (``PRAGMA foreign_keys = OFF``), not checked on write;
    the DuckDB schema has no foreign keys at all.

    Args:
        conn: An open DuckDB connection. For the SQLite destination the target
            database must already be attached.
        resource: Metadata Resource for the table being written; supplies the
            ordered column list.
        parquet_path: Path to the source Parquet file for this table.
        table_ref: Quoted SQL reference to the destination table.

    Returns:
        The number of rows in the destination table after the insert (which, for a
        freshly created table, is the number of rows written).

    Raises:
        duckdb.Error: If the Parquet file doesn't match the declared schema, or the
            destination rejects a row for a ``NOT NULL`` / ``UNIQUE`` / ``PRIMARY
            KEY`` / ``CHECK`` violation as it lands.
    """
    columns_sql = ", ".join(f'"{c}"' for c in resource.get_field_names())
    conn.execute(
        f"INSERT INTO {table_ref} "  # noqa: S608
        f"SELECT {columns_sql} FROM read_parquet(?)",
        [str(parquet_path)],
    )
    row = conn.execute(f"SELECT count(*) FROM {table_ref}").fetchone()  # noqa: S608
    assert row is not None  # SELECT count(*) always returns exactly one row
    return row[0]


################################################################################
# pudl.sqlite
#
# This whole section -- _has_integer_rowid_alias_pk, _validate_primary_key,
# _write_pudl_sqlite -- is expected to be deleted once the pudl.sqlite deprecation
# period ends. It has no callers outside build_pudl_sqlite_asset().
################################################################################


def _has_integer_rowid_alias_pk(resource: Resource) -> bool:
    """Return whether a resource's primary key is susceptible to SQLite's ROWID alias.

    When a table's primary key is a *single* column of ``integer`` (or ``year``) type,
    SQLite treats that column as an alias for its internal ``rowid`` rather than as an
    ordinary constrained column. This has a surprising consequence: inserting ``NULL``
    into that column does not raise an error, even though the column also has an
    explicit ``NOT NULL`` constraint (every PUDL primary key column does -- see
    ``Resource._check_primary_key_in_fields``). Instead, SQLite silently substitutes the
    next available rowid and the insert succeeds -- a bad row (``NULL`` where a primary
    key value was expected) is quietly laundered into a plausible-looking row instead of
    failing loudly.

    Composite (multi-column) primary keys and non-integer single-column primary keys are
    not ROWID aliases, so SQLite's own ``NOT NULL``/``UNIQUE`` enforcement on those
    columns works as expected.

    Args:
        resource: Metadata Resource for the table, whose ``schema.primary_key`` and
            field types are inspected.

    Returns:
        True if the resource has a single-column ``integer`` (or ``year``) primary key.
        False for composite or non-integer primary keys.
    """
    pk = resource.schema.primary_key
    if len(pk) != 1:
        return False
    pk_field = next(f for f in resource.schema.fields if f.name == pk[0])
    return pk_field.type in ("integer", "year")


def _validate_primary_key(resource: Resource, paths: PudlPaths) -> None:
    """Check a table's Parquet data against its own primary key before writing.

    Only active when :func:`_has_integer_rowid_alias_pk` is True, since that is the only
    kind of primary key SQLite doesn't already enforce ``NOT NULL`` / ``UNIQUE`` on
    correctly. For all other cases SQLite's own constraint enforcement at insert time is
    sufficient, and this returns immediately without reading the Parquet file.

    Args:
        resource: Metadata Resource for the table to check.
        paths: PUDL workspace paths, used to locate the table's Parquet file.

    Raises:
        ValueError: If the Parquet data contains a null or duplicate primary key
            value. The message names the resource and lists each violation.
    """
    if not _has_integer_rowid_alias_pk(resource):
        return
    lazy_df = get_parquet_table_polars(resource.name, paths=paths)
    if errors := resource.check_primary_key(lazy_df):
        raise ValueError(
            f"{resource.name}: " + "\n".join(str(error) for error in errors)
        )


def _write_pudl_sqlite(
    table_names: Sequence[str],
    paths: PudlPaths,
) -> TableWriteReport:
    r"""Assemble ``$PUDL_OUTPUT/pudl.sqlite`` from the Parquet outputs.

    Lays down an empty schema through a throwaway SQLAlchemy engine, attaches to the
    empty database with DuckDB, reads each table's Parquet file, and streams it into the
    SQLite database through DuckDB's interface. This is ~20x faster than writing through
    Python's ``sqlite3``.

    The SQLite schema drops the CHECK and type constraints our metadata defines because
    they would be enforced by SQLite here, and it is slow. The data is already validated
    elsewhere by pandera and dbt. Foreign key constraints are declared but not enforced,
    per SQLite's ``PRAGMA foreign_keys = OFF`` default.

    Args:
        table_names: Tables to load, in the order they should be inserted.
        paths: Workspace paths, used to locate both the destination file and each
            table's Parquet file. Any existing destination file is deleted first;
            parent directories are created as needed.

    Returns:
        A :class:`TableWriteReport` with a row count per table written and a
        :class:`TableWriteErrorInfo` per table that failed.
    """
    attach_alias = "pudl_sqlite"
    db_path = paths.sqlite_path("pudl")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.unlink(missing_ok=True)

    engine = sa.create_engine(f"sqlite:///{db_path}")
    PUDL_PACKAGE.to_sql(
        dialect="sqlite", check_types=False, check_values=False
    ).create_all(engine)
    engine.dispose()

    conn = duckdb.connect()
    conn.execute("PRAGMA disable_progress_bar")
    conn.execute("LOAD sqlite")
    conn.execute(f"ATTACH '{db_path}' AS {attach_alias} (TYPE sqlite)")  # noqa: S608

    report = TableWriteReport(db_path=db_path)
    n_tables = len(table_names)
    try:
        for n, table_name in enumerate(table_names, start=1):
            logger.info(f"Writing SQLite {n}/{n_tables} {table_name}")
            # Fetched outside the try/except: a missing Resource is a schema bug,
            # not a per-table data problem.
            resource = PUDL_PACKAGE.get_resource(table_name)
            try:
                _validate_primary_key(resource, paths)
                report.row_counts[table_name] = _copy_table(
                    conn,
                    resource,
                    paths.parquet_path(table_name),
                    table_ref=f'{attach_alias}."{table_name}"',
                )
            except _WRITE_EXCEPTIONS as exc:
                logger.error(f"Failed to write {table_name} to sqlite: {exc}")
                report.errors.append(TableWriteErrorInfo(table_name, exc))
    finally:
        conn.execute(f"DETACH {attach_alias}")
        conn.close()
    return report


################################################################################
# pudl.duckdb
################################################################################


def _write_pudl_duckdb(
    table_names: Sequence[str],
    paths: PudlPaths,
) -> TableWriteReport:
    r"""Assemble ``$PUDL_OUTPUT/pudl.duckdb`` from the Parquet outputs.

    Lays down an empty schema through a throwaway SQLAlchemy engine, then reads each
    table's Parquet file straight into the DuckDB file with DuckDB's own engine.

    Unlike the SQLite schema, keep every CHECK and type constraint our metadata defines
    (DuckDB enforces them cheaply on write). It omits foreign keys entirely: a handful
    of our FK column pairs have mismatched types that DuckDB rejects (issue #5552).

    Args:
        table_names: Tables to load, in the order they should be inserted.
        paths: Workspace paths, used to locate both the destination file and each
            table's Parquet file. Any existing destination file is deleted first;
            parent directories are created as needed.

    Returns:
        A :class:`TableWriteReport` with a row count per table written and a
        :class:`TableWriteErrorInfo` per table that failed.
    """
    db_path = paths.duckdb_path("pudl")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.unlink(missing_ok=True)

    engine = sa.create_engine(f"duckdb:///{db_path}")
    engine.dialect.max_identifier_length = _DUCKDB_MAX_IDENTIFIER_LENGTH
    PUDL_PACKAGE.to_sql(dialect="duckdb", include_foreign_keys=False).create_all(engine)
    engine.dispose()

    conn = duckdb.connect(str(db_path))
    conn.execute("PRAGMA disable_progress_bar")

    report = TableWriteReport(db_path=db_path)
    n_tables = len(table_names)
    try:
        for n, table_name in enumerate(table_names, start=1):
            logger.info(f"Writing DuckDB {n}/{n_tables} {table_name}")
            # Fetched outside the try/except: a missing Resource is a schema bug,
            # not a per-table data problem.
            resource = PUDL_PACKAGE.get_resource(table_name)
            try:
                report.row_counts[table_name] = _copy_table(
                    conn,
                    resource,
                    paths.parquet_path(table_name),
                    table_ref=f'"{table_name}"',
                )
            except _WRITE_EXCEPTIONS as exc:
                logger.error(f"Failed to write {table_name} to duckdb: {exc}")
                report.errors.append(TableWriteErrorInfo(table_name, exc))
    finally:
        conn.close()
    return report


################################################################################
# Dagster asset factory
################################################################################


def build_pudl_db_asset(
    *,
    name: str,
    description: str,
    write_db: Callable[[Sequence[str], PudlPaths], TableWriteReport],
    asset_keys: Sequence[dg.AssetKey],
) -> dg.AssetsDefinition:
    """Build the Dagster asset that assembles one database from the Parquet outputs.

    Args:
        name: Asset name (``"pudl_sqlite"`` / ``"pudl_duckdb"``).
        description: Dagster asset description.
        write_db: The per-format writer (:func:`_write_pudl_sqlite` /
            :func:`_write_pudl_duckdb`). It resolves its own destination path from the
            workspace paths and reports it back on the :class:`TableWriteReport`.
        asset_keys: Keys of the Parquet-writing assets whose tables to include; used
            both as the asset's deps and as the ordered list of tables to insert.

    Returns:
        A Dagster :class:`~dagster.AssetsDefinition` for the database asset.
    """

    @dg.asset(
        name=name,
        group_name="out_pudl",
        deps=list(asset_keys),
        required_resource_keys={"pudl_paths"},
        description=description,
    )
    def _pudl_db(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """Materialize the database and record its size and table count."""
        pudl_paths: PudlPaths = context.resources.pudl_paths
        table_names = [key.path[-1] for key in asset_keys]
        report = write_db(table_names, pudl_paths)

        metadata: dict[str, dg.MetadataValue] = {
            "path": dg.MetadataValue.path(report.db_path),
            "table_count": dg.MetadataValue.int(len(report.row_counts)),
            "bytes": dg.MetadataValue.int(report.db_path.stat().st_size),
        }
        if report.errors:
            metadata["failed_tables"] = dg.MetadataValue.md(
                "\n".join(f"- {error}" for error in report.errors)
            )
            raise dg.Failure(description=report.summary(), metadata=metadata)

        return dg.MaterializeResult(metadata=metadata)

    return _pudl_db


def build_pudl_sqlite_asset(asset_keys: Sequence[dg.AssetKey]) -> dg.AssetsDefinition:
    """Build the ``pudl_sqlite`` asset. Delete this once ``pudl.sqlite`` is retired."""
    return build_pudl_db_asset(
        name="pudl_sqlite",
        description=SQLITE_DESCRIPTION,
        write_db=_write_pudl_sqlite,
        asset_keys=asset_keys,
    )


def build_pudl_duckdb_asset(asset_keys: Sequence[dg.AssetKey]) -> dg.AssetsDefinition:
    """Build the ``pudl_duckdb`` asset."""
    return build_pudl_db_asset(
        name="pudl_duckdb",
        description=DUCKDB_DESCRIPTION,
        write_db=_write_pudl_duckdb,
        asset_keys=asset_keys,
    )


__all__ = [
    "TableWriteErrorInfo",
    "TableWriteReport",
    "build_pudl_duckdb_asset",
    "build_pudl_sqlite_asset",
]
