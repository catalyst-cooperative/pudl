"""Extract FERC EQR data."""

import io
import re
import shutil
import tempfile
import zipfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal, get_args

import dagster as dg
import duckdb
from duckdb import DuckDBPyConnection
from upath import UPath

from pudl.dagster.partitions import ferceqr_year_quarters
from pudl.dagster.resources import FercEqrArchiveResource
from pudl.helpers import ParquetData, persist_table_as_parquet
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

_PROGRESS_LOG_INTERVAL = 500
"""How often (in filings) to log extraction progress for a quarter.

Some quarters contain thousands of filings, each processed one at a time; without
this, a long-running extraction has no visible sign of progress in the logs.
"""

FercEqrTableType = Literal["ident", "contracts", "transactions", "indexPub"]
"""The four raw table types present in each FERC EQR filing."""

_ALL_TABLE_TYPES: tuple[FercEqrTableType, ...] = get_args(FercEqrTableType)
"""Canonical list of all :data:`FercEqrTableType` values, in extraction order.

``ident`` is extracted first so its CID can be attached to the other tables; see
:func:`_extract_ident`.
"""


@contextmanager
def _get_csv(base_path: UPath, year_quarter: str) -> Generator[zipfile.ZipFile]:
    """Download CSV to a tempmorary directory to avoid reading into memory."""
    zip_name = f"ferceqr-{year_quarter}.zip"
    remote_path = base_path / zip_name

    # Create temp directory to download zip to
    with (
        tempfile.TemporaryDirectory() as tmp_dir,
    ):
        # Download file to local path
        local_path = Path(tmp_dir) / zip_name
        local_path.write_bytes(remote_path.read_bytes())
        # Yield open zipfile
        with zipfile.ZipFile(local_path) as zf:
            yield zf


_UNSAFE_CSV_NAME_CHARS_REGEX: re.Pattern = re.compile(f"[{re.escape('\'"*?[]')}]")
"""Characters stripped from an extracted CSV's filename before duckdb reads it.

``*``, ``?``, ``[``, and ``]`` are glob metacharacters that duckdb's CSV reader
interprets even when passed a single literal path (not a SQL string): a file literally
named ``[ab].csv`` gets silently read as ``a.csv`` instead, if that file happens to
exist alongside it -- no error, just wrong data. Quote characters are stripped
defensively too, a long-standing precaution predating this docstring; the specific
failure it was guarding against isn't reproducible against the current duckdb version,
but real filings do contain apostrophes (e.g. "Citizens' Electric") so there's no reason
to stop.

As of 2026-08-03 none of the known to be unsafe characters appeared in the most recent
batch of FERC EQR filenames in any of the 52 quarters (2013q3-2026q2, ~146k
filings/~533k CSVs). Real filenames do contain parentheses, ``#``, and occasional
encoding-mangled accented letters, none of which are unsafe here and are left alone.
Stripping this set costs nothing and closes a silent-wrong-data failure mode before it
has a chance to occur.
"""


def _clean_csv_name(csv_path: Path) -> Path:
    """Strip characters from an extracted CSV's filename that would confuse duckdb."""
    cleaned_name = _UNSAFE_CSV_NAME_CHARS_REGEX.sub("", csv_path.name)
    if cleaned_name == csv_path.name:
        return csv_path
    return csv_path.rename(csv_path.parent / cleaned_name)


def _get_table_name(table_type: FercEqrTableType, year_quarter: str) -> str:
    if table_type != "indexPub":
        return f"raw_ferceqr__{table_type}_{year_quarter}"
    return f"raw_ferceqr__index_pub_{year_quarter}"


def _clear_raw_table_partition(table_type: FercEqrTableType, year_quarter: str) -> None:
    """Delete any existing per-filing parquet output for one raw table+quarter.

    Each filing's output file is named after that filing's own ID. Filing IDs change
    with every revision or resubmission. This means if we don't clear the extracted
    parquet output for a quarter before re-extracting it, we can end up with multiple
    duplicate filings for the same company and quarter. This is unlikely to be an
    issue in the production builds, but is a problem for local development and
    testing.
    """
    directory = ParquetData(
        table_name=_get_table_name(table_type, year_quarter)
    ).parquet_directory
    shutil.rmtree(directory, ignore_errors=True)


def _extract_ident(
    ident_csv: str,
    year_quarter: str,
    filing_name: str,
    duckdb_connection: DuckDBPyConnection,
) -> str | None:
    """Extract data from ident csv, write to parquet, and return CID from table.

    This table is always extracted first so we can pull the CID from it and include
    a CID column in all other tables.

    Returns:
        The company identifier (CID) read from the ident table's first row, or
        ``None`` if the CSV parsed but contained no rows -- in which case no
        ``raw_ferceqr__ident`` parquet is written for this filing. A CSV that fails
        to parse at all instead raises ``duckdb.Error``, left to the caller.
    """
    # Use duckdb to read CSV and write as parquet
    csv_rel = duckdb_connection.read_csv(
        ident_csv, all_varchar=True, store_rejects=True, ignore_errors=True
    )
    row = csv_rel.select("company_identifier").limit(1).fetchone()
    if row is None:
        return None
    (cid,) = row
    persist_table_as_parquet(
        csv_rel.select(f"*, '{year_quarter}' AS year_quarter"),
        table_name=_get_table_name("ident", year_quarter),
        partitions={"filing": filing_name},
    )
    return cid


def _extract_other_table(
    table_type: FercEqrTableType,
    csv_path: str | Path,
    year_quarter: str,
    cid: str | None,
    filing_name: str,
    duckdb_connection: DuckDBPyConnection,
):
    """Extract data from a table other than ident and add year_quarter and CID columns.

    ``cid`` is None when the filing has no usable identity CSV (missing entirely,
    or present but unpareable) -- the table is still worth extracting, just
    without a company_identifier to attach, so a real SQL NULL is used rather
    than the literal string ``"None"``.
    """
    cid_expr = f"'{cid}'" if cid is not None else "NULL"
    # Use duckdb to read CSV and write as parquet
    persist_table_as_parquet(
        duckdb_connection.read_csv(
            csv_path, all_varchar=True, store_rejects=True, ignore_errors=True
        ).select(
            f"*, '{year_quarter}' AS year_quarter, {cid_expr} as company_identifier"
        ),
        table_name=_get_table_name(table_type, year_quarter),
        partitions={"filing": filing_name},
    )


def _resolve_cid(
    ident_path: Path,
    year_quarter: str,
    filing_name: str,
    duckdb_connection: DuckDBPyConnection,
) -> str | None:
    """Extract one filing's ident table and return its CID, or ``None``.

    Warns (but does not raise) if the identity CSV fails to parse entirely or
    parses with no rows -- either way the rest of the filing is still worth
    extracting, just with a null company_identifier.
    """
    try:
        cid = _extract_ident(
            ident_csv=str(ident_path),
            year_quarter=year_quarter,
            filing_name=filing_name,
            duckdb_connection=duckdb_connection,
        )
    except duckdb.Error as err:
        logger.warning(
            f"Failed to parse ident table from {ident_path.name} ({err}) -- "
            "processing remaining tables with a null company_identifier."
        )
        return None
    if cid is None:
        logger.warning(
            f"Identity CSV {ident_path.name!r} for filing {filing_name!r} "
            f"({year_quarter}) contains no rows -- processing remaining "
            "tables with a null company_identifier."
        )
    return cid


def _csvs_to_parquet(
    csv_path: Path,
    year_quarter: str,
    filing_name: str,
    duckdb_connection: DuckDBPyConnection,
) -> frozenset[FercEqrTableType]:
    """Mirror CSVs in filing to a parquet file.

    Each filing is expected to contain a CSV for each of 4 EQR tables, extracted
    to a separate parquet file. Real filings are sometimes incomplete or
    malformed in ways that have been observed in practice, and those cases are
    not fatal to the rest of the filing. A missing contracts, transactions, or
    indexPub CSV is routine -- many thousands of filings in a given quarter
    simply have no data of that type -- so it's counted in the return value
    rather than logged; logging a warning per filing for something this common
    would flood the logs without conveying anything useful. A missing, empty,
    or unparsable identity CSV, by contrast, is rare and more consequential (it
    means no company_identifier (CID) can be attached to the other tables), so
    it's still logged individually; the other tables are still extracted, with
    a null CID rather than being dropped entirely. A zip archive that fails to
    parse due to corruption and unrecognized CSVs are also logged with a
    warning per filing since it is relatively rare.

    More than one CSV matching the same table type has never been observed in
    the wild and has no established handling strategy, so it raises rather
    than silently guessing (e.g. by using the first match) -- a warning here
    would be easy to miss among the routine ones above.

    Records rejected by DuckDB due to too many or too few columns, invalid
    UTF-8 encoding, or other reasons are not fatal. The bad records are
    loaded into their own parquet files for later inspection.

    Returns:
        The subset of ``_ALL_TABLE_TYPES`` whose CSV was present in this
        filing, regardless of whether it was successfully parsed. Used by the
        caller to tally how many filings in the quarter included each table.
    """
    # Clean csv filenames for duckdb compatibility, then get ident table path
    csv_paths = [_clean_csv_name(csv_file) for csv_file in csv_path.iterdir()]
    ident_matches = [
        csv_file for csv_file in csv_paths if csv_file.stem.endswith("ident")
    ]

    found_table_types: set[FercEqrTableType] = set()
    cid = None
    if not ident_matches:
        logger.warning(
            f"Filing {filing_name!r} for {year_quarter} has no identity CSV -- "
            "processing remaining tables with a null company_identifier. "
            f"Files present: {[p.name for p in csv_paths]}."
        )
    else:
        found_table_types.add("ident")
        if len(ident_matches) > 1:
            raise ValueError(
                f"Filing {filing_name!r} for {year_quarter} has "
                f"{len(ident_matches)} identity CSVs, expected at most 1: "
                f"{[p.name for p in ident_matches]}. This filing shape has "
                "not been observed before -- inspect the raw archive to "
                "determine how to handle it before proceeding."
            )
        ident_path = ident_matches[0]
        csv_paths.remove(ident_path)
        cid = _resolve_cid(
            ident_path=ident_path,
            year_quarter=year_quarter,
            filing_name=filing_name,
            duckdb_connection=duckdb_connection,
        )

    # Group remaining CSVs by table type, warning about anything unexpected: an
    # unrecognized CSV, or more than one matching CSV for the same table type.
    # A table with no matching CSV at all is routine and simply omitted from
    # the return value -- not warned about, see docstring.
    other_table_types = tuple(t for t in _ALL_TABLE_TYPES if t != "ident")
    files_by_type: dict[FercEqrTableType, list[Path]] = {
        t: [] for t in other_table_types
    }
    for file in csv_paths:
        table_type_matches = [
            key for key in other_table_types if file.stem.endswith(key)
        ]
        if not table_type_matches:
            logger.warning(
                f"Filing {filing_name!r} for {year_quarter} contains "
                f"unrecognized CSV {file.name!r} -- skipping this file."
            )
            continue
        files_by_type[table_type_matches[0]].append(file)

    for table_type, files in files_by_type.items():
        if not files:
            continue
        found_table_types.add(table_type)
        if len(files) > 1:
            raise ValueError(
                f"Filing {filing_name!r} for {year_quarter} has {len(files)} "
                f"{table_type} CSVs, expected at most 1: "
                f"{[p.name for p in files]}. This filing shape has not been "
                "observed before -- inspect the raw archive to determine "
                "how to handle it before proceeding."
            )

        # Use duckdb to read CSV and write as parquet
        try:
            _extract_other_table(
                table_type=table_type,
                csv_path=files[0],
                year_quarter=year_quarter,
                cid=cid,
                filing_name=filing_name,
                duckdb_connection=duckdb_connection,
            )
        except duckdb.Error as err:
            logger.warning(
                f"Failed to parse {table_type} table from {files[0].name} "
                f"({err}) -- skipping this table."
            )

    return frozenset(found_table_types)


def _get_rejected_record_counts(
    duckdb_connection: DuckDBPyConnection,
) -> dict[str, int]:
    """Count rejected CSV records by DuckDB's reason for rejecting them.

    ``reject_errors.error_type`` is a DuckDB-defined enum with one row per
    rejected record; as of DuckDB 1.5 its possible values are ``CAST``,
    ``MISSING COLUMNS``, ``TOO MANY COLUMNS``, ``UNQUOTED VALUE``, ``LINE SIZE
    OVER MAXIMUM``, ``INVALID ENCODING``, and ``INVALID STATE``. In FERC EQR
    filings the two seen in practice are invalid UTF-8 encoding and a wrong
    column count from unescaped quotes within a field.

    Returns:
        A dict mapping each ``error_type`` string observed among rejected records
        (e.g. ``"INVALID ENCODING"``) to the count of records rejected for that
        reason, e.g. ``{"INVALID ENCODING": 12, "MISSING COLUMNS": 4}``. Error
        types with no rejected records are simply absent from the dict.
    """
    rows = duckdb_connection.sql(
        "SELECT error_type, count(*) AS n FROM reject_errors GROUP BY error_type"
    ).fetchall()
    return dict(rows)


def _save_extract_errors(
    year_quarter: str, duckdb_connection: DuckDBPyConnection
) -> None:
    """Persist DuckDB's CSV parsing errors for the quarter to parquet.

    Joins DuckDB's ``reject_errors`` table (one row per rejected CSV record) against
    ``reject_scans`` (one row per CSV file scanned) to attach the source filename to
    each rejected record, then writes the result to the
    ``raw_ferceqr__extract_errors`` table. ``extract_ferceqr`` builds its own
    :class:`~pudl.helpers.ParquetData` pointing at this same table/quarter after
    calling this function, the same way it does for the other four raw tables.
    """
    persist_table_as_parquet(
        duckdb_connection.table("reject_errors")
        .join(
            duckdb_connection.table("reject_scans"),
            condition="reject_errors.scan_id=reject_scans.scan_id AND reject_errors.file_id=reject_scans.file_id",
        )
        .select(
            f"reject_errors.*, parse_filename(reject_scans.file_path), '{year_quarter}' as year_quarter"
        ),
        table_name="raw_ferceqr__extract_errors",
        partitions={"year_quarter": year_quarter},
    )


@dg.multi_asset(
    partitions_def=ferceqr_year_quarters,
    outs={
        "raw_ferceqr__ident": dg.AssetOut(kinds={"duckdb"}),
        "raw_ferceqr__contracts": dg.AssetOut(kinds={"duckdb"}),
        "raw_ferceqr__transactions": dg.AssetOut(kinds={"duckdb"}),
        "raw_ferceqr__index_pub": dg.AssetOut(kinds={"duckdb"}),
        "raw_ferceqr__extract_errors": dg.AssetOut(kinds={"duckdb"}),
    },
)
def extract_ferceqr(
    context: dg.AssetExecutionContext,
    ferceqr_archive: FercEqrArchiveResource = FercEqrArchiveResource(),
):
    """Extract year quarter from CSVs and load to parquet files.

    This method will loop through the nested EQR archive zipfiles and extract all tables
    from them, and write to parquet. It opens a duckdb connection at the top level to
    keep track of extraction errors, so we can write these to the ``raw_ferceqr__extract_errors``
    table. Summary statistics about the extraction (filing counts, corrupt archives,
    and unextractable records by reason) are logged and attached as Dagster metadata
    on the ``raw_ferceqr__extract_errors`` output.
    """
    # Get year/quarter from selected partition
    year_quarter = context.partition_key

    # Clear any leftover per-filing output from a previous extraction of this same
    # quarter before starting, so amended filings that changed ID between archive
    # pulls can't leave stale, orphaned duplicates behind. See
    # _clear_raw_table_partition for why this is necessary.
    for table_type in _ALL_TABLE_TYPES:
        _clear_raw_table_partition(table_type, year_quarter)

    table_file_counts = dict.fromkeys(_ALL_TABLE_TYPES, 0)
    corrupt_filing_count = 0

    # Open top level zipfile
    with (
        _get_csv(ferceqr_archive.upath, year_quarter) as quarter_archive,
        duckdb.connect() as conn,
    ):
        # Disable DuckDB progress bar, as it is quite noisy in the logs.
        conn.execute("PRAGMA disable_progress_bar")
        # Loop through all nested zipfiles (one for each filing in the quarter)
        filing_names = quarter_archive.namelist()
        logger.info(f"Extracting {len(filing_names)} filings for {year_quarter}.")
        for filing_number, filing in enumerate(filing_names, start=1):
            # Quarters can contain thousands of filings, each processed one at a
            # time below -- log progress periodically so a long-running extraction
            # doesn't look stalled.
            if filing_number % _PROGRESS_LOG_INTERVAL == 0:
                logger.info(
                    f"Extracted {filing_number}/{len(filing_names)} filings "
                    f"for {year_quarter}."
                )
            # Extract CSVs from filing to a temporary directory so duckdb can be used
            # to parse CSVs and mirror to parquet
            try:
                with (
                    zipfile.ZipFile(
                        io.BytesIO(quarter_archive.read(filing))
                    ) as filing_archive,
                    tempfile.TemporaryDirectory() as tmp_dir,
                ):
                    filing_archive.extractall(path=tmp_dir)
                    found_table_types = _csvs_to_parquet(
                        csv_path=Path(tmp_dir),
                        year_quarter=year_quarter,
                        filing_name=Path(filing).stem,
                        duckdb_connection=conn,
                    )
                    for table_type in found_table_types:
                        table_file_counts[table_type] += 1
            except zipfile.BadZipfile:
                corrupt_filing_count += 1
                logger.warning(f"Could not open filing: {filing}.")
        logger.info(
            f"Finished extracting {len(filing_names)} filings for {year_quarter}."
        )
        _save_extract_errors(year_quarter, conn)
        rejected_record_counts = _get_rejected_record_counts(conn)

    extraction_stats = {
        "total_filings": len(filing_names),
        "corrupt_filings": corrupt_filing_count,
        "table_file_counts": table_file_counts,
        "rejected_record_counts": rejected_record_counts,
    }
    logger.info(f"Extraction summary for {year_quarter}: {extraction_stats}")
    context.add_output_metadata(
        metadata={"extraction_stats": dg.MetadataValue.json(extraction_stats)},
        output_name="raw_ferceqr__extract_errors",
    )

    return (
        dg.Output(
            output_name="raw_ferceqr__ident",
            value=ParquetData(table_name=_get_table_name("ident", year_quarter)),
        ),
        dg.Output(
            output_name="raw_ferceqr__contracts",
            value=ParquetData(table_name=_get_table_name("contracts", year_quarter)),
        ),
        dg.Output(
            output_name="raw_ferceqr__transactions",
            value=ParquetData(table_name=_get_table_name("transactions", year_quarter)),
        ),
        dg.Output(
            output_name="raw_ferceqr__index_pub",
            value=ParquetData(table_name=_get_table_name("indexPub", year_quarter)),
        ),
        dg.Output(
            output_name="raw_ferceqr__extract_errors",
            value=ParquetData(table_name="raw_ferceqr__extract_errors"),
        ),
    )
