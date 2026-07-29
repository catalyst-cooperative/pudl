"""Extract FERC EQR data."""

import io
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path

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


@contextmanager
def _get_csv(base_path: UPath, year_quarter: str) -> zipfile.ZipFile:
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


def _clean_csv_name(csv_path: Path) -> Path:
    """Standardize zip file names to avoid errors when opening."""
    new_path = csv_path
    if "'" in csv_path.name:
        new_path = csv_path.rename(csv_path.parent / csv_path.name.replace("'", ""))
    if '"' in csv_path.name:
        new_path = csv_path.rename(csv_path.parent / csv_path.name.replace('"', ""))
    return new_path


def _get_table_name(table_type: str, year_quarter: str) -> str:
    if table_type != "indexPub":
        return f"raw_ferceqr__{table_type}_{year_quarter}"
    return f"raw_ferceqr__index_pub_{year_quarter}"


def _extract_ident(
    ident_csv: str,
    year_quarter: str,
    filing_name: str,
    duckdb_connection: DuckDBPyConnection,
) -> str:
    """Extract data from ident csv, write to parquet, and return CID from table.

    This table is always extracted first so we can pull the CID from it and include
    a CID column in all other tables.
    """
    # Use duckdb to read CSV and write as parquet
    csv_rel = duckdb_connection.read_csv(
        ident_csv, all_varchar=True, store_rejects=True, ignore_errors=True
    )
    (cid,) = csv_rel.select("company_identifier").limit(1).fetchone()
    persist_table_as_parquet(
        csv_rel.select(f"*, '{year_quarter}' AS year_quarter"),
        table_name=_get_table_name("ident", year_quarter),
        partitions={"filing": filing_name},
    )
    return cid


def _extract_other_table(
    table_type: str,
    csv_path: str,
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


def _csvs_to_parquet(
    csv_path: Path,
    year_quarter: str,
    filing_name: str,
    duckdb_connection: DuckDBPyConnection,
):
    """Mirror CSVs in filing to a parquet file.

    Each filing is expected to contain a CSV for each of 4 EQR tables, extracted
    to a separate parquet file. Real filings are sometimes incomplete -- e.g.
    missing the identity CSV entirely, or including a CSV that doesn't match any
    known table type. A missing or unparsable identity CSV means no
    company_identifier (CID) can be attached to the other tables, but those
    other tables (typically contracts/transactions, the most valuable ones) are
    still extracted, with a null CID rather than being dropped entirely. An
    unrecognized CSV is skipped on its own, not fatal to the rest of the filing.
    All these cases log a warning naming the files involved.
    """
    # Clean csv filenames for duckdb compatibility, then get ident table path
    csv_paths = [_clean_csv_name(csv_file) for csv_file in csv_path.iterdir()]
    ident_matches = [
        csv_file for csv_file in csv_paths if csv_file.stem.endswith("ident")
    ]

    cid = None
    if not ident_matches:
        logger.warning(
            f"Filing {filing_name!r} for {year_quarter} has no identity CSV -- "
            "processing remaining tables with a null company_identifier. "
            f"Files present: {[p.name for p in csv_paths]}."
        )
    else:
        if len(ident_matches) > 1:
            logger.warning(
                f"Filing {filing_name!r} for {year_quarter} has "
                f"{len(ident_matches)} identity CSVs, expected 1: "
                f"{[p.name for p in ident_matches]}. Using "
                f"{ident_matches[0].name!r}."
            )
        ident_path = ident_matches[0]
        csv_paths.remove(ident_path)

        try:
            # Extract ident table and return CID
            cid = _extract_ident(
                ident_csv=str(ident_path),
                year_quarter=year_quarter,
                filing_name=filing_name,
                duckdb_connection=duckdb_connection,
            )
        except TypeError:
            logger.warning(
                f"Failed to parse ident table from {ident_path.name} -- "
                "processing remaining tables with a null company_identifier."
            )
            cid = None

    # Loop through remaining CSVs and extract, adding extracted CID as a column in each table
    for file in csv_paths:
        # Detect which table type CSV is and prep output directory
        table_type_matches = [
            key
            for key in ["contracts", "transactions", "indexPub"]
            if file.stem.endswith(key)
        ]
        if not table_type_matches:
            logger.warning(
                f"Filing {filing_name!r} for {year_quarter} contains "
                f"unrecognized CSV {file.name!r} -- skipping this file."
            )
            continue
        table_type = table_type_matches[0]

        # Use duckdb to read CSV and write as parquet
        _extract_other_table(
            table_type=table_type,
            csv_path=file,
            year_quarter=year_quarter,
            cid=cid,
            filing_name=filing_name,
            duckdb_connection=duckdb_connection,
        )


def _save_extract_errors(year_quarter: str, duckdb_connection: DuckDBPyConnection):
    """Create parquet file with metadata on any CSV parsing errors."""
    return persist_table_as_parquet(
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
        "raw_ferceqr__ident": dg.AssetOut(),
        "raw_ferceqr__contracts": dg.AssetOut(),
        "raw_ferceqr__transactions": dg.AssetOut(),
        "raw_ferceqr__index_pub": dg.AssetOut(),
        "raw_ferceqr__extract_errors": dg.AssetOut(),
    },
)
def extract_ferceqr(
    context: dg.AssetExecutionContext,
    ferceqr_archive: FercEqrArchiveResource = FercEqrArchiveResource(),
) -> tuple[ParquetData, ParquetData, ParquetData, ParquetData, ParquetData]:
    """Extract year quarter from CSVs and load to parquet files.

    This method will loop through the nested EQR archive zipfiles and extract all tables
    from them, and write to parquet. It opens a duckdb connection at the top level to
    keep track of extraction errors, so we can write these to the ``raw_ferceqr__extract_errors``
    table.
    """
    # Get year/quarter from selected partition
    year_quarter = context.partition_key

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
                    _csvs_to_parquet(
                        csv_path=Path(tmp_dir),
                        year_quarter=year_quarter,
                        filing_name=Path(filing).stem,
                        duckdb_connection=conn,
                    )
            except zipfile.BadZipfile:
                logger.warning(f"Could not open filing: {filing}.")
        logger.info(
            f"Finished extracting {len(filing_names)} filings for {year_quarter}."
        )
        metadata = _save_extract_errors(year_quarter, conn)
    return (
        ParquetData(table_name=_get_table_name("ident", year_quarter)),
        ParquetData(table_name=_get_table_name("contracts", year_quarter)),
        ParquetData(table_name=_get_table_name("transactions", year_quarter)),
        ParquetData(table_name=_get_table_name("indexPub", year_quarter)),
        metadata,
    )
