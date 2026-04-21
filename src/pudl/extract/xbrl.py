"""Generic extractor for all FERC XBRL data."""

import io
import logging
import re
import sys
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

from dagster import op
from ferc_xbrl_extractor.cli import run_main

import pudl
from pudl.resources import RuntimeSettings
from pudl.settings import FercGenericXbrlToSqliteSettings, XbrlFormNumber
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


class _FilteringStream:
    """Pass-through text stream that drops matching noisy lines."""

    def __init__(self, wrapped, drop_patterns: list[re.Pattern[str]]):
        self._wrapped = wrapped
        self._drop_patterns = drop_patterns
        self._dropped_previous_line = False

    def write(self, text: str) -> int:
        for line in text.splitlines(keepends=True):
            stripped = line.rstrip("\r\n")
            if stripped and any(p.search(stripped) for p in self._drop_patterns):
                self._dropped_previous_line = True
                continue

            # Arelle occasionally emits a blank line right after spam lines.
            if not stripped and self._dropped_previous_line:
                continue

            self._wrapped.write(line)
            self._dropped_previous_line = False
        return len(text)

    def flush(self) -> None:
        self._wrapped.flush()


@contextmanager
def _suppress_arelle_message_spam():
    """Filter known Arelle console spam without suppressing normal logs."""
    drop_patterns = [
        re.compile(r"^Message:\s+Try\s+#\d+"),
        re.compile(r"^Message log error:\s+Formatting field not found in record:"),
    ]
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = _FilteringStream(old_stdout, drop_patterns)
    sys.stderr = _FilteringStream(old_stderr, drop_patterns)
    try:
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


class FercXbrlDatastore:
    """Simple datastore wrapper for accessing ferc1 xbrl resources."""

    def __init__(self, datastore: Datastore):
        """Instantiate datastore wrapper for ferc1 resources."""
        self.datastore = datastore

    def get_taxonomy(self, form: XbrlFormNumber) -> io.BytesIO:
        """Returns the path to the taxonomy entry point within the an archive."""
        raw_archive = self.datastore.get_unique_resource(
            f"ferc{form.value}",
            data_format="xbrl_taxonomy",
        )

        return io.BytesIO(raw_archive)

    def get_filings(self, year: int, form: XbrlFormNumber) -> io.BytesIO:
        """Return the corresponding archive full of XBRL filings."""
        return io.BytesIO(
            self.datastore.get_unique_resource(
                f"ferc{form.value}", year=year, data_format="xbrl"
            )
        )


def xbrl2sqlite_op_factory(form: XbrlFormNumber) -> Callable:
    """Generates xbrl2sqlite op for a given FERC form."""

    @op(
        name=f"ferc{form.value}_xbrl",
        required_resource_keys={
            "etl_settings",
            "datastore",
            "runtime_settings",
        },
        tags={"data_format": "xbrl", "dataset": f"ferc{form.value}"},
    )
    def inner_op(context) -> None:
        output_path = PudlPaths().output_dir
        rs: RuntimeSettings = context.resources.runtime_settings
        settings = (
            context.resources.etl_settings.ferc_to_sqlite_settings.get_dataset_settings(
                dataset=f"ferc{form.value}", data_format="xbrl"
            )
        )
        datastore = FercXbrlDatastore(context.resources.datastore)

        logger.info(f"====== xbrl2sqlite runtime_settings: {rs}")
        if settings is None or not settings.years:
            logger.info(
                f"Skipping dataset ferc{form.value}_xbrl: no config or no years configured."
            )
            return

        sqlite_path = PudlPaths().sqlite_db_path(f"ferc{form.value}_xbrl")
        if sqlite_path.exists():
            sqlite_path.unlink()
        duckdb_path = PudlPaths().duckdb_db_path(f"ferc{form.value}_xbrl")
        if duckdb_path.exists():
            duckdb_path.unlink()

        convert_form(
            settings,
            form,
            datastore,
            output_path=output_path,
            sqlite_path=sqlite_path,
            duckdb_path=duckdb_path,
            batch_size=rs.xbrl_batch_size,
            workers=rs.xbrl_num_workers,
            loglevel=rs.xbrl_loglevel,
        )

    return inner_op


def convert_form(
    form_settings: FercGenericXbrlToSqliteSettings,
    form: XbrlFormNumber,
    datastore: FercXbrlDatastore,
    output_path: Path,
    sqlite_path: Path,
    duckdb_path: Path,
    batch_size: int | None = None,
    workers: int | None = None,
    loglevel: str = "INFO",
) -> None:
    """Clone a single FERC XBRL form to SQLite.

    Args:
        form_settings: Validated settings for converting the desired XBRL form to SQLite.
        form: FERC form number.
        datastore: Instance of a FERC XBRL datastore for retrieving data.
        output_path: PUDL output directory
        sql_path: path to the SQLite DB we'd like to write to.
        batch_size: Number of XBRL filings to process in a single CPU process.
        workers: Number of CPU processes to create for processing XBRL filings.

    Returns:
        None
    """
    datapackage_path = output_path / f"ferc{form.value}_xbrl_datapackage.json"
    metadata_path = output_path / f"ferc{form.value}_xbrl_taxonomy_metadata.json"

    taxonomy_archive = datastore.get_taxonomy(form)
    # Process XBRL filings for each year requested
    filings_archives = [
        datastore.get_filings(year, form) for year in form_settings.years
    ]
    # if we set clobber=True, clobbers on *every* call to run_main;
    # we already delete the existing base on `clobber=True` in `xbrl2sqlite`
    # Arelle can emit very verbose internals; keep its logger at ERROR unless
    # troubleshooting parser internals.
    logging.getLogger("arelle").setLevel(logging.ERROR)

    with _suppress_arelle_message_spam():
        run_main(
            filings=filings_archives,
            sqlite_path=sqlite_path,
            duckdb_path=duckdb_path,
            taxonomy=taxonomy_archive,
            form_number=form.value,
            metadata_path=metadata_path,
            datapackage_path=datapackage_path,
            workers=workers,
            batch_size=batch_size,
            loglevel=loglevel,
            logfile=None,
        )
