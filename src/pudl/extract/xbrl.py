"""Generic extractor for all FERC XBRL data."""

import io
import logging
import re
import sys
from _io import BytesIO
from contextlib import contextmanager

from ferc_xbrl_extractor.cli import run_main

import pudl.logging_helpers
from pudl.settings import (
    FercToSqliteDataConfig,
    XbrlFormNumber,
)
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
            str(form),
            data_format="xbrl_taxonomy",
        )

        return io.BytesIO(raw_archive)

    def get_filings(self, year: int, form: XbrlFormNumber) -> io.BytesIO:
        """Return the corresponding archive full of XBRL filings."""
        return io.BytesIO(
            self.datastore.get_unique_resource(str(form), year=year, data_format="xbrl")
        )


def convert_form(
    ferc_to_sqlite: FercToSqliteDataConfig,
    form: XbrlFormNumber,
    datastore: FercXbrlDatastore,
    pudl_paths: PudlPaths,
    batch_size: int | None = None,
    workers: int | None = None,
    loglevel: str = "INFO",
) -> None:
    """Clone a single FERC XBRL form to SQLite.

    Args:
        ferc_to_sqlite: Validated data configuration for converting FERC data to SQLite.
        form: FERC form number.
        datastore: Instance of a FERC XBRL datastore for retrieving data.
        pudl_paths: ``PudlPaths`` resource.
        batch_size: Number of XBRL filings to process in a single CPU process.
        workers: Number of CPU processes to create for processing XBRL filings.
        loglevel: Log level to pass to ``ferc_xbrl_extractor``.
    """
    output_path = pudl_paths.pudl_output
    sqlite_path = pudl_paths.sqlite_db_path(f"{form}_xbrl")
    duckdb_path = pudl_paths.duckdb_db_path(f"{form}_xbrl")

    taxonomy_archive = datastore.get_taxonomy(form)
    # Process XBRL filings for each year requested
    filings_archives: list[BytesIO] = [
        datastore.get_filings(year, form)
        for year in ferc_to_sqlite.get_data_config(
            dataset=str(form), data_format="xbrl"
        ).years
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
            output_dir=output_path,
            workers=workers,
            batch_size=batch_size,
            loglevel=loglevel,
            logfile=None,
        )
