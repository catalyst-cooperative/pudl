"""Generic extractor for all FERC XBRL data."""
import io
from collections.abc import Callable
from datetime import date
from pathlib import Path

from dagster import op
from ferc_xbrl_extractor.cli import run_main

import pudl
from pudl.resources import RuntimeSettings
from pudl.settings import FercGenericXbrlToSqliteSettings, XbrlFormNumber
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


class FercXbrlDatastore:
    """Simple datastore wrapper for accessing ferc1 xbrl resources."""

    def __init__(self, datastore: Datastore):
        """Instantiate datastore wrapper for ferc1 resources."""
        self.datastore = datastore

    def get_taxonomy(self, year: int, form: XbrlFormNumber) -> tuple[io.BytesIO, str]:
        """Returns the path to the taxonomy entry point within the an archive."""
        taxonomy_dates = {2021: date(2022, 1, 1), 2022: date(2022, 1, 1)}
        taxonomy_date = taxonomy_dates[year]
        raw_archive = self.datastore.get_unique_resource(
            f"ferc{form.value}",
            year=taxonomy_date.year,
            data_format="xbrl_taxonomy",
        )

        taxonomy_entry_point = f"taxonomy/form{form.value}/{taxonomy_date}/form/form{form.value}/form-{form.value}_{taxonomy_date.isoformat()}.xsd"

        return io.BytesIO(raw_archive), taxonomy_entry_point

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
            "ferc_to_sqlite_settings",
            "datastore",
            "runtime_settings",
        },
        tags={"data_format": "xbrl", "dataset": f"ferc{form.value}"},
    )
    def inner_op(context) -> None:
        output_path = PudlPaths().output_dir
        rs: RuntimeSettings = context.resources.runtime_settings
        settings = context.resources.ferc_to_sqlite_settings.get_xbrl_dataset_settings(
            form
        )
        datastore = FercXbrlDatastore(context.resources.datastore)

        logger.info(f"====== xbrl2sqlite runtime_settings: {rs}")
        if settings is None or settings.disabled:
            logger.info(
                f"Skipping dataset ferc{form.value}_xbrl: no config or is disabled."
            )
            return

        sql_path = PudlPaths().sqlite_db_path(f"ferc{form.value}_xbrl")
        if sql_path.exists():
            if rs.clobber:
                sql_path.unlink()
            else:
                raise RuntimeError(
                    f"Found existing DB at {sql_path} and clobber was set to False. Aborting."
                )

        convert_form(
            settings,
            form,
            datastore,
            output_path=output_path,
            sql_path=sql_path,
            batch_size=rs.xbrl_batch_size,
            workers=rs.xbrl_num_workers,
        )

    return inner_op


def convert_form(
    form_settings: FercGenericXbrlToSqliteSettings,
    form: XbrlFormNumber,
    datastore: FercXbrlDatastore,
    output_path: Path,
    sql_path: Path,
    batch_size: int | None = None,
    workers: int | None = None,
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
    datapackage_path = str(output_path / f"ferc{form.value}_xbrl_datapackage.json")
    metadata_path = str(output_path / f"ferc{form.value}_xbrl_taxonomy_metadata.json")
    # Process XBRL filings for each year requested
    for year in form_settings.years:
        taxonomy_archive, taxonomy_entry_point = datastore.get_taxonomy(year, form)
        filings_archive = datastore.get_filings(year, form)
        # if we set clobber=True, clobbers on *every* call to run_main;
        # we already delete the existing base on `clobber=True` in `xbrl2sqlite`
        run_main(
            instance_path=filings_archive,
            sql_path=sql_path,
            clobber=False,
            taxonomy=taxonomy_archive,
            entry_point=taxonomy_entry_point,
            form_number=form.value,
            metadata_path=metadata_path,
            datapackage_path=datapackage_path,
            workers=workers,
            batch_size=batch_size,
            loglevel="INFO",
            logfile=None,
        )
