"""Generic extractor for all FERC XBRL data."""
import io
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from dagster import Field, Noneable, op
from ferc_xbrl_extractor.cli import run_main

import pudl
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


@op(
    config_schema={
        "clobber": Field(
            bool, description="Clobber existing ferc1 database.", default_value=False
        ),
        "workers": Field(
            Noneable(int),
            description="Specify number of worker processes for parsing XBRL filings.",
            default_value=None,
        ),
        "batch_size": Field(
            int,
            description="Specify number of XBRL instances to be processed at a time (defaults to 50)",
            default_value=50,
        ),
    },
    required_resource_keys={"ferc_to_sqlite_settings", "datastore"},
)
def xbrl2sqlite(context) -> None:
    """Clone the FERC Form 1 XBRL Database to SQLite."""
    output_path = PudlPaths().output_dir
    clobber = context.op_config["clobber"]
    batch_size = context.op_config["batch_size"]
    workers = context.op_config["workers"]
    ferc_to_sqlite_settings = context.resources.ferc_to_sqlite_settings
    datastore = context.resources.datastore
    datastore = FercXbrlDatastore(datastore)

    # Loop through all other forms and perform conversion
    for form in XbrlFormNumber:
        # Get desired settings object
        settings = ferc_to_sqlite_settings.get_xbrl_dataset_settings(form)

        # If no settings for form in question, skip
        if settings is None:
            continue

        if settings.disabled:
            logger.info(f"Dataset ferc{form}_xbrl is disabled, skipping")
            continue

        sql_path = Path(
            urlparse(PudlPaths().sqlite_db(f"ferc{form.value}_xbrl")).path
        ).resolve()

        if sql_path.exists():
            if clobber:
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
            batch_size=batch_size,
            workers=workers,
        )


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
