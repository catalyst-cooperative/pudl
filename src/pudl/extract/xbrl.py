"""Generic extractor for all FERC XBRL data."""
import io
import json
from datetime import date, datetime
from pathlib import Path

import sqlalchemy as sa
from dagster import Field, Noneable, op
from ferc_xbrl_extractor import xbrl
from ferc_xbrl_extractor.instance import InstanceBuilder

import pudl
from pudl.helpers import EnvVar
from pudl.settings import FercGenericXbrlToSqliteSettings, XbrlFormNumber
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


class FercXbrlDatastore:
    """Simple datastore wrapper for accessing ferc1 xbrl resources."""

    def __init__(self, datastore: Datastore):
        """Instantiate datastore wrapper for ferc1 resources."""
        self.datastore = datastore

    def get_taxonomy(self, year: int, form: XbrlFormNumber) -> tuple[io.BytesIO, str]:
        """Returns the path to the taxonomy entry point within the an archive."""
        raw_archive = self.datastore.get_unique_resource(
            f"ferc{form.value}", year=year, data_format="XBRL"
        )

        # Construct path to taxonomy entry point within archive
        taxonomy_date = date(year, 1, 1).isoformat()
        taxonomy_entry_point = f"taxonomy/form{form.value}/{taxonomy_date}/form/form{form.value}/form-{form.value}_{taxonomy_date}.xsd"

        return io.BytesIO(raw_archive), taxonomy_entry_point

    def get_filings(self, year: int, form: XbrlFormNumber) -> list[InstanceBuilder]:
        """Return list of filings from archive."""
        archive = self.datastore.get_zipfile_resource(
            f"ferc{form.value}", year=year, data_format="XBRL"
        )

        # Load RSS feed metadata
        filings = []
        with archive.open("rssfeed") as f:
            metadata = json.load(f)

            # Loop through all filings by a given filer in a given quarter
            # And take the most recent one
            for key, filing_info in metadata.items():
                latest = datetime.min
                for filing_id, info in filing_info.items():
                    # Parse date from 9-tuple
                    published = datetime.fromisoformat(info["published_parsed"])

                    if published > latest:
                        latest = published
                        latest_filing = filing_id

                # Create in memory buffers with file data to be used in conversion
                filings.append(
                    InstanceBuilder(
                        io.BytesIO(archive.open(f"{latest_filing}.xbrl").read()),
                        latest_filing,
                    )
                )

        return filings


def _get_sqlite_engine(
    form_number: int, output_path: Path, clobber: bool
) -> sa.engine.Engine:
    """Create SQLite engine for specified form and drop tables.

    Args:
        form_number: FERC form number.
        output_path: path to PUDL outputs.
        clobber: Flag indicating whether or not to drop tables.
    """
    # Read in the structure of the DB, if it exists
    logger.info(
        f"Dropping the old FERC Form {form_number} XBRL derived SQLite DB if it exists."
    )
    db_path = output_path / f"ferc{form_number}_xbrl.sqlite"

    logger.info(f"Connecting to SQLite at {db_path}...")
    sqlite_engine = sa.create_engine(f"sqlite:///{db_path}")
    logger.info(f"Connected to SQLite at {db_path}!")
    try:
        # So that we can wipe it out
        pudl.helpers.drop_tables(sqlite_engine, clobber=clobber)
    except sa.exc.OperationalError:
        pass

    return sqlite_engine


@op(
    config_schema={
        "pudl_output_path": Field(
            EnvVar(
                env_var="PUDL_OUTPUT",
            ),
            description="Path of directory to store the database in.",
            default_value=None,
        ),
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
    """Clone the FERC Form 1 XBRL Databsae to SQLite."""
    output_path = Path(context.op_config["pudl_output_path"])
    clobber = context.op_config["clobber"]
    batch_size = context.op_config["batch_size"]
    workers = context.op_config["workers"]
    ferc_to_sqlite_settings = context.resources.ferc_to_sqlite_settings
    datastore = datastore = context.resources.datastore
    datastore = FercXbrlDatastore(datastore)

    # Loop through all other forms and perform conversion
    for form in XbrlFormNumber:
        # Get desired settings object
        settings = ferc_to_sqlite_settings.get_xbrl_dataset_settings(form)

        # If no settings for form in question, skip
        if settings is None:
            continue

        sqlite_engine = _get_sqlite_engine(form.value, output_path, clobber)

        convert_form(
            settings,
            form,
            datastore,
            sqlite_engine,
            output_path=output_path,
            batch_size=batch_size,
            workers=workers,
        )


def convert_form(
    form_settings: FercGenericXbrlToSqliteSettings,
    form: XbrlFormNumber,
    datastore: FercXbrlDatastore,
    sqlite_engine: sa.engine.Engine,
    output_path: Path,
    batch_size: int | None = None,
    workers: int | None = None,
) -> None:
    """Clone a single FERC XBRL form to SQLite.

    Args:
        form_settings: Validated settings for converting the desired XBRL form to SQLite.
        form: FERC form number.
        datastore: Instance of a FERC XBRL datastore for retrieving data.
        sqlite_engine: SQLAlchemy connection to mirrored database.
        pudl_settings: Dictionary containing paths and database URLs
            used by PUDL.
        batch_size: Number of XBRL filings to process in a single CPU process.
        workers: Number of CPU processes to create for processing XBRL filings.

    Returns:
        None
    """
    datapackage_path = str(output_path / f"ferc{form.value}_xbrl_datapackage.json")
    metadata_path = str(output_path / f"ferc{form.value}_xbrl_taxonomy_metadata.json")
    # Process XBRL filings for each year requested
    for year in form_settings.years:
        raw_archive, taxonomy_entry_point = datastore.get_taxonomy(year, form)
        xbrl.extract(
            datastore.get_filings(year, form),
            sqlite_engine,
            raw_archive,
            form.value,
            batch_size=batch_size,
            workers=workers,
            datapackage_path=datapackage_path,
            metadata_path=metadata_path,
            archive_file_path=taxonomy_entry_point,
        )
