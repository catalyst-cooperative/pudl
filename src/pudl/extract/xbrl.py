"""Generic extractor for all FERC XBRL data."""
import io
import json
from datetime import date, datetime

import sqlalchemy as sa
from ferc_xbrl_extractor import xbrl
from ferc_xbrl_extractor.instance import InstanceBuilder

import pudl
from pudl.settings import (
    FercGenericXbrlToSqliteSettings,
    FercToSqliteSettings,
    XbrlFormNumber,
)
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


class FercXbrlDatastore:
    """Simple datastore wrapper for accessing ferc1 xbrl resources."""

    def __init__(self, datastore: Datastore):
        """Instantiate datastore wrapper for ferc1 resources."""
        self.datastore = datastore

    def get_taxonomy(self, year: int, form: XbrlFormNumber):
        """Returns the path to the taxonomy entry point within the an archive."""
        raw_archive = self.datastore.get_unique_resource(
            f"ferc{form.value}", year=year, data_format="XBRL"
        )

        # Construct path to taxonomy entry point within archive
        taxonomy_date = date(year, 1, 1).isoformat()
        taxonomy_entry_point = f"taxonomy/form{form.value}/{taxonomy_date}/form/form{form.value}/form-{form.value}_{taxonomy_date}.xsd"

        return io.BytesIO(raw_archive), taxonomy_entry_point

    def get_filings(self, year: int, form: XbrlFormNumber):
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
                        latest_filing = f"{filing_id}.xbrl"

                # Create in memory buffers with file data to be used in conversion
                filings.append(
                    InstanceBuilder(
                        io.BytesIO(archive.open(latest_filing).read()), filing_id
                    )
                )

        return filings


def _get_sqlite_engine(
    form_number: int, pudl_settings: dict, clobber: bool
) -> sa.engine.Engine:
    """Create SQLite engine for specified form and drop tables.

    Args:
        form_number: FERC form number.
        pudl_settings: Dictionary of settings.
        clobber: Flag indicating whether or not to drop tables.
    """
    # Read in the structure of the DB, if it exists
    logger.info(
        f"Dropping the old FERC Form {form_number} XBRL derived SQLite DB if it exists."
    )
    sqlite_engine = sa.create_engine(pudl_settings[f"ferc{form_number}_xbrl_db"])
    try:
        # So that we can wipe it out
        pudl.helpers.drop_tables(sqlite_engine, clobber=clobber)
    except sa.exc.OperationalError:
        pass

    return sqlite_engine


def xbrl2sqlite(
    ferc_to_sqlite_settings: FercToSqliteSettings = FercToSqliteSettings(),
    pudl_settings: dict = None,
    clobber: bool = False,
    datastore: Datastore = None,
    batch_size: int | None = None,
    workers: int | None = None,
):
    """Clone the FERC Form 1 XBRL Databsae to SQLite.

    Args:
        ferc_to_sqlite_settings: Object containing Ferc to SQLite validated
            settings.
        pudl_settings: Dictionary containing paths and database URLs
            used by PUDL.
        clobber: Flag indicating whether or not to drop tables.
        datastore: Instance of a datastore to access the resources.
        batch_size: Number of XBRL filings to process in a single CPU process.
        workers: Number of CPU processes to create for processing XBRL filings.

    Returns:
        None
    """
    datastore = FercXbrlDatastore(datastore)

    # Loop through all other forms and perform conversion
    for form in XbrlFormNumber:
        # Get desired settings object
        settings = ferc_to_sqlite_settings.get_xbrl_dataset_settings(form)

        # If no settings for form in question, skip
        if settings is None:
            continue

        sqlite_engine = _get_sqlite_engine(form.value, pudl_settings, clobber)

        convert_form(
            settings,
            form,
            datastore,
            sqlite_engine,
            pudl_settings=pudl_settings,
            batch_size=batch_size,
            workers=workers,
        )


def convert_form(
    form_settings: FercGenericXbrlToSqliteSettings,
    form: XbrlFormNumber,
    datastore: FercXbrlDatastore,
    sqlite_engine: sa.engine.Engine,
    pudl_settings: dict = None,
    batch_size: int | None = None,
    workers: int | None = None,
):
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
    # Process XBRL filings for each year requested
    for year in form_settings.years:
        raw_archive, taxonomy_entry_point = datastore.get_taxonomy(year, form)
        xbrl.extract(
            datastore.get_filings(year, form),
            sqlite_engine,
            raw_archive,
            form.value,
            requested_tables=form_settings.tables,
            batch_size=batch_size,
            workers=workers,
            datapackage_path=pudl_settings[f"ferc{form.value}_xbrl_descriptor"],
            metadata_path=pudl_settings[f"ferc{form.value}_taxonomy_metadata"],
            archive_file_path=taxonomy_entry_point,
        )
