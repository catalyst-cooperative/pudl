"""PyTest based testing of the FERC DBF Extraction logic."""

import logging

import sqlalchemy as sa

from pudl.extract.dbf import FercDbfReader

logger = logging.getLogger(__name__)


def test_ferc1_dbf2sqlite(ferc1_engine_dbf):
    """Attempt to access the DBF based FERC 1 SQLite DB fixture."""
    assert isinstance(ferc1_engine_dbf, sa.engine.Engine)  # nosec: B101
    assert (  # nosec: B101
        "f1_respondent_id" in sa.inspect(ferc1_engine_dbf).get_table_names()
    )


def test_ferc1_schema(ferc_to_sqlite_settings, pudl_datastore_fixture):
    """Check to make sure we aren't missing any old FERC Form 1 tables or fields.

    Exhaustively enumerate all historical sets of FERC Form 1 database tables and their
    constituent fields. Check to make sure that the current database definition, based
    on the given reference year and our compilation of the DBF filename to table name
    mapping from 2015, includes every single table and field that appears in the
    historical FERC Form 1 data.
    """
    ferc1_dbf_settings = ferc_to_sqlite_settings.ferc1_dbf_to_sqlite_settings
    refyear = ferc1_dbf_settings.refyear
    dbf_reader = FercDbfReader(pudl_datastore_fixture, dataset="ferc1")
    ref_archive = dbf_reader.get_archive(year=refyear, data_format="dbf")

    logger.info(f"Checking for new, unrecognized FERC1 tables in {refyear}.")
    table_schemas = ref_archive.get_db_schema()
    for table in table_schemas:
        if table not in dbf_reader.get_table_names():
            raise AssertionError(
                f"New FERC Form 1 table '{table}' in {refyear} "
                f"does not exist in 2015 list of tables"
            )
    for yr in ferc1_dbf_settings.years:
        logger.info(f"Searching for lost FERC1 tables and fields in {yr}.")
        # Some early years might need part=None to eliminate split-respondent
        # strange archives, but let's assume this is not needed here for now.
        yr_archive = dbf_reader.get_archive(year=yr, data_format="dbf")
        for table in yr_archive.get_db_schema():
            if table not in dbf_reader.get_table_names():
                raise AssertionError(
                    f"Long lost FERC1 table: '{table}' found in year {yr}. "
                    f"Refyear: {refyear}"
                )
            # Check that legacy fields have not been lost (i.e. they're present in refyear)
            yr_columns = yr_archive.get_table_schema(table).get_column_names()
            ref_columns = ref_archive.get_table_schema(table).get_column_names()
            unknowns = yr_columns.difference(ref_columns)
            if unknowns:
                raise AssertionError(
                    f"Long lost FERC1 fields '{sorted(unknowns)}' found in table "
                    f"'{table}' from year {yr}. "
                    f"Refyear: {refyear}"
                )
