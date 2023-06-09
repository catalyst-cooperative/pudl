"""PyTest based testing of the FERC DBF Extraction logic."""

import logging

import pytest
import sqlalchemy as sa

from pudl.extract.dbf import FercDbfReader
from pudl.extract.ferc1 import Ferc1DbfExtractor
from pudl.extract.ferc2 import Ferc2DbfExtractor

logger = logging.getLogger(__name__)


def test_ferc1_dbf2sqlite(ferc1_engine_dbf):
    """Attempt to access the DBF based FERC 1 SQLite DB fixture."""
    assert isinstance(ferc1_engine_dbf, sa.engine.Engine)  # nosec: B101
    assert (  # nosec: B101
        "f1_respondent_id" in sa.inspect(ferc1_engine_dbf).get_table_names()
    )


@pytest.mark.parametrize(
    "extractor_class",
    [
        pytest.param(Ferc1DbfExtractor, id="ferc1"),
        pytest.param(Ferc2DbfExtractor, id="ferc2"),
    ],
)
def test_ferc_schema(ferc_to_sqlite_settings, pudl_datastore_fixture, extractor_class):
    """Check to make sure we aren't missing any old FERC Form N tables or fields.

    Exhaustively enumerate all historical sets of FERC Form N database tables and their
    constituent fields. Check to make sure that the current database definition, based
    on the given reference year and our compilation of the DBF filename to table name
    mapping from 2015, includes every single table and field that appears in the
    historical FERC Form 1 data.
    """
    dataset = extractor_class.DATASET
    dbf_settings = getattr(ferc_to_sqlite_settings, f"{dataset}_dbf_to_sqlite_settings")
    refyear = dbf_settings.refyear
    dbf_reader = FercDbfReader(pudl_datastore_fixture, dataset=dataset)
    ref_archive = dbf_reader.get_archive(year=refyear, data_format="dbf")

    logger.info(f"Checking for new, unrecognized {dataset} tables in {refyear}.")
    table_schemas = ref_archive.get_db_schema()
    for table in table_schemas:
        if table not in dbf_reader.get_table_names():
            raise AssertionError(
                f"New {dataset} table '{table}' in {refyear} "
                f"does not exist in canonical list of tables"
            )

    # Retrieve all supported partitions for the dataset
    descriptor = pudl_datastore_fixture.get_datapackage_descriptor(dataset)
    parts = descriptor.get_partition_filters(data_format="dbf")
    for yr in dbf_settings.years:
        # Check that for each year in the settings, there are partitions defined.
        yr_parts = [p for p in parts if p.get("year", None) == yr]
        if not yr_parts:
            logger.debug(f"Partitions supported by {dataset} are: {parts}")
            raise AssertionError(f"No partitions found for {dataset} in year {yr}.")
        # Check that validation function picks exactly one partition for each year.
        yr_valid_parts = [p for p in yr_parts if extractor_class.is_valid_partition(p)]
        if len(yr_valid_parts) != 1:
            logger.debug(
                f"Filter for {dataset} for year {yr} is: {yr_valid_parts} "
                f"(from {yr_parts})"
            )
            raise AssertionError(
                f"is_valid_partition() function for {dataset} "
                f"should select exactly one partition for year {yr}."
            )
        logger.info(f"Searching for lost {dataset} tables and fields in {yr}.")
        yr_archive = dbf_reader.get_archive(**yr_valid_parts[0])
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
