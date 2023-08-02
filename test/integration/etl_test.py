"""PyTest based testing of the FERC Database & PUDL data package initializations.

This module also contains fixtures for returning connections to the databases. These
connections can be either to the live databases for post-ETL testing or to new temporary
databases, which are created from scratch and dropped after the tests have completed.
"""
import logging

import pandas as pd
import sqlalchemy as sa
from dagster import build_init_resource_context

import pudl

logger = logging.getLogger(__name__)


def test_pudl_engine(pudl_engine, pudl_sql_io_manager, check_foreign_keys):
    """Get pudl_engine and do basic inspection.

    By default the foreign key checks are not enabled in pudl.sqlite. This test will
    check if there are any foregin key errors if check_foreign_keys is True.
    """
    assert isinstance(pudl_engine, sa.engine.Engine)  # nosec: B101
    insp = sa.inspect(pudl_engine)
    assert "plants_pudl" in insp.get_table_names()  # nosec: B101
    assert "utilities_pudl" in insp.get_table_names()  # nosec: B101

    if check_foreign_keys:
        # Raises ForeignKeyErrors if there are any
        pudl_sql_io_manager.check_foreign_keys()


def test_ferc1_xbrl2sqlite(ferc1_engine_xbrl, ferc1_xbrl_taxonomy_metadata):
    """Attempt to access the XBRL based FERC 1 SQLite DB & XBRL taxonomy metadata.

    We're testing both the SQLite & JSON taxonomy here because they are generated
    together by the FERC 1 XBRL ETL.
    """
    # Does the database exist, and contain a table we expect it to contain?
    assert isinstance(ferc1_engine_xbrl, sa.engine.Engine)  # nosec: B101
    assert (  # nosec: B101
        "identification_001_duration" in sa.inspect(ferc1_engine_xbrl).get_table_names()
    )

    # Has the metadata we've read in from JSON contain a long list of entities?
    assert isinstance(ferc1_xbrl_taxonomy_metadata, dict)  # nosec: B101
    assert "plants_steam_ferc1" in ferc1_xbrl_taxonomy_metadata.keys()  # nosec: B101
    assert len(ferc1_xbrl_taxonomy_metadata) > 10  # nosec: B101
    assert len(ferc1_xbrl_taxonomy_metadata) < 100  # nosec: B101

    # Can we normalize that list of entities and find data in it that we expect?
    df = pd.json_normalize(
        ferc1_xbrl_taxonomy_metadata["plant_in_service_ferc1"]["instant"]
    )
    assert (  # nosec: B101
        df.loc[
            df.name == "reactor_plant_equipment_nuclear_production", "balance"
        ].values
        == "debit"
    )
    assert (  # nosec: B101
        df.loc[
            df.name == "reactor_plant_equipment_nuclear_production",
            "references.account",
        ].values
        == "322"
    )


class TestExcelExtractor:
    """Verify that we can lead excel files as provided via the datastore."""

    @staticmethod
    def expected_file_name(extractor, page, year, expected_name):
        """Check if extractor can access files with expected file names."""
        if extractor.excel_filename(page, year=year) != expected_name:
            raise AssertionError(
                f"file name for {page} in {year} doesn't match datastore."
            )

    def test_excel_filename_eia860(self, pudl_datastore_fixture):
        """Spot check eia860 extractor gets the correct excel sheet names."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        self.expected_file_name(
            extractor=extractor,
            page="boiler_generator_assn",
            year=2011,
            expected_name="EnviroAssocY2011.xlsx",
        )
        self.expected_file_name(
            extractor=extractor,
            page="generator_retired",
            year=2016,
            expected_name="3_1_Generator_Y2016.xlsx",
        )
        self.expected_file_name(
            extractor=extractor,
            page="utility",
            year=2018,
            expected_name="1___Utility_Y2018.xlsx",
        )
        self.expected_file_name(
            extractor=extractor, page="plant", year=2003, expected_name="PLANTY03.DBF"
        )

    def test_excel_filename_eia923(self, pudl_datastore_fixture):
        """Spot check eia923 extractor gets the correct excel sheet names."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        self.expected_file_name(
            extractor=extractor,
            page="generation_fuel",
            year=2009,
            expected_name="EIA923 SCHEDULES 2_3_4_5 M Final 2009 REVISED 05252011.XLS",
        )
        self.expected_file_name(
            extractor=extractor,
            page="fuel_receipts_costs",
            year=2019,
            expected_name="EIA923_Schedules_2_3_4_5_M_12_2019_Final_Revision.xlsx",
        )
        self.expected_file_name(
            extractor=extractor,
            page="boiler_fuel",
            year=2012,
            expected_name="EIA923_Schedules_2_3_4_5_M_12_2012_Final_Revision.xlsx",
        )

    def test_extract_eia860(self, pudl_datastore_fixture):
        """Spot check extraction of eia860 excel files."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        page = "ownership"
        year = 2018
        if (
            "Ownership"
            not in extractor.load_excel_file(page="ownership", year=2018).sheet_names
        ):
            raise AssertionError(f"page {page} not found in datastore for {year}")

    def test_extract_eia923(self, pudl_datastore_fixture):
        """Spot check extraction eia923 excel files."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        page = "stocks"
        year = 2018
        if (
            "Page 3 Boiler Fuel Data"
            not in extractor.load_excel_file(page=page, year=year).sheet_names
        ):
            raise AssertionError(f"page {page} not found in datastore for {year}")


class TestFerc1ExtractDebugFunctions:
    """Verify the ferc1 extraction debug functions are working properly."""

    def test_extract_dbf(self, ferc1_engine_dbf):
        """Test extract_dbf."""
        years = [2020, 2021]  # add desired years here
        configured_dataset_settings = {"ferc1": {"years": years}}

        dataset_init_context = build_init_resource_context(
            config=configured_dataset_settings
        )
        configured_dataset_settings = pudl.resources.dataset_settings(
            dataset_init_context
        )

        ferc1_dbf_raw_dfs = pudl.extract.ferc1.extract_dbf(configured_dataset_settings)

        for table_name, df in ferc1_dbf_raw_dfs.items():
            assert (df.report_year >= 2020).all() and (
                df.report_year < 2022
            ).all(), f"Unexpected years found in table: {table_name}"

    def test_extract_xbrl(self, ferc1_engine_dbf):
        """Test extract_xbrl."""
        years = [2021]  # add desired years here
        configured_dataset_settings = {"ferc1": {"years": years}}

        dataset_init_context = build_init_resource_context(
            config=configured_dataset_settings
        )
        configured_dataset_settings = pudl.resources.dataset_settings(
            dataset_init_context
        )

        ferc1_xbrl_raw_dfs = pudl.extract.ferc1.extract_xbrl(
            configured_dataset_settings
        )

        for table_name, xbrl_tables in ferc1_xbrl_raw_dfs.items():
            for table_type, df in xbrl_tables.items():
                # Some raw xbrl tables are empty
                if not df.empty and table_type == "duration":
                    assert (df.report_year >= 2021).all() and (
                        df.report_year < 2022
                    ).all(), f"Unexpected years found in table: {table_name}"
