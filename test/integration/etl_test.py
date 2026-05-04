"""PyTest based testing of the FERC Database & PUDL data package initializations.

Database connections are provided by session-scoped fixtures in ``conftest.py``. The
``prebuilt_outputs`` fixture builds all integration databases via ``dg launch``
before these tests run.
"""

import logging

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

import pudl

logger = logging.getLogger(__name__)


@pytest.mark.order(2)
def test_pudl_engine(pudl_engine: sa.Engine):
    """Verify that key PUDL tables exist and are populated.

    Foreign key validation lives in a separate data-validation test so the nightly
    build can report it independently from the rest of the integration suite.
    """
    assert isinstance(pudl_engine, sa.Engine)
    insp: Inspector = sa.inspect(pudl_engine)
    required_tables = (
        "core_pudl__entity_plants_pudl",
        "core_pudl__entity_utilities_pudl",
    )

    for table_name in required_tables:
        assert table_name in insp.get_table_names()

    with pudl_engine.connect() as connection:
        for table_name in required_tables:
            first_row: int | None = connection.execute(
                sa.select(sa.literal(1)).select_from(sa.table(table_name)).limit(1)
            ).scalar()
            assert first_row is not None, f"Expected {table_name} to contain data."


class TestCsvExtractor:
    """Verify that we can load CSV files as provided via the datastore."""

    @pytest.mark.order(1)
    def test_extract_eia176(self, pudl_datastore_fixture):
        """Spot check extraction of eia176 csv files."""
        extractor = pudl.extract.eia176.Extractor(pudl_datastore_fixture)
        page = "custom"
        year = 2018
        if "company" not in extractor.load_source(page=page, year=year).columns:
            raise AssertionError(f"page {page} not found in datastore for {year}")

    @pytest.mark.order(1)
    def test_extract_eia191(self, pudl_datastore_fixture):
        """Spot check extraction of eia191 csv files."""
        extractor = pudl.extract.eia191.Extractor(pudl_datastore_fixture)
        page = "data"
        year = 2018
        if (
            "working_gas_capacity_(mcf)"
            not in extractor.load_source(page=page, year=year).columns
        ):
            raise AssertionError(f"page {page} not found in datastore for {year}")

    @pytest.mark.order(1)
    def test_extract_eia757a(self, pudl_datastore_fixture):
        """Spot check extraction of eia757a csv files."""
        extractor = pudl.extract.eia757a.Extractor(pudl_datastore_fixture)
        page = "data"
        year = 2017
        if (
            "ng_liquid_storage_capacity"
            not in extractor.load_source(page=page, year=year).columns
        ):
            raise AssertionError(f"page {page} not found in datastore for {year}")


class TestExcelExtractor:
    """Verify that we can lead excel files as provided via the datastore."""

    @staticmethod
    def expected_file_name(extractor, page, year, expected_name):
        """Check if extractor can access files with expected file names."""
        if extractor.source_filename(page, year=year) != expected_name:
            raise AssertionError(
                f"file name for {page} in {year} doesn't match datastore."
            )

    @pytest.mark.order(1)
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

    @pytest.mark.order(1)
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

    @pytest.mark.order(1)
    def test_extract_eia860(self, pudl_datastore_fixture):
        """Spot check extraction of eia860 excel files."""
        extractor = pudl.extract.eia860.Extractor(pudl_datastore_fixture)
        page = "ownership"
        year = 2018
        if "Ownership ID" not in extractor.load_source(page=page, year=year).columns:
            raise AssertionError(f"page {page} not found in datastore for {year}")

    @pytest.mark.order(1)
    def test_extract_eia923(self, pudl_datastore_fixture):
        """Spot check extraction eia923 excel files."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        page = "stocks"
        year = 2018
        if "Oil\nJune" not in extractor.load_source(page=page, year=year).columns:
            raise AssertionError(f"page {page} not found in datastore for {year}")
