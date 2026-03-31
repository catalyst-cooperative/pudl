"""Integration tests for Excel-based extractors."""

import pudl


class TestExcelExtractor:
    """Verify that we can load Excel files as provided via the datastore."""

    @staticmethod
    def expected_file_name(extractor, page, year, expected_name):
        """Check if extractor can access files with expected file names."""
        if extractor.source_filename(page, year=year) != expected_name:
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
        if "Ownership ID" not in extractor.load_source(page=page, year=year).columns:
            raise AssertionError(f"page {page} not found in datastore for {year}")

    def test_extract_eia923(self, pudl_datastore_fixture):
        """Spot check extraction eia923 excel files."""
        extractor = pudl.extract.eia923.Extractor(pudl_datastore_fixture)
        page = "stocks"
        year = 2018
        if "Oil\nJune" not in extractor.load_source(page=page, year=year).columns:
            raise AssertionError(f"page {page} not found in datastore for {year}")
