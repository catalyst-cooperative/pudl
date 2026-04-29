"""Integration tests for CSV-based extractors."""

import pudl


class TestCsvExtractor:
    """Verify that we can load CSV files as provided via the datastore."""

    def test_extract_eia176(self, pudl_datastore_fixture):
        """Spot check extraction of eia176 csv files."""
        extractor = pudl.extract.eia176.Extractor(pudl_datastore_fixture)
        page = "custom"
        year = 2018
        if "company" not in extractor.load_source(page=page, year=year).columns:
            raise AssertionError(f"page {page} not found in datastore for {year}")

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
