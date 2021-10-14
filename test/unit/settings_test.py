"""Tests for settings validation."""
import unittest

from pydantic import ValidationError

from pudl.settings import (DatasetsSettings, Eia860Settings, Eia923Settings,
                           Ferc1Settings)


class TestFerc1Settings(unittest.TestCase):
    """
    Test Ferc1 settings validation.

    EIA860 and EIA923 use the same validation functions.
    """

    def test_not_working_year(self):
        """Make sure a validation error is being thrown when given an invalid year."""
        with self.assertRaises(ValidationError):
            Ferc1Settings(years=[1901])

    def test_duplicate_sort_years(self):
        """Test years are sorted and deduplicated."""
        returned_settings = Ferc1Settings(years=[2001, 2001, 2000])
        expected_years = [2000, 2001]

        self.assertListEqual(expected_years, returned_settings.years)

    def test_default_years(self):
        """Test all years are used as default."""
        returned_settings = Ferc1Settings()

        expected_years = Ferc1Settings.working_years
        self.assertListEqual(expected_years, returned_settings.years)

    def test_not_working_table(self):
        """Make sure a validation error is being thrown when given an invalid table."""
        with self.assertRaises(ValidationError):
            Ferc1Settings(tables=["fake_table"])

    def test_duplicate_sort_tables(self):
        """Test tables are sorted and deduplicated."""
        returned_settings = Ferc1Settings(
            tables=["plants_pumped_storage_ferc1", "plant_in_service_ferc1", "plant_in_service_ferc1"])
        expected_tables = ["plant_in_service_ferc1", "plants_pumped_storage_ferc1"]

        self.assertListEqual(expected_tables, returned_settings.tables)

    def test_default_tables(self):
        """Test all tables are used as default."""
        returned_settings = Ferc1Settings()

        expected_tables = Ferc1Settings.working_tables
        self.assertListEqual(expected_tables, returned_settings.tables)


class TestEIA860Settings(unittest.TestCase):
    """
    Test EIA860 setting validation.

    Most of the validation is covered in TestFerc1Settings.
    """

    def test_860m(self):
        """Test validation error is raised when eia860m date is within 860 years."""
        settings_cls = Eia860Settings
        settings_cls.eia860m_date = "2019-11"

        with self.assertRaises(ValidationError):
            settings_cls(eia860m=True)


class TestDatasetsSettings(unittest.TestCase):
    """Test pydantic model that validates all datasets."""

    def test_default_behavior(self):
        """Make sure all of the years and tables are added if nothing is specified."""
        settings = DatasetsSettings()

        expected_years = Ferc1Settings.working_years
        returned_years = settings.ferc1.years
        self.assertListEqual(expected_years, returned_years)

        expected_tables = Ferc1Settings.working_tables
        returned_tables = settings.ferc1.tables
        self.assertListEqual(expected_tables, returned_tables)

        expected_years = Eia860Settings.working_years
        returned_years = settings.eia860.years
        self.assertListEqual(expected_years, returned_years)

        expected_tables = Eia860Settings.working_tables
        returned_tables = settings.eia860.tables
        self.assertListEqual(expected_tables, returned_tables)

        expected_years = Eia923Settings.working_years
        returned_years = settings.eia923.years
        self.assertListEqual(expected_years, returned_years)

        expected_tables = Eia923Settings.working_tables
        returned_tables = settings.eia923.tables
        self.assertListEqual(expected_tables, returned_tables)

    def test_glue(self):
        """Test glue settings get added when ferc and eia are requested."""
        settings = DatasetsSettings()
        assert settings.glue, "Glue settings we not added when they should have been."

        assert settings.glue.eia
        assert settings.glue.ferc1

    def test_eia923_dependency(self):
        """Test 860 is added if 923 is specified and 860 is not."""
        eia923_settings = Eia923Settings()
        settings = DatasetsSettings(eia923=eia923_settings)

        assert settings.eia860

        self.assertListEqual(settings.eia860.years, Eia860Settings.working_years)
        self.assertListEqual(settings.eia860.tables, Eia860Settings.working_tables)

    def test_eia860_dependency(self):
        """Test 923 tables are added to eia860 if 923 is not specified."""
        eia860_settings = Eia860Settings()
        settings = DatasetsSettings(eia860=eia860_settings)

        expected_tables = ['boiler_fuel_eia923', 'generation_eia923']

        self.assertListEqual(settings.eia923.tables, expected_tables)
        self.assertListEqual(settings.eia923.years, eia860_settings.years)

    @unittest.skip("Pydantic allows you to instantiate a Model with an unkown arg.")
    def test_unknown_dataset(self):
        """Test unkown dataset fed to DatasetsSettings."""
        with self.assertRaises(ValidationError):
            DatasetsSettings().parse_obj({"unknown_data": "data"})
