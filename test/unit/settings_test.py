"""Tests for settings validation."""
import pytest
from pydantic import ValidationError

from pudl.metadata.classes import DataSource
from pudl.settings import (
    DatasetsSettings,
    Eia860Settings,
    Eia923Settings,
    EiaSettings,
    EpaCemsSettings,
    Ferc1DbfToSqliteSettings,
    Ferc1Settings,
    GenericDatasetSettings,
)


class TestGenericDatasetSettings:
    """Test generic dataset behavior."""

    def test_missing_field_error(self):
        """Test GenericDatasetSettings throws error if user forgets to add a field.

        In this case, the required ``data_source`` parameter is missing.
        """
        with pytest.raises(ValidationError):
            working_partitions = {"years": [2001]}
            working_tables = ["table"]

            class Test(GenericDatasetSettings):
                data_source: DataSource(
                    working_partitions=working_partitions, working_tables=working_tables
                )

            Test()


class TestFerc1DbfToSqliteSettings:
    """Test Ferc1DbfToSqliteSettings."""

    def test_ref_year(self):
        """Test reference year is within working years."""
        with pytest.raises(ValidationError):
            Ferc1DbfToSqliteSettings(ferc1_to_sqlite_refyear=1990)


class TestFerc1Settings:
    """Test Ferc1 settings validation.

    EIA860 and EIA923 use the same validation functions.
    """

    def test_not_working_year(self):
        """Make sure a validation error is being thrown when given an invalid year."""
        with pytest.raises(ValidationError):
            Ferc1Settings(years=[1901])

    def test_duplicate_sort_years(self):
        """Test years are sorted and deduplicated."""
        returned_settings = Ferc1Settings(years=[2001, 2001, 2000])
        expected_years = [2000, 2001]

        assert expected_years == returned_settings.years

    def test_default_years(self):
        """Test all years are used as default."""
        returned_settings = Ferc1Settings()

        expected_years = DataSource.from_id("ferc1").working_partitions["years"]
        assert expected_years == returned_settings.years

        dbf_expected_years = [year for year in expected_years if year <= 2020]
        assert dbf_expected_years == returned_settings.dbf_years

        xbrl_expected_years = [year for year in expected_years if year >= 2021]
        assert xbrl_expected_years == returned_settings.xbrl_years

    def test_not_working_table(self):
        """Make sure a validation error is being thrown when given an invalid table."""
        with pytest.raises(ValidationError):
            Ferc1Settings(tables=["fake_table"])

    def test_duplicate_sort_tables(self):
        """Test tables are sorted and deduplicated."""
        returned_settings = Ferc1Settings(
            tables=[
                "plants_pumped_storage_ferc1",
                "plant_in_service_ferc1",
                "plant_in_service_ferc1",
            ]
        )
        expected_tables = ["plant_in_service_ferc1", "plants_pumped_storage_ferc1"]

        assert expected_tables == returned_settings.tables

    def test_default_tables(self):
        """Test all tables are used as default."""
        returned_settings = Ferc1Settings()

        expected_tables = DataSource.from_id("ferc1").get_resource_ids()
        assert expected_tables == returned_settings.tables


class TestEpaCemsSettings:
    """Test EpaCems settings validation."""

    def test_not_working_state(self):
        """Make sure a validation error is being thrown when given an invalid state."""
        with pytest.raises(ValidationError):
            EpaCemsSettings(states=["fake_state"])

    def test_duplicate_sort_states(self):
        """Test states are sorted and deduplicated."""
        returned_settings = EpaCemsSettings(states=["CA", "CA", "AL"])
        expected_states = ["AL", "CA"]

        assert expected_states == returned_settings.states

    def test_default_states(self):
        """Test all states are used as default."""
        returned_settings = EpaCemsSettings()

        expected_states = DataSource.from_id("epacems").working_partitions["states"]
        assert expected_states == returned_settings.states

    def test_all_states(self):
        """Test all states are used as default."""
        returned_settings = EpaCemsSettings(states=["all"])

        expected_states = DataSource.from_id("epacems").working_partitions["states"]
        assert expected_states == returned_settings.states


class TestEIA860Settings:
    """Test EIA860 setting validation.

    Most of the validation is covered in TestFerc1Settings.
    """

    def test_860m(self):
        """Test validation error is raised when eia860m date is within 860 years."""
        settings_cls = Eia860Settings
        settings_cls.eia860m_date = "2019-11"

        with pytest.raises(ValidationError):
            settings_cls(eia860m=True)


class TestEiaSettings:
    """Test pydantic model that validates EIA datasets."""

    def test_eia923_dependency(self):
        """Test 860 is added if 923 is specified and 860 is not."""
        eia923_settings = Eia923Settings()
        settings = EiaSettings(eia923=eia923_settings)
        data_source = DataSource.from_id("eia860")

        assert settings.eia860

        assert settings.eia860.years == data_source.working_partitions["years"]
        assert settings.eia860.tables == data_source.get_resource_ids()

    def test_eia860_dependency(self):
        """Test 923 tables are added to eia860 if 923 is not specified."""
        eia860_settings = Eia860Settings()
        settings = EiaSettings(eia860=eia860_settings)

        expected_tables = ["boiler_fuel_eia923", "generation_eia923"]

        assert settings.eia923.tables == expected_tables
        assert settings.eia923.years == eia860_settings.years


class TestDatasetsSettings:
    """Test pydantic model that validates all datasets."""

    def test_default_behavior(self):
        """Make sure all of the years and tables are added if nothing is specified."""
        settings = DatasetsSettings()
        data_source = DataSource.from_id("ferc1")

        expected_years = data_source.working_partitions["years"]
        returned_years = settings.ferc1.years
        assert expected_years == returned_years

        expected_tables = data_source.get_resource_ids()
        returned_tables = settings.ferc1.tables
        assert expected_tables == returned_tables

        assert settings.eia, "EIA settings were not added."

    def test_glue(self):
        """Test glue settings get added when ferc and eia are requested."""
        settings = DatasetsSettings()
        assert settings.glue, "Glue settings we not added when they should have been."

        assert settings.glue.eia
        assert settings.glue.ferc1


class TestGlobalConfig:
    """Test global pydantic model config works."""

    def test_unknown_dataset(self):
        """Test unkown dataset fed to DatasetsSettings."""
        with pytest.raises(ValidationError):
            DatasetsSettings().parse_obj({"unknown_data": "data"})

        with pytest.raises(ValidationError):
            EiaSettings().parse_obj({"unknown_data": "data"})

    def test_immutability(self):
        """Test immutability config is working correctly."""
        with pytest.raises(TypeError):
            settings = DatasetsSettings()
            settings.eia = EiaSettings()

        with pytest.raises(TypeError):
            settings = EiaSettings()
            settings.eia860 = Eia860Settings()
