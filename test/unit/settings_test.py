"""Tests for settings validation."""

import pytest
from dagster import DagsterInvalidConfigError, Field, build_init_resource_context
from pandas import json_normalize
from pydantic import ValidationError

from pudl.metadata.classes import DataSource
from pudl.resources import dataset_settings
from pudl.settings import (
    DatasetsSettings,
    Eia860Settings,
    Eia923Settings,
    EiaSettings,
    EpaCemsSettings,
    Ferc1DbfToSqliteSettings,
    Ferc1Settings,
    GenericDatasetSettings,
    _convert_settings_to_dagster_config,
)
from pudl.workspace.datastore import Datastore


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
        original_eia80m_date = settings_cls.eia860m_date
        settings_cls.eia860m_date = "2019-11"

        with pytest.raises(ValidationError):
            settings_cls(eia860m=True)
        settings_cls.eia860m_date = original_eia80m_date


class TestEiaSettings:
    """Test pydantic model that validates EIA datasets."""

    def test_eia923_dependency(self):
        """Test 860 is added if 923 is specified and 860 is not."""
        eia923_settings = Eia923Settings()
        settings = EiaSettings(eia923=eia923_settings)
        data_source = DataSource.from_id("eia860")

        assert settings.eia860

        assert settings.eia860.years == data_source.working_partitions["years"]

    def test_eia860_dependency(self):
        """Test 923 tables are added to eia860 if 923 is not specified."""
        eia860_settings = Eia860Settings()
        settings = EiaSettings(eia860=eia860_settings)
        assert settings.eia923.years == eia860_settings.years


class TestDatasetsSettings:
    """Test pydantic model that validates all datasets."""

    def test_default_behavior(self):
        """Make sure all of the years are added if nothing is specified."""
        settings = DatasetsSettings()
        data_source = DataSource.from_id("ferc1")

        expected_years = data_source.working_partitions["years"]
        returned_years = settings.ferc1.years
        assert expected_years == returned_years

        assert settings.eia, "EIA settings were not added."

    def test_glue(self):
        """Test glue settings get added when ferc and eia are requested."""
        settings = DatasetsSettings()
        assert settings.glue, "Glue settings we not added when they should have been."

        assert settings.glue.eia
        assert settings.glue.ferc1

    def test_convert_settings_to_dagster_config(self):
        """Test conversion of dictionary to Dagster config."""
        dct = {
            "eia": {
                "eia860": {"years": [2021, 2022]},
                "eia923": {"years": [2021, 2022]},
            }
        }
        expected_dct = {
            "eia": {
                "eia860": {"years": Field(list, default_value=[2021, 2022])},
                "eia923": {"years": Field(list, default_value=[2021, 2022])},
            }
        }

        _convert_settings_to_dagster_config(dct)
        assert dct.keys() == expected_dct.keys()
        assert dct["eia"].keys() == expected_dct["eia"].keys()
        assert isinstance(dct["eia"]["eia860"]["years"], Field)
        assert isinstance(dct["eia"]["eia923"]["years"], Field)


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


class TestDatasetsSettingsResource:
    """Test the DatasetsSettings dagster resource."""

    def test_invalid_datasource(self):
        """Test an error is thrown when there is an invalid datasource in the config."""
        init_context = build_init_resource_context(
            config={"new_datasource": {"years": [1990]}}
        )
        with pytest.raises(DagsterInvalidConfigError):
            _ = dataset_settings(init_context)

    def test_invalid_field_type(self):
        """Test an error is thrown when there is an incorrect type in the config."""
        init_context = build_init_resource_context(config={"ferc1": {"years": 2021}})
        with pytest.raises(DagsterInvalidConfigError):
            _ = dataset_settings(init_context)

    def test_default_values(self):
        """Test the correct default values are created for dagster config."""
        expected_states = EpaCemsSettings().states
        assert (
            dataset_settings.config_schema.default_value["epacems"]["states"]
            == expected_states
        )


def test_partitions_with_json_normalize(pudl_etl_settings):
    """Ensure the FERC1 and CEMS partitions normalize."""
    datasets = pudl_etl_settings.get_datasets()

    ferc_parts = json_normalize(datasets["ferc1"].partitions)
    if list(ferc_parts.columns) != ["year"]:
        raise AssertionError(
            "FERC1 paritions should have year and state columns only, found:"
            f"{ferc_parts}"
        )

    cems_parts = json_normalize(datasets["epacems"].partitions)
    if list(cems_parts.columns) != ["year", "state"]:
        raise AssertionError(
            "CEMS paritions should have year and state columns only, found:"
            f"{cems_parts}"
        )


def test_partitions_for_datasource_table(pudl_settings_fixture, pudl_etl_settings):
    """Test whether or not we can make the datasource table."""
    ds = Datastore(local_cache_path=pudl_settings_fixture["data_dir"])
    datasource = pudl_etl_settings.make_datasources_table(ds)
    datasets = pudl_etl_settings.get_datasets().keys()
    if datasource.empty and datasets != 0:
        raise AssertionError(
            "Datasource table is empty with the following datasets in the settings: "
            f"{datasets}"
        )
