"""Tests for settings validation."""

from typing import Self

import pytest
from dagster import DagsterInvalidConfigError, Field, build_init_resource_context
from pandas import json_normalize
from pydantic import ValidationError

from pudl.metadata.classes import DataSource
from pudl.resources import dataset_settings
from pudl.settings import (
    DatasetsSettings,
    Eia860mSettings,
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
from pudl.workspace.setup import PudlPaths


class TestGenericDatasetSettings:
    """Test generic dataset behavior."""

    def test_missing_field_error(self: Self):
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

    def test_ref_year(self: Self):
        """Test reference year is within working years."""
        with pytest.raises(ValidationError):
            Ferc1DbfToSqliteSettings(ferc1_to_sqlite_refyear=1990)


class TestFerc1Settings:
    """Test Ferc1 settings validation.

    EIA860 and EIA923 use the same validation functions.
    """

    def test_not_working_year(self: Self):
        """Make sure a validation error is being thrown when given an invalid year."""
        with pytest.raises(ValidationError):
            Ferc1Settings(years=[1901])

    def test_duplicate_sort_years(self: Self):
        """Test years are sorted and deduplicated."""
        with pytest.raises(ValidationError):
            _ = Ferc1Settings(years=[2001, 2001, 2000])

    def test_none_years_raise(self: Self):
        """Test years are sorted and deduplicated."""
        with pytest.raises(ValidationError):
            _ = Ferc1Settings(years=None)

    def test_default_years(self: Self):
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

    def test_not_working_quarter(self: Self):
        """Make sure a validation error is being thrown when given an invalid quarter."""
        with pytest.raises(ValidationError):
            EpaCemsSettings(year_quarters=["1990q4"])

    def test_duplicate_quarters(self: Self):
        """Test year_quarters are deduplicated."""
        with pytest.raises(ValidationError):
            _ = EpaCemsSettings(year_quarters=["1999q4", "1999q4"])

    def test_default_quarters(self: Self):
        """Test all quarters are used as default."""
        returned_settings = EpaCemsSettings()

        expected_year_quarters = DataSource.from_id("epacems").working_partitions[
            "year_quarters"
        ]
        assert expected_year_quarters == returned_settings.year_quarters

    def test_all_year_quarters(self: Self):
        """Test the `all` option for the cems settings."""
        epacems_settings_all = EpaCemsSettings(year_quarters=["all"])
        working_partitions_all = DataSource.from_id("epacems").working_partitions[
            "year_quarters"
        ]
        assert epacems_settings_all.year_quarters == working_partitions_all

    def test_none_quarters_raise(self: Self):
        """Test that setting a required partition to None raises an error."""
        with pytest.raises(ValidationError):
            _ = EpaCemsSettings(quarters=None)


class TestEIA860Settings:
    """Test EIA860 setting validation.

    Most of the validation is covered in TestFerc1Settings.
    """

    def test_860m(self: Self):
        """Test validation error is raised when eia860m date is within 860 years."""
        settings_cls = Eia860Settings
        original_eia80m_year_month = settings_cls.eia860m_year_month
        settings_cls.eia860m_year_month = "2019-11"

        with pytest.raises(ValidationError):
            settings_cls(eia860m=True)
        settings_cls.eia860m_year_month = original_eia80m_year_month


class TestEia860mSettings:
    """Test EIA860m settings."""

    def test_all_year_quarters(self: Self):
        """Test the `all` option for the eia860m settings."""
        settings_all = Eia860mSettings(year_months=["all"]).year_months
        partitions_all = DataSource.from_id("eia860m").working_partitions["year_months"]
        assert settings_all == partitions_all


class TestEiaSettings:
    """Test pydantic model that validates EIA datasets."""

    def test_eia923_dependency(self: Self):
        """Test that there is some overlap between EIA860 and EIA923 data."""
        eia923_settings = Eia923Settings()
        settings = EiaSettings(eia923=eia923_settings)
        data_source = DataSource.from_id("eia860")
        assert settings.eia860
        # assign both EIA form years
        eia860_years = settings.eia860.years
        eia923_years_partition = data_source.working_partitions["years"]
        eia923_years_settings = settings.eia923.years
        # assert that there is some overlap between EIA years
        assert not set(eia860_years).isdisjoint(eia923_years_partition)
        assert not set(eia860_years).isdisjoint(eia923_years_settings)

    def test_eia860_dependency(self: Self):
        """Test that there is some overlap between EIA860 and EIA923 data."""
        eia860_settings = Eia860Settings()
        settings = EiaSettings(eia860=eia860_settings)
        data_source = DataSource.from_id("eia923")
        assert settings.eia923
        # assign both EIA form years
        eia923_years = settings.eia923.years
        eia860_years_partition = data_source.working_partitions["years"]
        eia860_years_settings = settings.eia860.years
        # assert that there is some overlap between EIA years
        assert not set(eia923_years).isdisjoint(eia860_years_partition)
        assert not set(eia923_years).isdisjoint(eia860_years_settings)


class TestDatasetsSettings:
    """Test pydantic model that validates all datasets."""

    def test_default_behavior(self: Self):
        """Make sure all of the years are added if nothing is specified."""
        settings = DatasetsSettings()
        data_source = DataSource.from_id("ferc1")

        expected_years = data_source.working_partitions["years"]
        returned_years = settings.ferc1.years
        assert expected_years == returned_years

        assert settings.eia, "EIA settings were not added."

    def test_glue(self: Self):
        """Test glue settings get added when ferc and eia are requested."""
        settings = DatasetsSettings()
        assert settings.glue, "Glue settings we not added when they should have been."

        assert settings.glue.eia
        assert settings.glue.ferc1

    def test_convert_settings_to_dagster_config(self: Self):
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

    def test_unknown_dataset(self: Self):
        """Test unkown dataset fed to DatasetsSettings."""
        with pytest.raises(ValidationError):
            DatasetsSettings().model_validate({"unknown_data": "data"})

        with pytest.raises(ValidationError):
            EiaSettings().model_validate({"unknown_data": "data"})

    def test_immutability(self: Self):
        """Test immutability config is working correctly."""
        with pytest.raises(ValidationError):
            settings = DatasetsSettings()
            settings.eia = EiaSettings()

        with pytest.raises(ValidationError):
            settings = EiaSettings()
            settings.eia860 = Eia860Settings()


class TestDatasetsSettingsResource:
    """Test the DatasetsSettings dagster resource."""

    def test_invalid_datasource(self: Self):
        """Test an error is thrown when there is an invalid datasource in the config."""
        init_context = build_init_resource_context(
            config={"new_datasource": {"years": [1990]}}
        )
        with pytest.raises(DagsterInvalidConfigError):
            _ = dataset_settings(init_context)

    def test_invalid_field_type(self: Self):
        """Test an error is thrown when there is an incorrect type in the config."""
        init_context = build_init_resource_context(config={"ferc1": {"years": 2021}})
        with pytest.raises(DagsterInvalidConfigError):
            _ = dataset_settings(init_context)

    def test_default_values(self: Self):
        """Test the correct default values are created for dagster config."""
        expected_year_quarters = EpaCemsSettings().year_quarters
        assert (
            dataset_settings.config_schema.default_value["epacems"]["year_quarters"]
            == expected_year_quarters
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
    if list(cems_parts.columns) != ["year_quarter"]:
        raise AssertionError(
            "CEMS paritions should have year_quarter columns only, found:"
            f"{cems_parts}"
        )


@pytest.mark.slow
def test_partitions_for_datasource_table(pudl_etl_settings):
    """Test whether or not we can make the datasource table."""
    ds = Datastore(local_cache_path=PudlPaths().data_dir)
    datasource = pudl_etl_settings.make_datasources_table(ds)
    datasets = pudl_etl_settings.get_datasets().keys()
    if datasource.empty and datasets != 0:
        raise AssertionError(
            "Datasource table is empty with the following datasets in the settings: "
            f"{datasets}"
        )
