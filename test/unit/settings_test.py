"""Tests for settings validation."""

import inspect
from typing import Self

import pandas as pd
import pytest
from dagster import DagsterInvalidConfigError, Field, build_init_resource_context
from pandas import json_normalize
from pydantic import BaseModel, ValidationError

import pudl.settings as _settings_module
from pudl.metadata.classes import DataSource
from pudl.resources import dataset_settings
from pudl.settings import (
    DatasetsSettings,
    Eia860mSettings,
    Eia860Settings,
    Eia923Settings,
    EiaSettings,
    EpaCemsSettings,
    EtlSettings,
    Ferc1DbfToSqliteSettings,
    Ferc1Settings,
    Ferc1XbrlToSqliteSettings,
    FercToSqliteSettings,
    GenericDatasetSettings,
    GridPathRAToolkitSettings,
    _convert_settings_to_dagster_config,
    create_dagster_config,
)


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
                data_source: DataSource = DataSource(
                    working_partitions=working_partitions,
                    working_tables=working_tables,
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


class TestEia860Settings:
    """Test EIA860 setting validation."""

    def test_eia860_years_overlap_eia860m_years(self: Self):
        """Test validation error is raised when eia860m date is within eia860 years."""
        # Identify the last valid EIA-860 year:
        max_eia860_year = max(Eia860Settings().years)
        # Use that year to construct an EIA-860M year that overlaps the EIA-860 years:
        bad_eia860m_year_month = f"{max_eia860_year}-01"

        # Attempt to construct an EIA-860 settings object with an EIA-860M year that
        # overlaps the EIA-860 years, which should result in a ValidationError:
        with pytest.raises(ValidationError):
            _ = Eia860Settings(
                eia860m=True,
                years=[max_eia860_year],
                eia860m_year_months=[bad_eia860m_year_month],
            )

    def test_eia860m_years_overlap_eia860m_years(self: Self):
        """Test validation error is raised when eia860m years overlap."""
        max_eia860_year = max(Eia860Settings().years)
        acceptable_eia860m_year = max_eia860_year + 1
        bad_eia860m_year_months = [
            f"{acceptable_eia860m_year}-01",
            f"{acceptable_eia860m_year}-02",
        ]
        with pytest.raises(ValidationError):
            _ = Eia860Settings(
                eia860m=True,
                years=[max_eia860_year],
                eia860m_year_months=bad_eia860m_year_months,
            )

    def test_eia860m_after_eia860(self: Self):
        """Test the creation of eia860m_year_months values."""
        settings_eia860 = Eia860Settings()
        max_eia860 = max(DataSource.from_id("eia860").working_partitions["years"])
        max_eia860m = pd.to_datetime(
            max(DataSource.from_id("eia860m").working_partitions["year_months"])
        ).year
        settings_eia860m_years = [
            pd.to_datetime(date).year for date in settings_eia860.eia860m_year_months
        ]
        # Assert that the default eia860m settings years are a complete range between the
        # year after the last available eia860 year and the latest available eia860m year
        assert sorted(settings_eia860m_years) == list(
            range(max_eia860 + 1, max_eia860m + 1)
        )

    def test_eia860m(self: Self):
        """Test creation of eia860m_year_month values when eia860m is True."""
        eia860_settings = Eia860Settings(eia860m=True)
        assert eia860_settings.eia860m_year_months


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


class TestGridPathRAToolkitSettings:
    """Test GridPath RA Toolkit settings validation and part selection."""

    def test_parts_compiled_from_selected_options(self: Self):
        """Ensure parts are derived even when ``parts`` is omitted from config."""
        settings = GridPathRAToolkitSettings(
            technology_types=["wind"],
            processing_levels=["extended"],
            daily_weather=True,
        )

        assert settings.parts == [
            "daily_weather",
            "aggregated_extended_wind_capacity",
            "wind_capacity_aggregations",
        ]

    def test_fast_profile_gridpath_parts_not_empty(self: Self):
        """Ensure packaged fast settings yield GridPath parts used by Dagster assets."""
        etl_settings = EtlSettings.from_yaml(
            "src/pudl/package_data/settings/etl_fast.yml"
        )
        assert etl_settings.datasets is not None

        gridpath_settings = etl_settings.datasets.gridpathratoolkit
        assert gridpath_settings.parts
        assert "aggregated_extended_wind_capacity" in gridpath_settings.parts

    def test_model_dump_round_trip(self: Self):
        """GridPathRAToolkitSettings must survive a model_dump → reconstruct round-trip.

        Regression: model_dump() includes computed fields by default in Pydantic v2,
        so ``parts`` appeared in the dump. Passing it back to the constructor then raised
        a ValidationError because ``parts`` is a computed field and the model uses
        ``extra="forbid"``.
        """
        settings = GridPathRAToolkitSettings()
        dumped = settings.model_dump()
        # parts must not be present; if it is, reconstruction will raise ValidationError
        GridPathRAToolkitSettings(**dumped)

    def test_dagster_config_excludes_computed_parts(self: Self):
        """The Dagster config schema must not include the computed ``parts`` field.

        Regression: create_dagster_config(DatasetsSettings()) included ``parts`` as a
        configurable Dagster field, causing DatasetsSettings(**resource_config) to fail
        with extra="forbid" when Dagster reconstructed the settings from that schema.
        """
        config = create_dagster_config(DatasetsSettings())
        gridpath_config = config.get("gridpathratoolkit", {})
        assert "parts" not in gridpath_config


class TestEtlSettings:
    """Test pydantic model that validates all the full ETL Settings."""

    @staticmethod
    def test_validate_xbrl_years():
        """Test validation error is raised when FERC XBRL->SQLite years don't overlap with PUDL years."""
        with pytest.raises(ValidationError):
            _ = EtlSettings(
                datasets=DatasetsSettings(ferc1=Ferc1Settings(years=[2021])),
                ferc_to_sqlite_settings=FercToSqliteSettings(
                    ferc1_xbrl_to_sqlite_settings=Ferc1XbrlToSqliteSettings(
                        years=[2023]
                    )
                ),
            )


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


def _all_settings_instances() -> list[BaseModel]:
    """Return one default instance of every concrete settings class in pudl.settings.

    Abstract base classes that are not meant to be instantiated directly are
    excluded. Classes that require non-default arguments are constructed
    explicitly. All remaining classes are constructed with no arguments.

    Any new settings class added to ``pudl.settings`` is automatically included
    here, so :func:`test_all_settings_model_dump_round_trip` stays comprehensive
    without manual maintenance.
    """
    # True abstract bases: no data_source / no years default — all concrete
    # subclasses are already covered by their own entries in this list.
    skip = {
        _settings_module.FrozenBaseModel,
        _settings_module.GenericDatasetSettings,
        _settings_module.FercGenericXbrlToSqliteSettings,
    }
    instances: list[BaseModel] = []
    for _, cls in inspect.getmembers(_settings_module, inspect.isclass):
        if (
            not issubclass(cls, BaseModel)
            or cls.__module__ != _settings_module.__name__
            or cls in skip
        ):
            continue
        instances.append(cls())
    return instances


@pytest.mark.parametrize(
    "instance",
    _all_settings_instances(),
    ids=lambda i: type(i).__name__,
)
def test_all_settings_model_dump_round_trip(instance: BaseModel) -> None:
    """Every settings class must survive a model_dump → reconstruct round-trip.

    Verifies that ``model_dump()`` produces a dict that can be passed back to
    the constructor without raising a ``ValidationError``. This catches accidental
    use of ``@computed_field``, which includes derived values in ``model_dump()``
    output that cannot be re-supplied to constructors on models with
    ``extra="forbid"``.
    """
    dumped = instance.model_dump()
    type(instance)(**dumped)


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
            f"CEMS paritions should have year_quarter columns only, found:{cems_parts}"
        )
