"""Tests for settings validation."""

import importlib.resources
import inspect
from typing import Self

import pandas as pd
import pytest
from dagster import build_init_resource_context
from dagster._core.execution.context.init import UnboundInitResourceContext
from pandas import json_normalize
from pydantic import BaseModel, ValidationError

import pudl.settings as _settings_module
from pudl.dagster.resources import (
    DatastoreResource,
    GlobalDataConfigResource,
    ZenodoDoiSettingsResource,
)
from pudl.metadata.classes import DataSource
from pudl.settings import (
    Eia860DataConfig,
    Eia860mDataConfig,
    Eia923DataConfig,
    EiaDataConfig,
    EpaCemsDataConfig,
    Ferc1DataConfig,
    Ferc1XbrlToSqliteDataConfig,
    FercToSqliteDataConfig,
    GenericDataConfig,
    GlobalDataConfig,
    GridPathRaToolkitDataConfig,
    PudlDataConfig,
    load_global_data_config,
)
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths


class TestGenericDataConfig:
    """Test generic dataset behavior."""

    def test_missing_field_error(self: Self):
        """Test GenericDataConfig throws error if user forgets to add a field.

        In this case, the required ``data_source`` parameter is missing.
        """
        with pytest.raises(ValidationError):
            working_partitions = {"years": [2001]}
            working_tables = ["table"]

            class Test(GenericDataConfig):
                data_source: DataSource = DataSource(  # type: ignore  # noqa: PGH003
                    working_partitions=working_partitions,
                    working_tables=working_tables,  # type: ignore  # noqa: PGH003
                )

            Test()


class TestFerc1DataConfig:
    """Test Ferc1 data config validation.

    EIA860 and EIA923 use the same validation functions.
    """

    def test_not_working_year(self: Self):
        """Make sure a validation error is being thrown when given an invalid year."""
        with pytest.raises(ValidationError):
            Ferc1DataConfig(years=[1901])

    def test_duplicate_sort_years(self: Self):
        """Test years are sorted and deduplicated."""
        with pytest.raises(ValidationError):
            _ = Ferc1DataConfig(years=[2001, 2001, 2000])

    def test_none_years_raise(self: Self):
        """Test that null years raise a validation error."""
        with pytest.raises(ValidationError):
            _ = Ferc1DataConfig(years=None)  # type: ignore  # noqa: PGH003

    def test_default_years(self: Self):
        """Test all years are used as default."""
        actual_data_config = Ferc1DataConfig()

        expected_years: list[int] = DataSource.from_id("ferc1").working_partitions[
            "years"
        ]
        assert expected_years == actual_data_config.years

        dbf_expected_years: list[int] = [
            year for year in expected_years if year <= 2020
        ]
        assert dbf_expected_years == actual_data_config.dbf_years

        xbrl_expected_years: list[int] = [
            year for year in expected_years if year >= 2021
        ]
        assert xbrl_expected_years == actual_data_config.xbrl_years


class TestEpaCemsDataConfig:
    """Test EpaCems data configuration validation."""

    def test_not_working_quarter(self: Self):
        """Make sure a validation error is being thrown when given an invalid quarter."""
        with pytest.raises(ValidationError):
            EpaCemsDataConfig(year_quarters=["1990q4"])

    def test_duplicate_quarters(self: Self):
        """Test year_quarters are deduplicated."""
        with pytest.raises(ValidationError):
            _ = EpaCemsDataConfig(year_quarters=["1999q4", "1999q4"])

    def test_default_quarters(self: Self):
        """Test all quarters are used as default."""
        returned_data_config = EpaCemsDataConfig()

        expected_year_quarters: list[str] = DataSource.from_id(
            "epacems"
        ).working_partitions["year_quarters"]
        assert expected_year_quarters == returned_data_config.year_quarters

    def test_all_year_quarters(self: Self):
        """Test the `all` option for the EPA CEMS data configuration."""
        epacems_data_config_all = EpaCemsDataConfig(year_quarters=["all"])
        working_partitions_all: list[str] = DataSource.from_id(
            "epacems"
        ).working_partitions["year_quarters"]
        assert epacems_data_config_all.year_quarters == working_partitions_all

    def test_none_quarters_raise(self: Self):
        """Test that setting a required partition to None raises an error."""
        with pytest.raises(ValidationError):
            _ = EpaCemsDataConfig(quarters=None)  # type: ignore  # noqa: PGH003


class TestEia860DataConfig:
    """Test EIA860 data configuration validation."""

    def test_eia860_years_overlap_eia860m_years(self: Self):
        """Test validation error is raised when eia860m date is within eia860 years."""
        # Identify the last valid EIA-860 year:
        max_eia860_year: int = max(Eia860DataConfig().years)
        # Use that year to construct an EIA-860M year that overlaps the EIA-860 years:
        bad_eia860m_year_month = f"{max_eia860_year}-01"

        # Attempt to construct an EIA-860 data config object with an EIA-860M year that
        # overlaps the EIA-860 years, which should result in a ValidationError:
        with pytest.raises(ValidationError):
            _ = Eia860DataConfig(
                eia860m=True,
                years=[max_eia860_year],
                eia860m_year_months=[bad_eia860m_year_month],
            )

    def test_eia860m_years_overlap_eia860m_years(self: Self):
        """Test validation error is raised when eia860m years overlap."""
        max_eia860_year: int = max(Eia860DataConfig().years)
        acceptable_eia860m_year: int = max_eia860_year + 1
        bad_eia860m_year_months: list[str] = [
            f"{acceptable_eia860m_year}-01",
            f"{acceptable_eia860m_year}-02",
        ]
        with pytest.raises(ValidationError):
            _ = Eia860DataConfig(
                eia860m=True,
                years=[max_eia860_year],
                eia860m_year_months=bad_eia860m_year_months,
            )

    def test_eia860m_after_eia860(self: Self):
        """Test the creation of eia860m_year_months values."""
        eia860_data_config = Eia860DataConfig()
        max_eia860 = max(DataSource.from_id("eia860").working_partitions["years"])
        max_eia860m = pd.to_datetime(
            max(DataSource.from_id("eia860m").working_partitions["year_months"])
        ).year
        eia860m_data_config_years: list[int] = [
            pd.to_datetime(date).year for date in eia860_data_config.eia860m_year_months
        ]
        # Assert that the default eia860m data config years are a complete range between the
        # year after the last available eia860 year and the latest available eia860m year
        assert sorted(eia860m_data_config_years) == list(
            range(max_eia860 + 1, max_eia860m + 1)
        )

    def test_eia860m(self: Self):
        """Test creation of eia860m_year_month values when eia860m is True."""
        eia860_data_config = Eia860DataConfig(eia860m=True)
        assert eia860_data_config.eia860m_year_months


class TestEia860mDataConfig:
    """Test EIA860m data config."""

    def test_all_year_quarters(self: Self):
        """Test the `all` option for the eia860m data configuration."""
        data_config_all: list[str] = Eia860mDataConfig(year_months=["all"]).year_months
        partitions_all = DataSource.from_id("eia860m").working_partitions["year_months"]
        assert data_config_all == partitions_all


class TestEiaDataConfig:
    """Test pydantic model that validates EIA datasets."""

    def test_eia923_dependency(self: Self):
        """Test that there is some overlap between EIA860 and EIA923 data."""
        eia923_data_config = Eia923DataConfig()
        eia_data_config = EiaDataConfig(eia923=eia923_data_config)
        data_source: DataSource = DataSource.from_id("eia860")
        assert eia_data_config.eia860 is not None
        assert eia_data_config.eia923 is not None
        # assign both EIA form years
        eia860_years: list[int] = eia_data_config.eia860.years
        eia923_years_partition: list[int] = data_source.working_partitions["years"]
        eia923_years_data_config: list[int] = eia_data_config.eia923.years
        # assert that there is some overlap between EIA years
        assert not set(eia860_years).isdisjoint(eia923_years_partition)
        assert not set(eia860_years).isdisjoint(eia923_years_data_config)

    def test_eia860_dependency(self: Self):
        """Test that there is some overlap between EIA860 and EIA923 data."""
        eia860_data_config = Eia860DataConfig()
        eia_data_config = EiaDataConfig(eia860=eia860_data_config)
        data_source: DataSource = DataSource.from_id("eia923")
        assert eia_data_config.eia923 is not None
        assert eia_data_config.eia860 is not None
        # assign both EIA form years
        eia923_years: list[int] = eia_data_config.eia923.years
        eia860_years_partition: list[int] = data_source.working_partitions["years"]
        eia860_years_data_config: list[int] = eia_data_config.eia860.years
        # assert that there is some overlap between EIA years
        assert not set(eia923_years).isdisjoint(eia860_years_partition)
        assert not set(eia923_years).isdisjoint(eia860_years_data_config)


class TestPudlDataConfig:
    """Test pydantic model that validates all datasets."""

    def test_default_behavior(self: Self):
        """Make sure all of the years are added if nothing is specified."""
        pudl_data_config = PudlDataConfig()
        data_source: DataSource = DataSource.from_id("ferc1")

        expected_years: list[int] = data_source.working_partitions["years"]
        assert pudl_data_config.ferc1 is not None
        returned_years: list[int] = pudl_data_config.ferc1.years
        assert expected_years == returned_years

        assert pudl_data_config.eia, "EIA data config was not added."

    def test_glue(self: Self):
        """Test glue data config get added when ferc and eia are requested."""
        pudl_data_config = PudlDataConfig()
        assert pudl_data_config.glue, (
            "Glue data config was not added when it should have been."
        )

        assert pudl_data_config.glue.eia
        assert pudl_data_config.glue.ferc1


class TestGridPathRaToolkitDataConfig:
    """Test GridPath RA Toolkit data configuration validation and part selection."""

    def test_parts_compiled_from_selected_options(self: Self):
        """Ensure parts are derived even when ``parts`` is omitted from config."""
        data_config = GridPathRaToolkitDataConfig(
            technology_types=["wind"],
            processing_levels=["extended"],
            daily_weather=True,
        )

        assert data_config.parts == [
            "daily_weather",
            "aggregated_extended_wind_capacity",
            "wind_capacity_aggregations",
        ]

    def test_fast_profile_gridpath_parts_not_empty(self: Self):
        """Ensure packaged fast data config yields GridPath parts used by Dagster assets."""
        with importlib.resources.as_file(
            importlib.resources.files("pudl.package_data.settings") / "etl_fast.yml"
        ) as path:
            global_data_config: GlobalDataConfig = load_global_data_config(str(path))
        assert global_data_config.pudl is not None

        gridpath_data_config: GridPathRaToolkitDataConfig | None = (
            global_data_config.pudl.gridpathratoolkit
        )
        assert gridpath_data_config is not None
        assert gridpath_data_config.parts
        assert "aggregated_extended_wind_capacity" in gridpath_data_config.parts

    def test_model_dump_round_trip(self: Self):
        """GridPathRaToolkitDataConfig must survive a model_dump → reconstruct round-trip.

        Regression: model_dump() includes computed fields by default in Pydantic v2,
        so ``parts`` appeared in the dump. Passing it back to the constructor then raised
        a ValidationError because ``parts`` is a computed field and the model uses
        ``extra="forbid"``.
        """
        data_config = GridPathRaToolkitDataConfig()
        dumped = data_config.model_dump()
        # parts must not be present; if it is, reconstruction will raise ValidationError
        GridPathRaToolkitDataConfig(**dumped)


class TestGlobalDataConfig:
    """Test pydantic model that validates all the Global data configuration."""

    @staticmethod
    def test_validate_xbrl_years():
        """Test validation error is raised when FERC XBRL->SQLite years don't overlap with PUDL years."""
        with pytest.raises(ValidationError):
            _ = GlobalDataConfig(
                pudl=PudlDataConfig(ferc1=Ferc1DataConfig(years=[2021])),
                ferc_to_sqlite=FercToSqliteDataConfig(
                    ferc1_xbrl=Ferc1XbrlToSqliteDataConfig(years=[2023])
                ),
            )


class TestGlobalConfig:
    """Test global pydantic model config works."""

    def test_unknown_dataset(self: Self):
        """Test unkown dataset fed to PudlDataConfig."""
        with pytest.raises(ValidationError):
            PudlDataConfig().model_validate({"unknown_data": "data"})

        with pytest.raises(ValidationError):
            GlobalDataConfig().model_validate({"unknown_data": "data"})

    def test_immutability(self: Self):
        """Test immutability config is working correctly."""
        with pytest.raises(ValidationError):
            data_config = PudlDataConfig()
            data_config.eia = EiaDataConfig()

        with pytest.raises(ValidationError):
            data_config = EiaDataConfig()
            data_config.eia860 = Eia860DataConfig()


class TestGlobalDataConfigResource:
    """Test the ETL settings Dagster resource."""

    def test_invalid_field_type(self: Self):
        """Test an error is thrown when the ETL settings path has the wrong type."""
        init_context: UnboundInitResourceContext = build_init_resource_context(
            config={"global_data_config_path": 2021}
        )
        with pytest.raises(ValidationError):
            _ = GlobalDataConfigResource.from_resource_context(init_context)

    def test_loads_from_file(self: Self):
        """Test that ETL settings are loaded from the shared ETL settings file."""
        with importlib.resources.as_file(
            importlib.resources.files("pudl.package_data.settings") / "etl_fast.yml"
        ) as path:
            init_context: UnboundInitResourceContext = build_init_resource_context(
                config={"global_data_config_path": str(path)}
            )

        loaded_data_config: GlobalDataConfig = (
            GlobalDataConfigResource.from_resource_context(init_context)
        )

        assert isinstance(loaded_data_config, GlobalDataConfig)
        assert loaded_data_config.pudl.ferc1 is not None


def test_datastore_resource_loads() -> None:
    """Test that the migrated datastore resource creates a runtime Datastore."""
    with ZenodoDoiSettingsResource.from_resource_context_cm(
        build_init_resource_context()
    ) as zenodo_dois:
        init_context: UnboundInitResourceContext = build_init_resource_context(
            config={
                "cloud_cache_path": "s3://pudl.catalyst.coop/zenodo",
                "use_local_cache": False,
            },
            resources={"zenodo_dois": zenodo_dois},
        )

        with DatastoreResource.from_resource_context_cm(init_context) as datastore:
            assert isinstance(datastore, Datastore)


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
        _settings_module.GenericDataConfig,
        _settings_module.FercDbfToSqliteDataConfig,
        _settings_module.FercGenericXbrlToSqliteDataConfig,
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
def test_all_data_config_model_dump_round_trip(instance: BaseModel) -> None:
    """Every data config class must survive a model_dump → reconstruct round-trip.

    Verifies that ``model_dump()`` produces a dict that can be passed back to
    the constructor without raising a ``ValidationError``. This catches accidental
    use of ``@computed_field``, which includes derived values in ``model_dump()``
    output that cannot be re-supplied to constructors on models with
    ``extra="forbid"``.
    """
    dumped = instance.model_dump()
    type(instance)(**dumped)


def test_partitions_with_json_normalize(pudl_data_config):
    """Ensure the FERC1 and CEMS partitions normalize."""
    datasets = pudl_data_config.get_datasets()
    ferc_parts: pd.DataFrame = json_normalize(datasets["ferc1"].partitions)
    if list(ferc_parts.columns) != ["year"]:
        raise AssertionError(
            "FERC1 paritions should have year and state columns only, found:"
            f"{ferc_parts}"
        )

    cems_parts: pd.DataFrame = json_normalize(datasets["epacems"].partitions)
    if list(cems_parts.columns) != ["year_quarter"]:
        raise AssertionError(
            f"CEMS paritions should have year_quarter columns only, found:{cems_parts}"
        )


@pytest.mark.slow
def test_partitions_for_datasource_table(pudl_data_config):
    """Test whether or not we can make the datasource table."""
    ds = Datastore(local_cache_path=PudlPaths().data_dir)
    datasource = pudl_data_config.make_datasources_table(ds)
    datasets = pudl_data_config.get_datasets().keys()
    if datasource.empty and datasets != 0:
        raise AssertionError(
            "Datasource table is empty with the following datasets in the settings: "
            f"{datasets}"
        )
