"""Module for validating pudl etl settings."""
import pathlib
from typing import ClassVar, List

import pandas as pd
import yaml
from pydantic import BaseModel as PydanticBaseModel
from pydantic import BaseSettings, root_validator, validator

import pudl
import pudl.workspace.setup
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import DBF_TABLES_FILENAMES
from pudl.metadata.resources.eia861 import TABLE_DEPENDENCIES


class BaseModel(PydanticBaseModel):
    """BaseModel with global configuration."""

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"


class GenericDatasetSettings(BaseModel):
    """
    An abstract pydantic model for generic datasets.

    Each dataset must specify working tables and partitions.
    A dataset can have an arbitrary number of partitions.
    """

    tables: List[str]

    @root_validator
    def validate_partitions(cls, partitions):  # noqa: N805
        """
        Validate the requested data partitions.

        Check that all the partitions defined in the ``working_partitions`` of the
        associated ``data_source`` (e.g. years or states) have been assigned in the
        definition of the class, and that the requested values are a subset of the
        allowable values defined by the ``data_source``.

        """
        for name, working_partitions in cls.data_source.working_partitions.items():
            try:
                partition = partitions[name]
            except KeyError:
                raise ValueError(f"{cls.__name__} is missing required '{name}' field.")

            partitions_not_working = list(set(partition) - set(working_partitions))
            if partitions_not_working:
                raise ValueError(
                    f"'{partitions_not_working}' {name} are not available.")
            partitions[name] = sorted(set(partition))
        return partitions

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables are available."""
        tables_not_working = list(set(tables) - set(cls.data_source.get_resource_ids()))
        if tables_not_working:
            raise ValueError(
                f"'{tables_not_working}' tables are not available.")
        return sorted(set(tables))


class Ferc1Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate Ferc1Settings.

    Parameters:
        data_source: DataSource metadata object
        years: List of years to validate.
        tables: List of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")

    years: List[int] = data_source.working_partitions["years"]
    tables: List[str] = data_source.get_resource_ids()


class Ferc714Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate Ferc714Settings.

    Parameters:
        data_source: DataSource metadata object
        tables: List of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")

    tables: List[str] = data_source.get_resource_ids()


class EpaCemsSettings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate EPA CEMS settings.

    Parameters:
        data_source: DataSource metadata object
        years: List of years to validate.
        states: List of states to validate.
        tables: List of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("epacems")

    years: List[int] = data_source.working_partitions["years"]
    states: List[str] = data_source.working_partitions["states"]
    tables: List[str] = data_source.get_resource_ids()

    @validator("states")
    def allow_all_keyword(cls, states):  # noqa: N805
        """Allow users to specify ['all'] to get all states."""
        if states == ["all"]:
            states = cls.data_source.working_partitions["states"]
        return states


class Eia923Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate EIA 923 settings.

    Parameters:
        data_source: DataSource metadata object
        years: List of years to validate.
        tables: List of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia923")

    years: List[int] = data_source.working_partitions["years"]
    tables: List[str] = data_source.get_resource_ids()


class Eia861Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate EIA 861 settings.

    Parameters:
        data_source: DataSource metadata object
        years: List of years to validate.
        tables: List of tables to validate.
        transform_functions: List of transform functions to be applied to eia861
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia861")

    years: List[int] = data_source.working_partitions["years"]
    tables: List[str] = data_source.get_resource_ids()
    transform_functions: List[str]

    @root_validator(pre=True)
    def generate_transform_functions(cls, values):  # noqa: N805
        """
        Map tables to transform functions.

        Args:
            values: eia861 settings.

        Returns:
            values: eia861 settings.
        """
        # balancing_authority_eia861 is always processed
        transform_functions = ["balancing_authority_eia861"]

        # Defaults to all transformation functions
        if not values.get("tables"):
            transform_functions.extend(list(TABLE_DEPENDENCIES))
        else:
            for table in values["tables"]:
                transform_functions.extend(
                    [tf_func for tf_func, tables in TABLE_DEPENDENCIES.items()
                        if table in tables]
                )

        values["transform_functions"] = sorted(set(transform_functions))

        return values


class Eia860Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate EIA 860 settings.

    This model also check 860m settings.

    Parameters:
        data_source: DataSource metadata object
        years: List of years to validate.
        tables: List of tables to validate.

        eia860m_date ClassVar[str]: The 860m year to date.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia860")
    eia860m_data_source: ClassVar[DataSource] = DataSource.from_id("eia860m")
    eia860m_date: ClassVar[str] = eia860m_data_source.working_partitions[
        "year_month"]

    years: List[int] = data_source.working_partitions["years"]
    tables: List[str] = data_source.get_resource_ids()
    eia860m: bool = True

    @validator("eia860m")
    def check_eia860m_date(cls, eia860m: bool) -> bool:  # noqa: N805
        """
        Check 860m date year is exactly one year later than most recent working 860 year.

        Args:
            eia860m: True if 860m is requested.

        Returns:
            eia860m: True if 860m is requested.

        Raises:
            ValueError: the 860m date is within 860 working years.
        """
        eia860m_year = pd.to_datetime(cls.eia860m_date).year
        expected_year = max(cls.data_source.working_partitions["years"]) + 1
        if eia860m and (eia860m_year != expected_year):
            raise AssertionError(
                """Attempting to integrate an eia860m year"""
                f"""({eia860m_year}) not immediately following the eia860 years:"""
                f"""{cls.data_source.working_partitions["years"]}. Consider switching eia860m"""
                """parameter to False."""
            )
        return eia860m


class GlueSettings(BaseModel):
    """
    An immutable pydantic model to validate Glue settings.

    Parameters:
        eia: Include eia in glue settings.
        ferc1: Include ferc1 in glue settings.
    """

    eia: bool = True
    ferc1: bool = True


class EiaSettings(BaseModel):
    """
    An immutable pydantic model to validate EIA datasets settings.

    Parameters:
        eia860: Immutable pydantic model to validate eia860 settings.
        eia923: Immutable pydantic model to validate eia923 settings.
    """

    eia860: Eia860Settings = None
    eia923: Eia923Settings = None

    @root_validator(pre=True)
    def default_load_all(cls, values):  # noqa: N805
        """
        If no datasets are specified default to all.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        if not any(values.values()):
            values["eia860"] = Eia860Settings()
            values["eia923"] = Eia923Settings()

        return values

    @root_validator
    def check_eia_dependencies(cls, values):  # noqa: N805
        """
        Make sure the dependencies between the eia datasets are satisfied.

        Dependencies:
        * eia860 requires eia923.boiler_fuel_eia923 and eia923.generation_eia923.
        * eia923 requires eia860 for harvesting purposes.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        eia923 = values.get("eia923")
        eia860 = values.get("eia860")
        if not eia923 and eia860:
            values["eia923"] = Eia923Settings(
                tables=['boiler_fuel_eia923', 'generation_eia923'],
                years=eia860.years
            )

        if eia923 and not eia860:
            values["eia860"] = Eia860Settings(
                years=eia923.years
            )
        return values


class DatasetsSettings(BaseModel):
    """
    An immutable pydantic model to validate PUDL Dataset settings.

    Parameters:
        ferc1: Immutable pydantic model to validate ferc1 settings.
        eia: Immutable pydantic model to validate eia(860, 923) settings.
        glue: Immutable pydantic model to validate glue settings.
        epacems: Immutable pydantic model to validate epacems settings.
    """

    ferc1: Ferc1Settings = None
    eia: EiaSettings = None
    glue: GlueSettings = None
    epacems: EpaCemsSettings = None

    @root_validator(pre=True)
    def default_load_all(cls, values):  # noqa: N805
        """
        If no datasets are specified default to all.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        if not any(values.values()):
            values["ferc1"] = Ferc1Settings()
            values["eia"] = EiaSettings()
            values["glue"] = GlueSettings()
            values["epacems"] = EpaCemsSettings()

        return values

    @root_validator
    def add_glue_settings(cls, values):  # noqa: N805
        """
        Add glue settings if ferc1 and eia data are both requested.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        ferc1 = bool(values.get("ferc1"))
        eia = bool(values.get("eia"))

        values["glue"] = GlueSettings(ferc1=ferc1, eia=eia)
        return values

    def get_datasets(self):  # noqa: N805
        """Gets dictionary of dataset settings."""
        return vars(self)


class Ferc1ToSqliteSettings(GenericDatasetSettings):
    """
    An immutable pydantic nodel to validate Ferc1 to SQLite settings.

    Parameters:
        tables: List of tables to validate.
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")
    years: List[int] = data_source.working_partitions["years"]
    tables: List[str] = sorted(list(DBF_TABLES_FILENAMES.keys()))

    refyear: ClassVar[int] = max(years)
    bad_cols: tuple = ()

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables."""
        default_tables = sorted(list(DBF_TABLES_FILENAMES.keys()))
        tables_not_working = list(set(tables) - set(default_tables))
        if len(tables_not_working) > 0:
            raise ValueError(
                f"'{tables_not_working}' tables are not available.")
        return sorted(set(tables))


class EtlSettings(BaseSettings):
    """Main settings validation class."""

    ferc1_to_sqlite_settings: Ferc1ToSqliteSettings = None
    datasets: DatasetsSettings = None

    name: str = None
    title: str = None
    description: str = None
    version: str = None

    pudl_in: str = pudl.workspace.setup.get_defaults()["pudl_in"]
    pudl_out: str = pudl.workspace.setup.get_defaults()["pudl_out"]

    @classmethod
    def from_yaml(cls, path: str):
        """
        Create an EtlSettings instance from a yaml_file path.

        Parameters:
            path: path to a yaml file.

        Returns:
            EtlSettings: etl settings object.
        """
        with pathlib.Path(path).open() as f:
            yaml_file = yaml.safe_load(f)
        return cls.parse_obj(yaml_file)
