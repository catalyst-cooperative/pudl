"""Module for validating pudl etl settings."""
import abc
import pathlib
from typing import Any, ClassVar, Dict, List

import pandas as pd
import yaml
from pydantic import BaseModel as PydanticBaseModel
from pydantic import BaseSettings, create_model, root_validator, validator

import pudl
import pudl.constants as pc
from pudl.extract.ferc1 import DBF_TABLES_FILENAMES


class BaseModel(PydanticBaseModel):
    """BaseModel with global configuration."""

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"


class GenericDatasetSettings(BaseModel, abc.ABC):
    """
    An abstract pydantic model for generic datasets.

    Each dataset must specify working tables are partitions.
    A dataset can have an arbitrary number of partitioins.
    """

    tables: List

    @property
    @abc.abstractmethod
    def working_partitions(cls) -> Dict[str, List[Any]]:  # noqa: N805
        """Abstract working_partitions property."""
        return cls.working_partitions

    @property
    @abc.abstractmethod
    def working_tables(cls) -> List:  # noqa: N805
        """Abstract working_tables property."""
        return cls.working_tables

    @root_validator
    def validate_partitions(cls, partitions):  # noqa: N805
        """Validate partitions are available."""
        for name, working_partitions in cls.working_partitions.items():
            try:
                partition = partitions[name]
            except KeyError:
                raise ValueError(f"{cls.__name__} is missing '{name}' field.")

            partitions_not_working = list(set(partition) - set(working_partitions))
            if len(partitions_not_working) > 0:
                raise ValueError(
                    f"'{partitions_not_working}' {name} are not available.")
            partitions[name] = sorted(set(partition))
        return partitions

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables are available."""
        tables_not_working = list(set(tables) - set(cls.working_tables))
        if len(tables_not_working) > 0:
            raise ValueError(
                f"'{tables_not_working}' tables are not available.")
        return sorted(set(tables))


def create_dataset_settings(name: str) -> GenericDatasetSettings:
    """
    Create a pydantic settings class for a dataset.

    This function dynamically creates subclasses of GenericDatasetSettings
    using information from the constants WORKING_PARTITIONS and PUDL_TABLES.
    The returned class can be subclassed to add additional fields or validators.

    Args:
        name: the name of the dataset.

    Returns:
        dataset_model: Subclass of GenericDatasetSettings
    """
    try:
        partitions = pc.WORKING_PARTITIONS[name]
        tables = pc.PUDL_TABLES[name]
    except KeyError:
        raise KeyError(f"{name} is not an available dataset.")

    # sort the partitions.
    tables = sorted(list(tables))
    partitions = {k: sorted(v) for (k, v) in partitions.items()}

    class_name = name.title() + "Settings"
    dataset_model = create_model(class_name,
                                 working_partitions=(ClassVar, partitions),
                                 working_tables=(ClassVar, tables),
                                 tables=(List, tables),
                                 **partitions,
                                 __base__=GenericDatasetSettings)
    return dataset_model


# Settings that don't need to be extended
Ferc1Settings = create_dataset_settings("ferc1")
Eia923Settings = create_dataset_settings("eia923")

# Settings that do need to be extended
Eia860BaseSettings = create_dataset_settings("eia860")
EpaCemsBaseSettings = create_dataset_settings("epacems")


class Eia860Settings(Eia860BaseSettings):
    """
    An immutable pydantic model to validate EIA860 settings.

    This model also check 860m settings.

    Parameters:
        years: List of years to validate.

        working_partitions ClassVar[Dict[str, Any]]: working paritions.
        eia860m_date ClassVar[str]: The 860m year to date.
    """

    eia860m: bool = False
    eia860m_date: ClassVar[str] = pc.WORKING_PARTITIONS["eia860m"]["year_month"]

    @validator("eia860m")
    def check_860m_date(cls, eia860m: bool) -> bool:  # noqa: N805
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
        if eia860m and (eia860m_year != max(cls.working_partitions["years"]) + 1):
            raise AssertionError(
                """Attempting to integrate an eia860m year"""
                f"""({eia860m_year}) that is within the eia860 years:"""
                f"""{cls.working_partitions["years"]}. Consider switching eia860m"""
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


class EpaCemsSettings(EpaCemsBaseSettings):
    """
    An immutable pydantic nodel to validate EPA CEMS settings.

    Parameters:
        states: List of states to validate.
        years: List of years to validate.
    """

    @validator("states")
    def allow_all_keyword(cls, states):  # noqa: N805
        """Allow users to specify ['all'] to get all states."""
        if states == ["all"]:
            states = cls.working_partitions["states"]
        return states


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

    def get_datasets(cls):  # noqa: N805
        """Gets dictionary of dataset settings."""
        return vars(cls)


class Ferc1ToSqliteSettings(GenericDatasetSettings):
    """
    An immutable pydantic nodel to validate Ferc1 to SQLite settings.

    Parameters:
        tables: List of tables to validate.
        years: List of years to validate.
    """

    working_partitions: ClassVar = {
        "years": list(pc.WORKING_PARTITIONS["ferc1"]["years"])
    }
    working_tables: ClassVar = sorted(list(DBF_TABLES_FILENAMES.keys()))

    years: List[int] = working_partitions["years"]
    tables: List[str] = working_tables

    refyear: ClassVar[int] = max(years)
    bad_cols: tuple = ()


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
