"""Module for validating pudl etl settings."""
import abc
from typing import ClassVar, List

import pandas as pd
from pydantic import BaseModel as PydanticBaseModel
from pydantic import root_validator, validator

from pudl.extract.ferc1 import DBF_TABLES_FILENAMES
from pudl.metadata.enums import EPACEMS_STATES


def validate_years(cls, years: List[int]) -> List[int]:
    """
    Validate years of a Basemodel dataset.

    This function checks the years are available. They are sorted and deduplicated.

    Args:
        years (List[int]): List of years to check.

    Returns:
        years (List[int]): List of sorted and deduplicated years.

    Raises:
        ValueError: some of the years are not available.
    """
    # Make sure the working tables are sorted too.
    cls.working_years = sorted(cls.working_years)
    # TODO (bendnorman): Hacky fix to not set defaults usings abstract property
    if not years:
        years = cls.working_years
    years_not_working = set(years) - set(cls.working_years)

    if len(years_not_working) > 0:
        raise ValueError(f"'{years_not_working}' years are not available.")
    return sorted(set(years))


def validate_tables(cls, tables: List[str]) -> List[str]:
    """
    Validate tables of a BaseModel dataset.

    This function checks the tables are available. They are sorted and deduplicated.

    Args:
        tables (List[int]): List of tables to check.

    Returns:
        tables (List[int]): List of sorted and deduplicated tables.

    Raises:
        ValueError: some of the tables are not available.
    """
    # Make sure the working tables are sorted too.
    cls.working_tables = sorted(cls.working_tables)
    # TODO (bendnorman): Hacky fix to not set defaults usings abstract property
    if not tables:
        tables = cls.working_tables
    tables_not_working = set(tables) - set(cls.working_tables)

    if len(tables_not_working) > 0:
        raise ValueError(f"'{tables_not_working}' tables are not available.")
    return sorted(set(tables))


class BaseModel(PydanticBaseModel):
    """BaseModel with global configuration."""

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"


class GenericDatasetSettings(BaseModel, abc.ABC):
    """
    An abstract pydantic model for generic datasets.

    Attributes:
        years (List[int]): List of years to validate.
        tables (List[str]): List of table to validate.

        working_years ClassVar[List[int]]: List of working years.
        working_tables ClassVar[List[str]]: List of working table.
    """

    years: List[int] = None
    tables: List[str] = None

    _validate_years = validator("years", allow_reuse=True, always=True)(validate_years)
    _validate_tables = validator("tables", allow_reuse=True,
                                 always=True)(validate_tables)

    @property
    @abc.abstractmethod
    def working_years(cls) -> List[int]:
        """Abstract working year property."""
        return cls.working_years

    @property
    @abc.abstractmethod
    def working_tables(cls) -> List[str]:
        """Abstract working tables property."""
        return cls.working_tables


class Ferc1Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate FERC1 settings.

    Attributes:
        years (List[int]): List of years to validate.
        tables (List[str]): List of table to validate.

        working_years ClassVar[List[int]]: List of working years.
        working_tables ClassVar[List[str]]: List of working table.
    """

    working_years: ClassVar[List[int]] = list(range(1994, 2020))
    working_tables: ClassVar[List[str]] = [
        'fuel_ferc1',
        'plants_steam_ferc1',
        'plants_small_ferc1',
        'plants_hydro_ferc1',
        'plants_pumped_storage_ferc1',
        'purchased_power_ferc1',
        'plant_in_service_ferc1'
    ]


class Eia860Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate EIA860 settings.

    This model also check 860m settings.

    Attributes:
        years (List[int]): List of years to validate.
        tables (List[str]): List of table to validate.

        working_years (ClassVar[List[int]]): List of working years.
        working_tables (ClassVar[List[str]]): List of working table.
    """

    working_years: ClassVar[List[int]] = list(range(2001, 2020))
    working_tables: ClassVar[List[str]] = [
        'boiler_generator_assn_eia860',
        'utilities_eia860',
        'plants_eia860',
        'generators_eia860',
        'ownership_eia860'
    ]
    eia860m_date: ClassVar[str] = '2020-11'

    eia860m: bool = False

    @validator("eia860m")
    def check_860m_date(cls, eia860m):
        """
        Check 860m date is not within 860 working years if 860m is requested.

        Args:
            eia860m (bool): True if 860m is requested.

        Returns:
            eia860m (bool): True if 860m is requested.

        Raises:
            ValueError: the 860m date is within 860 working years.
        """
        eia860m_year = pd.to_datetime(cls.eia860m_date).year
        if eia860m and eia860m_year in cls.working_years:
            raise AssertionError(
                """Attempting to integrate an eia860m year"""
                f"""({eia860m_year}) that is within the eia860 years:"""
                f"""{cls.working_years}. Consider switching eia860_ytd"""
                """parameter to False."""
            )
        return eia860m


class Eia923Settings(GenericDatasetSettings):
    """
    An immutable pydantic model to validate EIA923 settings.

    Attributes:
        years (List[int]): List of years to validate.
        tables (List[str]): List of table to validate.

        working_years (ClassVar[List[int]]): List of working years.
        working_tables (ClassVar[List[str]]): List of working table.
    """

    working_years: ClassVar[List[int]] = list(range(2001, 2020))
    working_tables: ClassVar[List[str]] = [
        'generation_fuel_eia923',
        'boiler_fuel_eia923',
        'generation_eia923',
        'coalmine_eia923',
        'fuel_receipts_costs_eia923'
    ]


class GlueSettings(BaseModel):
    """
    An immutable pydantic model to validate Glue settings.

    Attributes:
        eia (bool): Include eia in glue settings.
        ferc1 (bool): Include ferc1 in glue settings.
    """

    eia: bool = True
    ferc1: bool = True


class EpaCemsSettings(BaseModel):
    """
    An immutable pydantic nodel to validate EPA CEMS settings.

    Attributes:
        states (List[str]): List of states to validate.
        years (List[str]): List of years to validate.
    """

    working_years: ClassVar[List[int]] = list(range(1995, 2021))
    working_states: ClassVar[List[str]] = sorted(set(EPACEMS_STATES))

    years: List[int] = working_years
    states: List[str] = working_states

    _validate_years = validator("years", allow_reuse=True)(validate_years)

    @validator("states")
    def validate_states(cls, states: List[str]) -> List[str]:
        """
        Validate states of a BaseModel dataset.

        This function checks the states are available. They are sorted and deduplicated.
        If states is "all" then all working states are used.

        Args:
            states (List[int]): List of states to check.

        Returns:
            states (List[int]): List of sorted and deduplicated states.

        Raises:
            ValueError: some of the states are not available.
        """
        if states == ["all"]:
            states = cls.working_states

        states_not_working = set(states) - set(cls.working_states)

        if len(states_not_working) > 0:
            raise ValueError(f"'{states_not_working}' states are not available.")
        return sorted(set(states))


class EiaSettings(BaseModel):
    """
    An immutable pydantic model to validate EIA datasets settings.

    Attributes:
        eia860 (Eia860Settings): Immutable pydantic model to validate eia860 settings.
        eia923 (Eia923Settings): Immutable pydantic model to validate eia923 settings.
    """

    eia860: Eia860Settings = None
    eia923: Eia923Settings = None

    @root_validator
    def check_eia_dependencies(cls, values):
        """
        Make sure the dependencies between the eia datasets are satisfied.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        # If you want to load eia860 you need 923's boiler_fuel and generation tables.
        eia923 = values.get("eia923")
        eia860 = values.get("eia860")
        if not eia923 and eia860:
            values["eia923"] = Eia923Settings(
                tables=['boiler_fuel_eia923', 'generation_eia923'],
                years=eia860.years
            )

        # You can't load eia923 without eia860.
        if eia923 and not eia860:
            values["eia860"] = Eia860Settings(
                years=eia923.years
            )
        return values


class DatasetsSettings(BaseModel):
    """
    An immutable pydantic model to validate PUDL Dataset settings.

    Attributes:
        ferc1 (Ferc1Settings): Immutable pydantic model to validate ferc1 settings.
        eia860 (Eia860Settings): Immutable pydantic model to validate eia860 settings.
        eia923 (Eia923Settings): Immutable pydantic model to validate eia923 settings.
        glue (GlueSettings): Immutable pydantic model to validate glue settings.
    """

    ferc1: Ferc1Settings = None
    eia: EiaSettings = None
    glue: GlueSettings = None
    epacems: EpaCemsSettings = None

    @root_validator(pre=True)
    def default_load_all(cls, values):
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
        return values

    @root_validator
    def add_glue_settings(cls, values):
        """
        Add glue settings if ferc1 and eia data are both requested.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        if values.get("ferc1") and values.get("eia"):
            values["glue"] = GlueSettings()
        return values

    def get_datasets(cls):
        """Gets dictionary of dataset settings."""
        return vars(cls)


class Ferc1ToSqliteSettings(GenericDatasetSettings):
    """
    An immutable pydantic nodel to validate EPA CEMS settings.

    Attributes:
        tables (List[str]): List of states to validate.
        years (List[str]): List of years to validate.
        ferc1_to_sqlite_refyear (int): reference year. Defaults to most recent year.
    """

    working_years: ClassVar[List[int]] = list(range(1994, 2020))
    working_tables: ClassVar[List[str]] = DBF_TABLES_FILENAMES.keys()

    refyear: int = max(working_tables)
    bad_cols: tuple = ()

    @validator("refyear")
    def check_reference_year(cls, refyear: int) -> int:
        """Checks reference year is within available years."""
        if refyear not in cls.working_years:
            raise ValueError(f"Reference year {refyear} is outside the range of "
                             f"available FERC Form 1 data {cls.working_years}.")
        return refyear
