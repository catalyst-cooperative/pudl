"""Module for validating pudl etl settings."""
from typing import ClassVar, List

import pandas as pd
from pydantic import BaseModel, root_validator, validator


def _validate_years(cls, years: List[int]) -> List[int]:
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
    years_not_working = set(years) - set(cls.working_years)

    if len(years_not_working) > 0:
        raise ValueError(f"'{years_not_working}' years are not available.")
    return sorted(set(years))


def _validate_tables(cls, tables: List[str]) -> List[str]:
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
    tables_not_working = set(tables) - set(cls.working_tables)

    if len(tables_not_working) > 0:
        raise ValueError(f"'{tables_not_working}' tables are not available.")
    return sorted(set(tables))


class Ferc1Settings(BaseModel):
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

    years: List[int] = working_years
    tables: List[str] = working_tables

    class Config:
        """Pydantic config."""

        allow_mutation = False

    _validate_years = validator("years", allow_reuse=True)(_validate_years)
    _validate_tables = validator("tables", allow_reuse=True)(_validate_tables)


class Eia860Settings(BaseModel):
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

    years: List[int] = working_years
    tables: List[str] = working_tables
    eia860m: bool = False

    class Config:
        """Pydantic config."""

        allow_mutation = False

    _validate_years = validator("years", allow_reuse=True)(_validate_years)
    _validate_tables = validator("tables", allow_reuse=True)(_validate_tables)

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


class Eia923Settings(BaseModel):
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

    years: List[int] = working_years
    tables: List[str] = working_tables

    class Config:
        """Pydantic config."""

        allow_mutation = False

    _validate_years = validator("years", allow_reuse=True)(_validate_years)
    _validate_tables = validator("tables", allow_reuse=True)(_validate_tables)


class GlueSettings(BaseModel):
    """
    An immutable pydantic model to validate Glue settings.

    Attributes:
        eia (bool): Include eia in glue settings.
        ferc1 (bool): Include ferc1 in glue settings.
    """

    eia: bool = True
    ferc1: bool = True

    class Config:
        """Pydantic config."""

        allow_mutation = False


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
    eia860: Eia860Settings = None
    eia923: Eia923Settings = None
    glue: GlueSettings = None

    class Config:
        """Pydantic config."""

        allow_mutation = False

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
            values["eia860"] = Eia860Settings()
            values["eia923"] = Eia923Settings()
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
        if values.get("ferc1") and (values.get("eia860") or values.get("eia923")):
            values["glue"] = GlueSettings()
        return values

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

    def get_datasets(cls):
        """Gets dictionary of dataset settings."""
        return vars(cls)
