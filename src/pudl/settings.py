"""Module for validating pudl etl settings."""
import json
from enum import Enum, unique
from typing import Any, ClassVar, Self

import fsspec
import pandas as pd
import yaml
from dagster import Field as DagsterField
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings

import pudl
from pudl.metadata.classes import DataSource
from pudl.workspace.datastore import Datastore, ZenodoDoi

logger = pudl.logging_helpers.get_logger(__name__)


@unique
class XbrlFormNumber(Enum):
    """Contains full list of supported FERC XBRL forms."""

    FORM1 = 1
    FORM2 = 2
    FORM6 = 6
    FORM60 = 60
    FORM714 = 714


class FrozenBaseModel(BaseModel):
    """BaseModel with global configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class GenericDatasetSettings(FrozenBaseModel):
    """An abstract pydantic model for generic datasets.

    Each dataset must specify working partitions. A dataset can have an arbitrary number
    of partitions.

    Args:
        disabled: if true, skip processing this dataset.
    """

    disabled: bool = False
    data_source: ClassVar[DataSource]

    @model_validator(mode="after")
    def validate_partitions(self: Self):
        """Ensure that partitions and their values are valid.

        Checks that:

        * all partitions specified by the data source exist,
        * partitions are not None
        * only known to be working partition values are specified
        * no duplicate partition values are specified

        """
        for name, working_partitions in self.data_source.working_partitions.items():
            try:
                partition = getattr(self, name)
            except KeyError as err:
                raise ValueError(
                    f"{self.__name__} is missing required '{name}' field."
                ) from err

            # Partition should never be None -- should get a default value set in
            # the child classes based on the working partitions.
            if partition is None:
                raise ValueError(f"'In {self.__name__} partition {name} is None.")

            if nonworking_partitions := list(set(partition) - set(working_partitions)):
                raise ValueError(f"'{nonworking_partitions}' {name} are not available.")

            if len(partition) != len(set(partition)):
                raise ValueError(
                    f"'Duplicate values found in partition {name}: {partition}'"
                )

        return self

    @property
    def partitions(cls) -> list[None | dict[str, str]]:  # noqa: N805
        """Return list of dictionaries representing individual partitions.

        Convert a list of partitions into a list of dictionaries of partitions. This is
        intended to be used to store partitions in a format that is easy to use with
        ``pd.json_normalize``.
        """
        partitions = []
        for part_name in ["year_quarters", "years", "year_months"]:
            if hasattr(cls, part_name):
                partitions = [
                    {part_name.removesuffix("s"): part}
                    for part in getattr(cls, part_name)
                ]
        return partitions


class Ferc1Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate Ferc1Settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")

    years: list[int] = data_source.working_partitions["years"]

    @property
    def dbf_years(self):
        """Return validated years for which DBF data is available."""
        return [year for year in self.years if year <= 2020]

    @property
    def xbrl_years(self):
        """Return validated years for which DBF data is available."""
        return [year for year in self.years if year >= 2021]


class Ferc714Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate Ferc714Settings.

    Args:
        data_source: DataSource metadata object
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")

    # Note: Only older data is currently supported. Starting in 2021 FERC-714 is being
    # published as XBRL, and we haven't integrated it. The older data is published as
    # monolithic CSV files, so asking for any year processes all of them.
    years: list[int] = data_source.working_partitions["years"]


class EpaCemsSettings(GenericDatasetSettings):
    """An immutable pydantic model to validate EPA CEMS settings.

    Args:
        data_source: DataSource metadata object
        year_quarters: list of year_quarters to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("epacems")
    year_quarters: list[str] = data_source.working_partitions["year_quarters"]

    @field_validator("year_quarters")
    @classmethod
    def allow_all_keyword_year_quarters(cls, year_quarters):
        """Allow users to specify ['all'] to get all quarters."""
        if year_quarters == ["all"]:
            year_quarters = cls.data_source.working_partitions["year_quarters"]
        return year_quarters


class PhmsaGasSettings(GenericDatasetSettings):
    """An immutable pydantic model to validate PHMSA settings.

    Args:
        data_source: DataSource metadata object
        years: list of zipped data start years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("phmsagas")
    years: list[int] = data_source.working_partitions["years"]


class Eia923Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 923 settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia923")
    years: list[int] = data_source.working_partitions["years"]


class Eia861Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 861 settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        transform_functions: list of transform functions to be applied to eia861
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia861")
    years: list[int] = data_source.working_partitions["years"]


class Eia860Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 860 settings.

    This model also check 860m settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        eia860m: whether or not to incorporate an EIA-860m month.
        eia860m_year_month ClassVar[str]: The 860m year-month to incorporate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia860")
    years: list[int] = data_source.working_partitions["years"]

    eia860m: bool = True
    eia860m_year_month: ClassVar[str] = max(
        DataSource.from_id("eia860m").working_partitions["year_months"]
    )

    @field_validator("eia860m")
    @classmethod
    def check_eia860m_year_month(cls, eia860m: bool) -> bool:
        """Check 860m date-year is exactly one year after most recent working 860 year.

        Args:
            eia860m: True if 860m is requested.

        Returns:
            eia860m: True if 860m is requested.

        Raises:
            ValueError: the 860m date is within 860 working years.
        """
        eia860m_year = pd.to_datetime(cls.eia860m_year_month).year
        expected_year = max(cls.data_source.working_partitions["years"]) + 1
        if eia860m and (eia860m_year != expected_year):
            raise AssertionError(
                f"Attempting to integrate an eia860m year "
                f"({eia860m_year}) from {cls.eia860m_year_month} not immediately following"
                f"the eia860 years: {cls.data_source.working_partitions['years']}. "
                f"Consider switching eia860m parameter to False."
            )
        return eia860m


class Eia860mSettings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 860m settings.

    Args:
        data_source: DataSource metadata object
        year_months ClassVar[str]: The 860m year to date.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia860m")
    year_months: list[str] = data_source.working_partitions["year_months"]

    @field_validator("year_months")
    @classmethod
    def allow_all_keyword_year_months(cls, year_months):
        """Allow users to specify ['all'] to get all quarters."""
        if year_months == ["all"]:
            year_months = cls.data_source.working_partitions["year_months"]
        return year_months


class GlueSettings(FrozenBaseModel):
    """An immutable pydantic model to validate Glue settings.

    Args:
        eia: Include eia in glue settings.
        ferc1: Include ferc1 in glue settings.
    """

    eia: bool = True
    ferc1: bool = True


class EiaSettings(FrozenBaseModel):
    """An immutable pydantic model to validate EIA datasets settings.

    Args:
        eia860: Immutable pydantic model to validate eia860 settings.
        eia861: Immutable pydantic model to validate eia861 settings.
        eia923: Immutable pydantic model to validate eia923 settings.
    """

    eia860: Eia860Settings | None = None
    eia860m: Eia860mSettings | None = None
    eia861: Eia861Settings | None = None
    eia923: Eia923Settings | None = None

    @model_validator(mode="before")
    @classmethod
    def default_load_all(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If no datasets are specified default to all."""
        if not any(data.values()):
            data["eia860"] = Eia860Settings()
            data["eia860m"] = Eia860mSettings()
            data["eia861"] = Eia861Settings()
            data["eia923"] = Eia923Settings()

        return data

    @model_validator(mode="before")
    @classmethod
    def check_eia_dependencies(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Make sure the dependencies between the eia datasets are satisfied.

        Dependencies:
        * eia923 requires eia860 for harvesting purposes.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        if not data.get("eia923") and data.get("eia860"):
            data["eia923"] = Eia923Settings(years=data["eia860"].years)

        if data.get("eia923") and not data.get("eia860"):
            available_years = Eia860Settings().years
            data["eia860"] = Eia860Settings(
                years=[year for year in data["eia923"].years if year in available_years]
            )
        return data


class DatasetsSettings(FrozenBaseModel):
    """An immutable pydantic model to validate PUDL Dataset settings.

    Args:
        ferc1: Immutable pydantic model to validate ferc1 settings.
        eia: Immutable pydantic model to validate eia(860, 923) settings.
        glue: Immutable pydantic model to validate glue settings.
        epacems: Immutable pydantic model to validate epacems settings.
    """

    eia: EiaSettings | None = None
    epacems: EpaCemsSettings | None = None
    ferc1: Ferc1Settings | None = None
    ferc714: Ferc714Settings | None = None
    glue: GlueSettings | None = None
    phmsagas: PhmsaGasSettings | None = None

    @model_validator(mode="before")
    @classmethod
    def default_load_all(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If no datasets are specified default to all.

        Args:
            data: dataset settings inputs.

        Returns:
            Validated dataset settings inputs.
        """
        if not any(data.values()):
            data["eia"] = EiaSettings()
            data["epacems"] = EpaCemsSettings()
            data["ferc1"] = Ferc1Settings()
            data["ferc714"] = Ferc714Settings()
            data["glue"] = GlueSettings()
            data["phmsagas"] = PhmsaGasSettings()

        return data

    @model_validator(mode="before")
    @classmethod
    def add_glue_settings(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Add glue settings if ferc1 and eia data are both requested.

        Args:
            values (Dict[str, BaseModel]): dataset settings.

        Returns:
            values (Dict[str, BaseModel]): dataset settings.
        """
        ferc1 = bool(data.get("ferc1"))
        eia = bool(data.get("eia"))
        if ferc1 and eia:
            data["glue"] = GlueSettings(ferc1=ferc1, eia=eia)
        return data

    def get_datasets(self: Self):
        """Gets dictionary of dataset settings."""
        return vars(self)

    def make_datasources_table(self: Self, ds: Datastore) -> pd.DataFrame:
        """Compile a table of dataset information.

        There are three places we can look for information about a dataset:
        * the datastore (for DOIs, working partitions, etc)
        * the ETL settings (for partitions that are used in the ETL)
        * the DataSource info (which is stored within the ETL settings)

        The ETL settings and the datastore have different levels of nesting - and
        therefore names for datasets. The nesting happens particularly with the EI
        data. There are three EIA datasets right now eia923, eia860 and eia860m.
        eia860m is a monthly update of a few tables in the larger eia860 dataset.

        Args:
            ds: An initalized PUDL Datastore from which the DOI's for each raw input
                dataset can be obtained.

        Returns:
            a dataframe describing the partitions and DOI's of each of the datasets in
            this settings object.
        """
        datasets_settings = self.get_datasets()
        # grab all of the datasets that show up by name in the datastore
        datasets_in_datastore_format = {
            name: setting
            for (name, setting) in datasets_settings.items()
            if name in ds.get_known_datasets() and setting is not None
        }
        # add the eia datasets that are nested inside of the eia settings
        if datasets_settings.get("eia", False):
            datasets_in_datastore_format.update(
                {
                    "eia860": datasets_settings["eia"].eia860,
                    "eia860m": datasets_settings["eia"].eia860m,
                    "eia861": datasets_settings["eia"].eia861,
                    "eia923": datasets_settings["eia"].eia923,
                }
            )

        datasets = datasets_in_datastore_format.keys()
        df = pd.DataFrame(
            data={
                "datasource": datasets,
                "partitions": [
                    json.dumps(datasets_in_datastore_format[dataset].partitions)
                    for dataset in datasets
                ],
                "doi": [
                    str(_zenodo_doi_to_url(ds.get_datapackage_descriptor(dataset).doi))
                    for dataset in datasets
                ],
            }
        )
        df["pudl_version"] = pudl.__version__
        return df


class Ferc1DbfToSqliteSettings(GenericDatasetSettings):
    """An immutable Pydantic model to validate FERC 1 to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]

    refyear: ClassVar[int] = max(years)


class FercGenericXbrlToSqliteSettings(BaseSettings):
    """An immutable pydantic model to validate Ferc1 to SQLite settings.

    Args:
        taxonomy: URL of XBRL taxonomy used to create structure of SQLite DB.
        years: list of years to validate.
        disabled: if True, skip processing this dataset.
    """

    taxonomy: str
    years: list[int]
    disabled: bool = False


class Ferc1XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate Ferc1 to SQLite settings.

    Args:
        taxonomy: URL of taxonomy used to .
        years: list of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    taxonomy: str = "https://eCollection.ferc.gov/taxonomy/form1/2022-01-01/form/form1/form-1_2022-01-01.xsd"


class Ferc2XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 2 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc2")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    taxonomy: str = "https://eCollection.ferc.gov/taxonomy/form2/2022-01-01/form/form2/form-2_2022-01-01.xsd"


class Ferc2DbfToSqliteSettings(GenericDatasetSettings):
    """An immutable Pydantic model to validate FERC 2 to SQLite settings.

    Args:
        years: List of years to validate.
        disabled: if True, skip processing this dataset.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc2")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]

    refyear: ClassVar[int] = max(years)


class Ferc6DbfToSqliteSettings(GenericDatasetSettings):
    """An immutable Pydantic model to validate FERC 6 to SQLite settings.

    Args:
        years: List of years to validate.
        disabled: if True, skip processing this dataset.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc6")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    disabled: bool = False

    refyear: ClassVar[int] = max(years)


class Ferc6XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 6 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc6")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    taxonomy: str = "https://eCollection.ferc.gov/taxonomy/form6/2022-01-01/form/form6/form-6_2022-01-01.xsd"


class Ferc60DbfToSqliteSettings(GenericDatasetSettings):
    """An immutable Pydantic model to validate FERC 60 to SQLite settings.

    Args:
        years: List of years to validate.
        disabled: if True, skip processing this dataset.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc60")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    disabled: bool = False

    refyear: ClassVar[int] = max(years)


class Ferc60XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 60 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc60")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    taxonomy: str = "https://eCollection.ferc.gov/taxonomy/form60/2022-01-01/form/form60/form-60_2022-01-01.xsd"


class Ferc714XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 714 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")
    years: list[int] = [2021, 2022]
    taxonomy: str = "https://eCollection.ferc.gov/taxonomy/form714/2022-01-01/form/form714/form-714_2022-01-01.xsd"


class FercToSqliteSettings(BaseSettings):
    """An immutable pydantic model to validate FERC XBRL to SQLite settings.

    Args:
        ferc1_dbf_to_sqlite_settings: Settings for converting FERC 1 DBF data to SQLite.
        ferc1_xbrl_to_sqlite_settings: Settings for converting FERC 1 XBRL data to
            SQLite.
        other_xbrl_forms: List of non-FERC1 forms to convert from XBRL to SQLite.
    """

    ferc1_dbf_to_sqlite_settings: Ferc1DbfToSqliteSettings | None = None
    ferc1_xbrl_to_sqlite_settings: Ferc1XbrlToSqliteSettings | None = None
    ferc2_dbf_to_sqlite_settings: Ferc2DbfToSqliteSettings | None = None
    ferc2_xbrl_to_sqlite_settings: Ferc2XbrlToSqliteSettings | None = None
    ferc6_dbf_to_sqlite_settings: Ferc6DbfToSqliteSettings | None = None
    ferc6_xbrl_to_sqlite_settings: Ferc6XbrlToSqliteSettings | None = None
    ferc60_dbf_to_sqlite_settings: Ferc60DbfToSqliteSettings | None = None
    ferc60_xbrl_to_sqlite_settings: Ferc60XbrlToSqliteSettings | None = None
    ferc714_xbrl_to_sqlite_settings: Ferc714XbrlToSqliteSettings | None = None

    @model_validator(mode="before")
    @classmethod
    def default_load_all(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If no datasets are specified default to all."""
        if not any(data.values()):
            data["ferc1_dbf_to_sqlite_settings"] = Ferc1DbfToSqliteSettings()
            data["ferc1_xbrl_to_sqlite_settings"] = Ferc1XbrlToSqliteSettings()
            data["ferc2_dbf_to_sqlite_settings"] = Ferc2DbfToSqliteSettings()
            data["ferc2_xbrl_to_sqlite_settings"] = Ferc2XbrlToSqliteSettings()
            data["ferc6_dbf_to_sqlite_settings"] = Ferc6DbfToSqliteSettings()
            data["ferc6_xbrl_to_sqlite_settings"] = Ferc6XbrlToSqliteSettings()
            data["ferc60_dbf_to_sqlite_settings"] = Ferc60DbfToSqliteSettings()
            data["ferc60_xbrl_to_sqlite_settings"] = Ferc60XbrlToSqliteSettings()
            data["ferc714_xbrl_to_sqlite_settings"] = Ferc714XbrlToSqliteSettings()

        return data

    def get_xbrl_dataset_settings(
        self, form_number: XbrlFormNumber
    ) -> FercGenericXbrlToSqliteSettings:
        """Return a list with all requested FERC XBRL to SQLite datasets.

        Args:
            form_number: Get settings by FERC form number.
        """
        # Get requested settings object
        match form_number:
            case XbrlFormNumber.FORM1:
                settings = self.ferc1_xbrl_to_sqlite_settings
            case XbrlFormNumber.FORM2:
                settings = self.ferc2_xbrl_to_sqlite_settings
            case XbrlFormNumber.FORM6:
                settings = self.ferc6_xbrl_to_sqlite_settings
            case XbrlFormNumber.FORM60:
                settings = self.ferc60_xbrl_to_sqlite_settings
            case XbrlFormNumber.FORM714:
                settings = self.ferc714_xbrl_to_sqlite_settings

        return settings


class EtlSettings(BaseSettings):
    """Main settings validation class."""

    ferc_to_sqlite_settings: FercToSqliteSettings | None = None
    datasets: DatasetsSettings | None = None

    name: str | None = None
    title: str | None = None
    description: str | None = None
    version: str | None = None

    # This is list of fsspec compatible paths to publish the output datasets to.
    publish_destinations: list[str] = []

    @classmethod
    def from_yaml(cls, path: str) -> "EtlSettings":
        """Create an EtlSettings instance from a yaml_file path.

        Args:
            path: path to a yaml file; this could be remote.

        Returns:
            An ETL settings object.
        """
        with fsspec.open(path) as f:
            yaml_file = yaml.safe_load(f)
        return cls.model_validate(yaml_file)


def _convert_settings_to_dagster_config(settings_dict: dict[str, Any]) -> None:
    """Recursively convert a dictionary of dataset settings to dagster config in place.

    For each partition parameter in a :class:`GenericDatasetSettings` subclass, create a
    corresponding :class:`DagsterField`. By default the :class:`GenericDatasetSettings`
    subclasses will default to include all working paritions if the partition value is
    None. Get the value type so dagster can do some basic type checking in the UI.

    Args:
        settings_dict: dictionary of datasources and their parameters.
    """
    for key, value in settings_dict.items():
        if isinstance(value, dict):
            _convert_settings_to_dagster_config(value)
        else:
            settings_dict[key] = DagsterField(type(value), default_value=value)


def create_dagster_config(settings: GenericDatasetSettings) -> dict[str, DagsterField]:
    """Create a dictionary of dagster config out of a :class:`GenericDatasetsSettings`.

    Args:
        settings: A dataset settings object, subclassed from
            :class:`GenericDatasetSettings`.

    Returns:
        A dictionary of :class:`DagsterField` objects.
    """
    settings_dict = settings.model_dump()
    _convert_settings_to_dagster_config(settings_dict)
    return settings_dict


def _zenodo_doi_to_url(doi: ZenodoDoi) -> AnyHttpUrl:
    """Create a DOI URL out o a Zenodo DOI."""
    return AnyHttpUrl(f"https://doi.org/{doi}")
