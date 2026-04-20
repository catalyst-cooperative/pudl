"""Module for validating pudl ETL data configurations."""

import importlib.resources
import json
from enum import Enum, StrEnum, auto, unique
from pathlib import Path
from typing import Any, ClassVar, Self

import fsspec
import pandas as pd
import yaml
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
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

    model_config: ConfigDict = ConfigDict(frozen=True, extra="forbid")


class GenericDataConfig(FrozenBaseModel):
    """An abstract pydantic model for generic datasets.

    Each dataset must specify working partitions. A dataset can have an arbitrary number
    of partitions.
    """

    data_source: ClassVar[DataSource]
    """The DataSource metadata object for this dataset."""

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
                    f"{self.__class__.__name__} is missing required '{name}' field."
                ) from err

            # Partition should never be None -- should get a default value set in
            # the child classes based on the working partitions.
            if partition is None:
                raise ValueError(
                    f"'In {self.__class__.__name__} partition {name} is None."
                )

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
        :meth:`pandas.json_normalize`.
        """
        partitions = []
        for part_name in ["year_quarters", "years", "year_months"]:
            if hasattr(cls, part_name):
                partitions = [
                    {part_name.removesuffix("s"): part}
                    for part in getattr(cls, part_name)
                ]
        return partitions


class Ferc1DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate Ferc1DataConfig."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")

    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""

    @property
    def dbf_years(self):
        """Return validated years for which DBF data is available."""
        return [year for year in self.years if year <= 2020]

    @property
    def xbrl_years(self):
        """Return validated years for which XBRL data is available."""
        return [year for year in self.years if year >= 2021]


class Ferc714DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate Ferc714DataConfig."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")

    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""

    # The older 714 data is distributed as CSV files and has a different extraction
    # process than the FERC DBF extraction process.

    @property
    def csv_years(self):
        """Return validated years for which CSV data is available."""
        return [year for year in self.years if year < 2021]

    @property
    def xbrl_years(self):
        """Return validated years for which XBRL data is available."""
        return [year for year in self.years if year >= 2021]


class EpaCemsDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EPA CEMS data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("epacems")

    year_quarters: list[str] = data_source.working_partitions["year_quarters"]
    """The list of years-quarters to validate."""

    @field_validator("year_quarters")
    @classmethod
    def allow_all_keyword_year_quarters(cls, year_quarters):
        """Allow users to specify ['all'] to get all quarters."""
        if year_quarters == ["all"]:
            year_quarters = cls.data_source.working_partitions["year_quarters"]
        return year_quarters


class PhmsaGasDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate PHMSA data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("phmsagas")

    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""

    @property
    def extraction_years(self) -> list[int]:
        """The list of years to extract.

        These are different from the standard :attr:`years` because
        the oldest years (1970 - 1989) are published with multiple years
        in each tab. Instead of running the extraction step on each year
        and filtering on the year from each tab, we extract the whole tab
        all at once using the first year in the tab as the partition.
        """
        old_years = range(1970, 1990)
        first_year_tabs = [1970, 1980, 1982, 1984]
        return [
            year
            for year in self.years
            if (year not in old_years) or (year in first_year_tabs)
        ]


class Sec10kDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate SEC 10-K data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("sec10k")
    years: list[int] = data_source.working_partitions["years"]
    """The list of valid years for which SEC 10-K data is available."""
    tables: list[str] = data_source.working_partitions["tables"]


class NrelAtbDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate NREL ATB data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("nrelatb")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class Eia923DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 923 data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia923")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class Eia930DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 930 data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia930")
    half_years: list[str] = data_source.working_partitions["half_years"]
    """The list of half years to validate."""

    @field_validator("half_years")
    @classmethod
    def allow_all_keyword_half_years(cls, half_years):
        """Allow users to specify ['all'] to get all half-years."""
        if half_years == ["all"]:
            half_years = cls.data_source.working_partitions["half_years"]
        return half_years


class Eia861DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 861 data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia861")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class Eia860DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 860 data configuration.

    This model also checks 860m data configuration.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia860")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""

    eia860m: bool = True
    """Whether or not to incorporate an EIA-860m month."""

    all_eia860m_year_months: list[str] = DataSource.from_id(
        "eia860m"
    ).working_partitions["year_months"]
    """The list of all EIA-860m year-months."""

    eia860m_year_months: list[str] = Field(validate_default=True, default=[])
    """The 860m year-months to incorporate."""

    @field_validator("eia860m_year_months")
    @classmethod
    def add_other_860m_years(cls, v, info: ValidationInfo) -> list[str]:
        """Find extra years from EIA860m if applicable.

        There's a gap in reporting (after the new year but before the EIA 860 early
        release data in June) when we rely on two years worth of EIA860m data instead
        of just one. This function adds the last available month_year values for each
        year of 860m data that is not yet available in 860.

        """
        extra_eia860m_year_months_string = []
        if info.data["eia860m"]:
            all_eia860m_years = set(
                pd.to_datetime(info.data["all_eia860m_year_months"]).year
            )
            # The years in 860m that are not in 860
            extra_eia860m_years = {
                year
                for year in all_eia860m_years
                if year not in DataSource.from_id("eia860").working_partitions["years"]
            }
            # The years already listed as variables in eia860m_year_months
            years_in_v = set(pd.to_datetime(v).year)
            # The max year_month values available in 860m for each year not
            # covered by EIA860 (and not already listed in the eia860m_year_months
            # variable)
            extra_eia860m_year_months = [
                max(
                    date
                    for date in pd.to_datetime(info.data["all_eia860m_year_months"])
                    if date.year == year
                )
                for year in (extra_eia860m_years - years_in_v)
            ]
            if extra_eia860m_year_months:
                extra_eia860m_year_months_string = list(
                    pd.Series(extra_eia860m_year_months).dt.strftime("%Y-%m")
                )
        return v + extra_eia860m_year_months_string

    @field_validator("eia860m_year_months")
    @classmethod
    def no_repeat_years(cls, v, info: ValidationInfo) -> list[str]:
        """Make sure there are no duplicate 860m year values."""
        if info.data["eia860m"]:
            years_in_v = pd.to_datetime(v).year
            if len(years_in_v) != len(set(years_in_v)):
                raise ValueError(f"{v} contains duplicate year values.")
        return v

    @field_validator("eia860m_year_months")
    @classmethod
    def validate_eia860m_params(cls, v, info: ValidationInfo) -> list[str]:
        """Check that the year_month values for eia860m_year_months are valid."""
        eia860m_data_config = Eia860mDataConfig(year_months=v)
        return eia860m_data_config.year_months

    @field_validator("eia860m_year_months")
    @classmethod
    def only_years_not_in_eia860(cls, v, info: ValidationInfo) -> list[str]:
        """Ensure no EIA860m values are from years already in EIA860."""
        if info.data["eia860m"]:
            for year in pd.to_datetime(v).year.unique():
                if year in info.data["years"]:
                    raise ValueError(f"EIA860m year {year} available in EIA860")
        return v


class Eia860mDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 860m data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia860m")
    year_months: list[str] = data_source.working_partitions["year_months"]
    """The 860m year to date."""

    @field_validator("year_months")
    @classmethod
    def allow_all_keyword_year_months(cls, year_months):
        """Allow users to specify ['all'] to get all quarters."""
        if year_months == ["all"]:
            year_months = cls.data_source.working_partitions["year_months"]
        return year_months


class Eia757aDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 757a data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia757a")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class Eia191DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 191 data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia191")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class Eia176DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA 176 data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eia176")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class EiaAeoDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate EIA AEO data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("eiaaeo")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class VceRareDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate VCE RARE data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("vcerare")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""

    fips: bool = True
    """Include FIPS codes in VCE RARE Power Dataset."""


class CensusPepDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate Census PEP data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("censuspep")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class GlueDataConfig(FrozenBaseModel):
    """An immutable pydantic model to validate Glue data configuration."""

    eia: bool = True
    """Include eia in glue data configuration."""

    ferc1: bool = True
    """Include ferc1 in glue data configuration."""


@unique
class GridPathRaToolkitTechType(StrEnum):
    """Enum to constrain GridPath RA Toolkit technology types."""

    WIND = auto()
    SOLAR = auto()
    # Not yet implemented
    # THERMAL = auto()


@unique
class GridPathRaToolkitProcLevel(StrEnum):
    """Enum to constraint GridPath RA Toolkit processing levels."""

    EXTENDED = auto()
    # Not yet implemented
    # AGGREGATED = auto()
    # ORIGINAL = auto()


class GridPathRaToolkitDataConfig(GenericDataConfig):
    """An immutable pydantic model to validate GridPath RA Toolkit data configuration.

    Note that the default values for technology_types, processing_levels, and
    daily_weather are such that by default, all working partitions will be included.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("gridpathratoolkit")
    technology_types: list[str] = ["wind", "solar"]
    processing_levels: list[str] = ["extended"]
    daily_weather: bool = True

    @field_validator("technology_types", "processing_levels")
    @classmethod
    def deduplicate_list(cls, v):
        """Deduplicate technology type and processing level values."""
        return list(set(v))

    @field_validator("technology_types")
    @classmethod
    def allowed_technology_types(cls, v: list[str]) -> list[str]:
        """Ensure that technology types are valid."""
        for tech_type in v:
            if tech_type not in GridPathRaToolkitTechType:
                raise ValueError(f"{tech_type} is not a valid technology type.")
        return v

    @field_validator("processing_levels")
    @classmethod
    def allowed_processing_levels(cls, v: list[str]) -> list[str]:
        """Ensure that processing levels are valid."""
        for proc_level in v:
            if proc_level not in GridPathRaToolkitProcLevel:
                raise ValueError(f"{proc_level} is not a valid processing level.")
        return v

    @property
    def parts(self) -> list[str]:
        """Construct parts from selected technologies, processing levels, and daily weather."""
        parts = []
        if self.daily_weather:
            parts.append("daily_weather")
        if "solar" in self.technology_types and "extended" in self.processing_levels:
            parts.append("aggregated_extended_solar_capacity")
        if "wind" in self.technology_types and "extended" in self.processing_levels:
            parts.append("aggregated_extended_wind_capacity")
        if "solar" in self.technology_types and (
            "extended" in self.processing_levels
            or "aggregated" in self.processing_levels
        ):
            parts.append("solar_capacity_aggregations")
        if "wind" in self.technology_types and (
            "extended" in self.processing_levels
            or "aggregated" in self.processing_levels
        ):
            parts.append("wind_capacity_aggregations")
        return parts


class EiaDataConfig(FrozenBaseModel):
    """An immutable pydantic model to validate EIA datasets data configuration."""

    eia176: Eia176DataConfig | None = None
    eia191: Eia191DataConfig | None = None
    eia757a: Eia757aDataConfig | None = None
    eia860: Eia860DataConfig | None = None
    eia860m: Eia860mDataConfig | None = None
    eia861: Eia861DataConfig | None = None
    eia923: Eia923DataConfig | None = None
    eia930: Eia930DataConfig | None = None
    eiaaeo: EiaAeoDataConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def default_load_all(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If no datasets are specified default to all."""
        if not any(data.values()):
            data["eia176"] = Eia176DataConfig()
            data["eia191"] = Eia191DataConfig()
            data["eia757a"] = Eia757aDataConfig()
            data["eia860"] = Eia860DataConfig()
            data["eia860m"] = Eia860mDataConfig()
            data["eia861"] = Eia861DataConfig()
            data["eia923"] = Eia923DataConfig()
            data["eia930"] = Eia930DataConfig()
            data["eiaaeo"] = EiaAeoDataConfig()

        return data

    @model_validator(mode="before")
    @classmethod
    def check_eia_dependencies(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Make sure the dependencies between the eia datasets are satisfied.

        Dependencies:
        * eia923 requires eia860 for harvesting purposes.

        Args:
            values (Dict[str, BaseModel]): dataset data configuration.

        Returns:
            values (Dict[str, BaseModel]): dataset data configuration.
        """
        if not data.get("eia923") and data.get("eia860"):
            data["eia923"] = Eia923DataConfig(years=data["eia860"].years)

        if data.get("eia923") and not data.get("eia860"):
            available_years = Eia860DataConfig().years
            data["eia860"] = Eia860DataConfig(
                years=[year for year in data["eia923"].years if year in available_years]
            )
        return data


class Rus7DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate RUS-7 datasets data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("rus7")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class Rus12DataConfig(GenericDataConfig):
    """An immutable pydantic model to validate RUS Form 12 data configuration."""

    data_source: ClassVar[DataSource] = DataSource.from_id("rus12")
    years: list[int] = data_source.working_partitions["years"]
    """The list of years to validate."""


class PudlDataConfig(FrozenBaseModel):
    """An immutable pydantic model to validate PUDL Dataset data configuration."""

    eia: EiaDataConfig | None = None
    epacems: EpaCemsDataConfig | None = None
    ferc1: Ferc1DataConfig | None = None
    ferc714: Ferc714DataConfig | None = None
    glue: GlueDataConfig | None = None
    gridpathratoolkit: GridPathRaToolkitDataConfig | None = None
    nrelatb: NrelAtbDataConfig | None = None
    phmsagas: PhmsaGasDataConfig | None = None
    sec10k: Sec10kDataConfig | None = None
    vcerare: VceRareDataConfig | None = None
    censuspep: CensusPepDataConfig | None = None
    rus7: Rus7DataConfig | None = None
    rus12: Rus12DataConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def default_load_all(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If no datasets are specified default to all.

        Args:
            data: PUDL data configuration.

        Returns:
            Validated PUDL data configuration.
        """
        if not any(data.values()):
            data["eia"] = EiaDataConfig()
            data["epacems"] = EpaCemsDataConfig()
            data["ferc1"] = Ferc1DataConfig()
            data["ferc714"] = Ferc714DataConfig()
            data["glue"] = GlueDataConfig()
            data["gridpathratoolkit"] = GridPathRaToolkitDataConfig()
            data["nrelatb"] = NrelAtbDataConfig()
            data["phmsagas"] = PhmsaGasDataConfig()
            data["rus7"] = Rus7DataConfig()
            data["sec10k"] = Sec10kDataConfig()
            data["vcerare"] = VceRareDataConfig()
            data["censuspep"] = CensusPepDataConfig()
            data["rus12"] = Rus12DataConfig()

        return data

    @model_validator(mode="before")
    @classmethod
    def add_glue_data_config(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Add glue data configuration if ferc1 and eia data are both requested.

        Args:
            values (Dict[str, BaseModel]): PUDL data configuration.

        Returns:
            values (Dict[str, BaseModel]): PUDL data configuration.
        """
        ferc1 = bool(data.get("ferc1"))
        eia = bool(data.get("eia"))
        if ferc1 and eia:
            data["glue"] = GlueDataConfig(ferc1=ferc1, eia=eia)
        return data

    def get_datasets(self: Self):
        """Gets dictionary of PUDL data configuration."""
        return vars(self)

    def make_datasources_table(self: Self, data_store: Datastore) -> pd.DataFrame:
        """Compile a table of dataset information.

        There are three places we can look for information about a dataset:
        * the datastore (for DOIs, working partitions, etc)
        * the PUDL data configuration (for partitions that are used in the ETL)
        * the DataSource info (which is stored within the PUDL data configuration)

        The PUDL data configuration and the datastore have different levels of nesting -
        and therefore names for datasets. The nesting happens particularly with the EIA
        data.

        Args:
            data_store: An initialized PUDL Datastore from which the DOI's for each raw input
                dataset can be obtained.

        Returns:
            a dataframe describing the partitions and DOI's of each of the datasets in
            this PUDL data config object.
        """
        pudl_data_config = self.get_datasets()
        # grab all of the datasets that show up by name in the datastore
        pudl_data_config_in_datastore_format = {
            name: data_config
            for (name, data_config) in pudl_data_config.items()
            if name in data_store.get_known_datasets() and data_config is not None
        }
        # add the eia datasets that are nested inside of the eia data configuration
        if pudl_data_config.get("eia", False):
            pudl_data_config_in_datastore_format.update(
                {
                    "eia860": pudl_data_config["eia"].eia860,
                    "eia860m": pudl_data_config["eia"].eia860m,
                    "eia861": pudl_data_config["eia"].eia861,
                    "eia923": pudl_data_config["eia"].eia923,
                }
            )

        datasets = pudl_data_config_in_datastore_format.keys()
        df = pd.DataFrame(
            data={
                "datasource": datasets,
                "partitions": [
                    json.dumps(pudl_data_config_in_datastore_format[dataset].partitions)
                    for dataset in datasets
                ],
                "doi": [
                    str(
                        _zenodo_doi_to_url(
                            data_store.get_datapackage_descriptor(dataset).doi
                        )
                    )
                    for dataset in datasets
                ],
            }
        )
        df["pudl_version"] = pudl.__version__
        return df


class FercDbfToSqliteDataConfig(GenericDataConfig):
    """Base class for all FERC DBF-to-SQLite data config models.

    Declares the ``years`` and ``refyear`` attributes shared by every FERC DBF
    form so that :class:`~pudl.extract.dbf.FercDbfExtractor` can be typed
    against this base rather than the looser :class:`GenericDataConfig`.
    """

    years: list[int] = []
    """Years of DBF data to extract."""

    refyear: ClassVar[int]
    """Reference year used to build the destination schema; provided by each subclass."""


class Ferc1DbfToSqliteDataConfig(FercDbfToSqliteDataConfig):
    """An immutable Pydantic model to validate FERC 1 to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    """The list of years to validate."""

    refyear: ClassVar[int] = max(years)
    """The reference year for the dataset."""


class FercGenericXbrlToSqliteDataConfig(BaseSettings):
    """An immutable pydantic model to validate Ferc1 to SQLite data config."""

    years: list[int]
    """The list of years to validate."""


class Ferc1XbrlToSqliteDataConfig(FercGenericXbrlToSqliteDataConfig):
    """An immutable pydantic model to validate Ferc1 to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    """The list of years to validate."""


class Ferc2XbrlToSqliteDataConfig(FercGenericXbrlToSqliteDataConfig):
    """An immutable pydantic model to validate FERC from 2 XBRL to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc2")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    """The list of years to validate."""


class Ferc2DbfToSqliteDataConfig(FercDbfToSqliteDataConfig):
    """An immutable Pydantic model to validate FERC 2 to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc2")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    """The list of years to validate."""

    refyear: ClassVar[int] = max(years)
    """The reference year for the dataset."""


class Ferc6DbfToSqliteDataConfig(FercDbfToSqliteDataConfig):
    """An immutable Pydantic model to validate FERC 6 to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc6")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    """The list of years to validate."""

    refyear: ClassVar[int] = max(years)
    """The reference year for the dataset."""


class Ferc6XbrlToSqliteDataConfig(FercGenericXbrlToSqliteDataConfig):
    """An immutable pydantic model to validate FERC from 6 XBRL to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc6")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    """The list of years to validate."""


class Ferc60DbfToSqliteDataConfig(FercDbfToSqliteDataConfig):
    """An immutable Pydantic model to validate FERC 60 to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc60")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    """The list of years to validate."""

    refyear: ClassVar[int] = max(years)
    """The reference year for the dataset."""


class Ferc60XbrlToSqliteDataConfig(FercGenericXbrlToSqliteDataConfig):
    """An immutable pydantic model to validate FERC from 60 XBRL to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc60")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    """The list of years to validate."""


class Ferc714XbrlToSqliteDataConfig(FercGenericXbrlToSqliteDataConfig):
    """An immutable pydantic model to validate FERC from 714 XBRL to SQLite data config."""

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year >= 2021
    ]
    """The list of years to validate."""


class FercToSqliteDataConfig(BaseSettings):
    """An immutable pydantic model to validate FERC XBRL to SQLite data config."""

    ferc1_dbf: Ferc1DbfToSqliteDataConfig | None = None
    ferc1_xbrl: Ferc1XbrlToSqliteDataConfig | None = None
    ferc2_dbf: Ferc2DbfToSqliteDataConfig | None = None
    ferc2_xbrl: Ferc2XbrlToSqliteDataConfig | None = None
    ferc6_dbf: Ferc6DbfToSqliteDataConfig | None = None
    ferc6_xbrl: Ferc6XbrlToSqliteDataConfig | None = None
    ferc60_dbf: Ferc60DbfToSqliteDataConfig | None = None
    ferc60_xbrl: Ferc60XbrlToSqliteDataConfig | None = None
    ferc714_xbrl: Ferc714XbrlToSqliteDataConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def default_load_all(cls, data: dict[str, Any]) -> dict[str, Any]:
        """If no datasets are specified default to all."""
        if not any(data.values()):
            data["ferc1_dbf"] = Ferc1DbfToSqliteDataConfig()
            data["ferc1_xbrl"] = Ferc1XbrlToSqliteDataConfig()
            data["ferc2_dbf"] = Ferc2DbfToSqliteDataConfig()
            data["ferc2_xbrl"] = Ferc2XbrlToSqliteDataConfig()
            data["ferc6_dbf"] = Ferc6DbfToSqliteDataConfig()
            data["ferc6_xbrl"] = Ferc6XbrlToSqliteDataConfig()
            data["ferc60_dbf"] = Ferc60DbfToSqliteDataConfig()
            data["ferc60_xbrl"] = Ferc60XbrlToSqliteDataConfig()
            data["ferc714_xbrl"] = Ferc714XbrlToSqliteDataConfig()

        return data

    def get_xbrl_data_config(
        self, form_number: XbrlFormNumber
    ) -> FercGenericXbrlToSqliteDataConfig | None:
        """Return a list with all requested FERC XBRL to SQLite datasets.

        Args:
            form_number: Get data config by FERC form number.
        """
        # Get requested data config object
        match form_number:
            case XbrlFormNumber.FORM1:
                return self.ferc1_xbrl
            case XbrlFormNumber.FORM2:
                return self.ferc2_xbrl
            case XbrlFormNumber.FORM6:
                return self.ferc6_xbrl
            case XbrlFormNumber.FORM60:
                return self.ferc60_xbrl
            case XbrlFormNumber.FORM714:
                return self.ferc714_xbrl

        return None


class GlobalDataConfig(BaseSettings):
    """Main settings validation class."""

    ferc_to_sqlite: FercToSqliteDataConfig | None = None
    pudl: PudlDataConfig | None = None

    name: str | None = None
    title: str | None = None
    description: str | None = None
    version: str | None = None

    @classmethod
    def from_yaml(cls, path: str) -> "GlobalDataConfig":
        """Create a GlobalDataConfig instance from a yaml_file path.

        Args:
            path: path to a yaml file; this could be remote.

        Returns:
            An ETL settings object.
        """
        with fsspec.open(path) as f:
            yaml_file = yaml.safe_load(f)
        return cls.model_validate(yaml_file)

    @model_validator(mode="after")
    def validate_xbrl_years(self):
        """Ensure the XBRL years in PudlDataConfig align with FercToSqliteDataConfig.

        For each of the FERC forms that we are processing in PUDL, check to ensure
        that the years we are trying to process in the PUDL ETL are included in the
        XBRL to SQLite data config.
        """
        if self.pudl is None or self.ferc_to_sqlite is None:
            return self

        for which_ferc in ["ferc1", "ferc714"]:
            if (
                self.pudl is not None
                and self.ferc_to_sqlite is not None
                and (pudl_ferc := getattr(self.pudl, which_ferc))
                and (
                    sqlite_ferc := getattr(
                        self.ferc_to_sqlite,
                        f"{which_ferc}_xbrl",
                    )
                )
                and not set(pudl_ferc.xbrl_years).issubset(set(sqlite_ferc.years))
            ):
                raise AssertionError(
                    "You are trying to build a PUDL database with different XBRL years "
                    f"than the ferc_to_sqlite years for {which_ferc}.\nPUDL years: {pudl_ferc.xbrl_years}\n"
                    f"SQLite Years: {sqlite_ferc.years}"
                )
        return self

    @property
    def pudl_data_config(self) -> PudlDataConfig:
        """Return validated PUDL data config or raise if it is unavailable."""
        if self.pudl is None:
            raise ValueError("Missing PUDL data config in GlobalDataConfig.")
        return self.pudl

    def get_xbrl_data_config(
        self, form_number: XbrlFormNumber
    ) -> FercGenericXbrlToSqliteDataConfig | None:
        """Proxy FERC XBRL data config lookup through the canonical global config."""
        if self.ferc_to_sqlite is None:
            raise ValueError(
                "ferc_to_sqlite is not set in GlobalDataConfig. "
                "Ensure ferc_to_sqlite is configured before accessing this method."
            )
        return self.ferc_to_sqlite.get_xbrl_data_config(form_number)


def load_global_data_config(path: str | Path) -> GlobalDataConfig:
    """Load global data config from an arbitrary path.

    Expands ``~`` and resolves the path relative to the current working directory
    before passing it to :meth:`GlobalDataConfig.from_yaml`, which expects an absolute
    path or a URI. This wrapper exists so callers can pass relative or user-expanded
    paths without knowing those normalisation details.
    """
    return GlobalDataConfig.from_yaml(str(Path(path).expanduser().resolve()))


def load_packaged_global_data_config(data_config_filename: str) -> GlobalDataConfig:
    """Load a named global data config profile from ``pudl.package_data.settings``.

    Uses :mod:`importlib.resources` to locate the YAML file inside the installed
    package, so the lookup works correctly regardless of the current working directory
    or whether the package is installed as a zip. This wrapper exists so callers can
    refer to profiles by short name (e.g. ``"etl_full"``) without knowing the
    package-data path or the ``.yml`` extension.
    """
    settings_path = (
        importlib.resources.files("pudl.package_data.settings")
        / f"{data_config_filename}.yml"
    )
    return GlobalDataConfig.from_yaml(str(settings_path))


def _zenodo_doi_to_url(doi: ZenodoDoi) -> AnyHttpUrl:
    """Create a DOI URL out o a Zenodo DOI."""
    return AnyHttpUrl(f"https://doi.org/{doi}")
