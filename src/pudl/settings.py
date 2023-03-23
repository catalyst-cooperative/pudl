"""Module for validating pudl etl settings."""
import itertools
import json
import pathlib
from enum import Enum, unique
from typing import ClassVar, Literal

import pandas as pd
import yaml
from pydantic import AnyHttpUrl
from pydantic import BaseModel as PydanticBaseModel
from pydantic import BaseSettings, root_validator, validator

import pudl
import pudl.workspace.setup
from pudl.helpers import flatten_list
from pudl.metadata.classes import DataSource
from pudl.metadata.constants import DBF_TABLES_FILENAMES, XBRL_TABLES
from pudl.metadata.resources.eia861 import TABLE_DEPENDENCIES
from pudl.workspace.datastore import Datastore


@unique
class XbrlFormNumber(Enum):
    """Contains full list of supported FERC XBRL forms."""

    FORM1 = 1
    FORM2 = 2
    FORM6 = 6
    FORM60 = 60
    FORM714 = 714


class BaseModel(PydanticBaseModel):
    """BaseModel with global configuration."""

    class Config:
        """Pydantic config."""

        allow_mutation = False
        extra = "forbid"


class GenericDatasetSettings(BaseModel):
    """An abstract pydantic model for generic datasets.

    Each dataset must specify working tables and partitions. A dataset can have an
    arbitrary number of partitions.
    """

    tables: list[str]

    @root_validator
    def validate_partitions(cls, partitions):  # noqa: N805
        """Validate the requested data partitions.

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
                    f"'{partitions_not_working}' {name} are not available."
                )
            partitions[name] = sorted(set(partition))
        return partitions

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables are available."""
        tables_not_working = list(set(tables) - set(cls.data_source.get_resource_ids()))
        if tables_not_working:
            raise ValueError(f"'{tables_not_working}' tables are not available.")
        return sorted(set(tables))

    @property
    def partitions(cls) -> list[None | dict[str, str]]:  # noqa: N805
        """Return list of dictionaries representing individual partitions.

        Convert a list of partitions into a list of dictionaries of partitions. This is
        intended to be used to store partitions in a format that is easy to use with
        ``pd.json_normalize``.
        """
        partitions = []
        if hasattr(cls, "years") and hasattr(cls, "states"):
            partitions = [
                {"year": year, "state": state}
                for year, state in itertools.product(cls.years, cls.states)
            ]
        elif hasattr(cls, "years"):
            partitions = [{"year": part} for part in cls.years]
        return partitions


class Ferc1Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate Ferc1Settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        tables: list of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")

    years: list[int] = data_source.working_partitions["years"]
    tables: list[str] = data_source.get_resource_ids()

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables are available."""
        unavailable_tables = list(set(tables) - set(cls.data_source.get_resource_ids()))
        if unavailable_tables:
            raise ValueError(f"'{unavailable_tables}' tables are not available.")
        return sorted(set(tables))

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
        tables: list of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")

    tables: list[str] = data_source.get_resource_ids()
    years: list[int] = data_source.working_partitions[
        "years"
    ]  # Years only apply to XBRL


class EpaCemsSettings(GenericDatasetSettings):
    """An immutable pydantic model to validate EPA CEMS settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        states: list of states to validate.
        tables: list of tables to validate.
        partition: Whether to output year-state partitioned Parquet files. If True,
            all available threads / CPUs will be used in parallel.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("epacems")

    years: list[int] = data_source.working_partitions["years"]
    states: list[str] = data_source.working_partitions["states"]
    tables: list[str] = data_source.get_resource_ids()
    partition: bool = False

    @validator("states")
    def allow_all_keyword(cls, states):  # noqa: N805
        """Allow users to specify ['all'] to get all states."""
        if states == ["all"]:
            states = cls.data_source.working_partitions["states"]
        return states


class Eia923Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 923 settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        tables: list of tables to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia923")

    years: list[int] = data_source.working_partitions["years"]
    tables: list[str] = data_source.get_resource_ids()


class Eia861Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 861 settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        tables: list of tables to validate.
        transform_functions: list of transform functions to be applied to eia861
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia861")

    years: list[int] = data_source.working_partitions["years"]
    tables: list[str] = data_source.get_resource_ids()
    transform_functions: list[str]

    @root_validator(pre=True)
    def generate_transform_functions(cls, values):  # noqa: N805
        """Map tables to transform functions.

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
                    [
                        tf_func
                        for tf_func, tables in TABLE_DEPENDENCIES.items()
                        if table in tables
                    ]
                )

        values["transform_functions"] = sorted(set(transform_functions))

        return values


class Eia860Settings(GenericDatasetSettings):
    """An immutable pydantic model to validate EIA 860 settings.

    This model also check 860m settings.

    Args:
        data_source: DataSource metadata object
        years: list of years to validate.
        tables: list of tables to validate.

        eia860m_date ClassVar[str]: The 860m year to date.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("eia860")
    eia860m_data_source: ClassVar[DataSource] = DataSource.from_id("eia860m")
    eia860m_date: ClassVar[str] = eia860m_data_source.working_partitions["year_month"]

    years: list[int] = data_source.working_partitions["years"]
    tables: list[str] = data_source.get_resource_ids()
    eia860m: bool = True

    @validator("eia860m")
    def check_eia860m_date(cls, eia860m: bool) -> bool:  # noqa: N805
        """Check 860m date-year is exactly one year after most recent working 860 year.

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
                """Attempting to integrate an eia860m year """
                f"""({eia860m_year}) from {cls.eia860m_date} not immediately following """
                f"""the eia860 years: {cls.data_source.working_partitions["years"]}. """
                """Consider switching eia860m parameter to False."""
            )
        return eia860m


class GlueSettings(BaseModel):
    """An immutable pydantic model to validate Glue settings.

    Args:
        eia: Include eia in glue settings.
        ferc1: Include ferc1 in glue settings.
    """

    eia: bool = True
    ferc1: bool = True


class EiaSettings(BaseModel):
    """An immutable pydantic model to validate EIA datasets settings.

    Args:
        eia860: Immutable pydantic model to validate eia860 settings.
        eia923: Immutable pydantic model to validate eia923 settings.
    """

    eia860: Eia860Settings = None
    eia923: Eia923Settings = None

    @root_validator(pre=True)
    def default_load_all(cls, values):  # noqa: N805
        """If no datasets are specified default to all.

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
        """Make sure the dependencies between the eia datasets are satisfied.

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
                tables=["boiler_fuel_eia923", "generation_eia923"], years=eia860.years
            )

        if eia923 and not eia860:
            values["eia860"] = Eia860Settings(years=eia923.years)
        return values


class DatasetsSettings(BaseModel):
    """An immutable pydantic model to validate PUDL Dataset settings.

    Args:
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
        """If no datasets are specified default to all.

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
        """Add glue settings if ferc1 and eia data are both requested.

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

    def make_datasources_table(self, ds: Datastore) -> pd.DataFrame:
        """Compile a table of dataset information.

        There are three places we can look for information about a dataset:
        * the datastore (for DOIs, working partitions, etc)
        * the ETL settings (for partitions that are used in the ETL)
        * the DataSource info (which is stored within the ETL settings)

        The ETL settings and the datastore have different levels of nesting - and therefor
        names for datasets. The nesting happens particularly with the EIA data. There
        are three EIA datasets right now - eia923, eia860 and eia860m. eia860m is a monthly
        update of a few tables in the larger eia860 dataset.

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
                    "eia923": datasets_settings["eia"].eia923,
                    "eia860": datasets_settings["eia"].eia860,
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
                    _make_doi_clickable(ds.get_datapackage_descriptor(dataset).doi)
                    for dataset in datasets
                ],
            }
        )
        # add in EIA860m if eia in general is in the settings and the 860m bool is True
        special_nested_datasets = pd.DataFrame()
        if (
            datasets_settings.get("eia", False)
            and datasets_settings["eia"].eia860.eia860m
        ):
            special_nested_datasets = pd.DataFrame(
                data={
                    "datasource": ["eia860m"],
                    "partitions": [
                        json.dumps(
                            datasets_in_datastore_format[
                                "eia860"
                            ].eia860m_data_source.working_partitions
                        )
                    ],
                    "doi": [
                        _make_doi_clickable(
                            ds.get_datapackage_descriptor("eia860m").doi
                        )
                    ],
                }
            )
        df = pd.concat([df, special_nested_datasets]).reset_index(drop=True)
        df["pudl_version"] = pudl.__version__
        return df


class Ferc1DbfToSqliteSettings(GenericDatasetSettings):
    """An immutable Pydantic model to validate FERC 1 to SQLite settings.

    Args:
        tables: List of tables to validate.
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc1")
    years: list[int] = [
        year for year in data_source.working_partitions["years"] if year <= 2020
    ]
    tables: list[str] = sorted(list(DBF_TABLES_FILENAMES.keys()))

    refyear: ClassVar[int] = max(years)

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables."""
        default_tables = sorted(list(DBF_TABLES_FILENAMES.keys()))
        tables_not_working = list(set(tables) - set(default_tables))
        if len(tables_not_working) > 0:
            raise ValueError(f"'{tables_not_working}' tables are not available.")
        return sorted(set(tables))


class FercGenericXbrlToSqliteSettings(BaseSettings):
    """An immutable pydantic model to validate Ferc1 to SQLite settings.

    Args:
        taxonomy: URL of XBRL taxonomy used to create structure of SQLite DB.
        tables: list of tables to validate.
        years: list of years to validate.
    """

    taxonomy: AnyHttpUrl
    tables: list[str] | None = None
    years: list[int]


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
    taxonomy: AnyHttpUrl = "https://eCollection.ferc.gov/taxonomy/form1/2022-01-01/form/form1/form-1_2022-01-01.xsd"
    tables: list[str] = XBRL_TABLES

    @validator("tables")
    def validate_tables(cls, tables):  # noqa: N805
        """Validate tables."""
        default_tables = sorted(list(XBRL_TABLES))
        tables_not_working = list(set(tables) - set(default_tables))
        if len(tables_not_working) > 0:
            raise ValueError(f"'{tables_not_working}' tables are not available.")
        return sorted(set(tables))


class Ferc2XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 2 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc2")
    years: list[int] = data_source.working_partitions["years"]
    taxonomy: AnyHttpUrl = "https://eCollection.ferc.gov/taxonomy/form2/2022-01-01/form/form2/form-2_2022-01-01.xsd"


class Ferc6XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 6 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc6")
    years: list[int] = data_source.working_partitions["years"]
    taxonomy: AnyHttpUrl = "https://eCollection.ferc.gov/taxonomy/form6/2022-01-01/form/form6/form-6_2022-01-01.xsd"


class Ferc60XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 60 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc60")
    years: list[int] = data_source.working_partitions["years"]
    taxonomy: AnyHttpUrl = "https://eCollection.ferc.gov/taxonomy/form60/2022-01-01/form/form60/form-60_2022-01-01.xsd"


class Ferc714XbrlToSqliteSettings(FercGenericXbrlToSqliteSettings):
    """An immutable pydantic model to validate FERC from 714 XBRL to SQLite settings.

    Args:
        years: List of years to validate.
    """

    data_source: ClassVar[DataSource] = DataSource.from_id("ferc714")
    years: list[int] = [2021]
    taxonomy: AnyHttpUrl = "https://eCollection.ferc.gov/taxonomy/form714/2022-01-01/form/form714/form-714_2022-01-01.xsd"


class FercToSqliteSettings(BaseSettings):
    """An immutable pydantic model to validate FERC XBRL to SQLite settings.

    Args:
        ferc1_dbf_to_sqlite_settings: Settings for converting FERC 1 DBF data to SQLite.
        ferc1_xbrl_to_sqlite_settings: Settings for converting FERC 1 XBRL data to SQLite.
        other_xbrl_forms: List of non-FERC1 forms to convert from XBRL to SQLite.
    """

    ferc1_dbf_to_sqlite_settings: Ferc1DbfToSqliteSettings = None
    ferc1_xbrl_to_sqlite_settings: Ferc1XbrlToSqliteSettings = None
    ferc2_xbrl_to_sqlite_settings: Ferc2XbrlToSqliteSettings = None
    ferc6_xbrl_to_sqlite_settings: Ferc6XbrlToSqliteSettings = None
    ferc60_xbrl_to_sqlite_settings: Ferc60XbrlToSqliteSettings = None
    ferc714_xbrl_to_sqlite_settings: Ferc714XbrlToSqliteSettings = None

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

    ferc_to_sqlite_settings: FercToSqliteSettings = None
    datasets: DatasetsSettings = None

    name: str = None
    title: str = None
    description: str = None
    version: str = None

    pudl_in: str = pudl.workspace.setup.get_defaults()["pudl_in"]
    pudl_out: str = pudl.workspace.setup.get_defaults()["pudl_out"]

    @root_validator(pre=False)
    def raw_table_validation_ferc1(cls, field_values):
        """Ensure FERC to SQLite and PUDL ETL settings are internally self-consistent.

        If FERC Form 1 tables are specified in both the :class:`FercToSqliteSettings`
        and :class:`Ferc1Settings` within a single :class:`EtlSettings` object, ensure
        that any FERC Form 1 tables required by the PUDL ETL will be produced by the
        FERC to SQLite extraction. Otherwise raise a validation error.

        To prevent this validation error, you can either remove one of the sets of
        tables (if you aren't performing both steps of the ETL) or ensure that the
        specified sets of tables are compatible.
        """
        # only check if we are actually loading any pudl tables. check for datasets
        # first bc default null is None, which will have no ferc1 attribute
        if (
            not field_values["datasets"]
            or (field_values["datasets"] and not field_values["datasets"].ferc1)
            or not field_values["ferc_to_sqlite_settings"]
        ):
            return field_values
        ferc1_settings = field_values["datasets"].ferc1
        ferc1_xbrl_to_sqlite_settings = field_values[
            "ferc_to_sqlite_settings"
        ].ferc1_xbrl_to_sqlite_settings
        ferc1_dbf_to_sqlite_settings = field_values[
            "ferc_to_sqlite_settings"
        ].ferc1_dbf_to_sqlite_settings

        pudl_etl_tables = ferc1_settings.tables
        table_map = {
            pudl_table: raw_dict
            for (
                pudl_table,
                raw_dict,
            ) in pudl.extract.ferc1.TABLE_NAME_MAP_FERC1.items()
            if pudl_table in pudl_etl_tables
        }

        def _get_tables_from_table_map_by_source(
            table_map: dict[str, dict[Literal["dbf", "xbrl"], str | list[str]]],
            source_ferc1: Literal["dbf", "xbrl"],
        ) -> set:
            """Convert the table_map into a set of source table names.

            We grab all of the raw tables needed from the FERC1 sources from the
            table_map. These raw tables are stored as either a string or a list of
            strings, so we flatten them. The we add the respondent table because it is
            always needed.
            """
            tables_needed = set(
                flatten_list(
                    [tbl_dict[source_ferc1] for tbl_dict in table_map.values()]
                )
            )
            # add the respondent table bc its always needed
            tables_needed.add(
                "f1_respondent_id" if source_ferc1 == "dbf" else "identification_001"
            )
            return tables_needed

        def missing_table_error_message(
            missing_tables: list[str],
            source_ferc1: Literal["dbf", "xbrl"],
        ) -> str:
            """Return error message for missing tables."""
            return (
                f"There are tables missing from ferc1_{source_ferc1}_to_sqlite_settings"
                " that are needed to load FERC-derivative PUDL tables in these settings."
                f"\nMissing Tables: {missing_tables}. \nNOTE: If you are not trying to "
                "run ferc_to_sqlite, but your settings contain FERC-derivative PUDL "
                "tables, you can EITHER provide NO ferc_to_sqlite_settings or provide "
                "tables consistent with your PUDL table settings."
            )

        # DBF table check
        if ferc1_dbf_to_sqlite_settings:
            dbf_tables_needed = _get_tables_from_table_map_by_source(table_map, "dbf")
            if dbf_missing := dbf_tables_needed.difference(
                set(ferc1_dbf_to_sqlite_settings.tables)
            ):
                raise AssertionError(missing_table_error_message(dbf_missing, "dbf"))

        # XBRL table check
        # the XBRL extractor can take a list of none and extract everything so only
        # do this check if there are any tables given.
        if ferc1_xbrl_to_sqlite_settings and ferc1_xbrl_to_sqlite_settings.tables:
            xbrl_tables_needed = _get_tables_from_table_map_by_source(table_map, "xbrl")
            xbrl_tables_needed = {tbl + "_instant" for tbl in xbrl_tables_needed} | {
                tbl + "_duration" for tbl in xbrl_tables_needed
            }
            if xbrl_missing := xbrl_tables_needed.difference(
                set(ferc1_xbrl_to_sqlite_settings.tables)
            ):
                raise AssertionError(missing_table_error_message(xbrl_missing, "xbrl"))
        return field_values

    @classmethod
    def from_yaml(cls, path: str) -> "EtlSettings":
        """Create an EtlSettings instance from a yaml_file path.

        Args:
            path: path to a yaml file.

        Returns:
            An ETL settings object.
        """
        with pathlib.Path(path).open() as f:
            yaml_file = yaml.safe_load(f)
        return cls.parse_obj(yaml_file)


def _make_doi_clickable(link):
    """Make a clickable DOI."""
    return f"https://doi.org/{link}"
