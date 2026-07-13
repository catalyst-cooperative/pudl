# pudl.settings

Module for validating pudl ETL data configurations.

## Attributes

| [`logger`](#pudl.settings.logger)   |    |
|-------------------------------------|----|

## Classes

| [`FercForm`](#pudl.settings.FercForm)                                                   | Contains full list of supported FERC forms.                                       |
|-----------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| [`FrozenBaseModel`](#pudl.settings.FrozenBaseModel)                                     | BaseModel with global configuration.                                              |
| [`GenericDataConfig`](#pudl.settings.GenericDataConfig)                                 | An abstract pydantic model for generic datasets.                                  |
| [`Ferc1DataConfig`](#pudl.settings.Ferc1DataConfig)                                     | An immutable pydantic model to validate Ferc1DataConfig.                          |
| [`Ferc714DataConfig`](#pudl.settings.Ferc714DataConfig)                                 | An immutable pydantic model to validate Ferc714DataConfig.                        |
| [`EpaCemsDataConfig`](#pudl.settings.EpaCemsDataConfig)                                 | An immutable pydantic model to validate EPA CEMS data configuration.              |
| [`PhmsaGasDataConfig`](#pudl.settings.PhmsaGasDataConfig)                               | An immutable pydantic model to validate PHMSA data configuration.                 |
| [`Sec10kDataConfig`](#pudl.settings.Sec10kDataConfig)                                   | An immutable pydantic model to validate SEC 10-K data configuration.              |
| [`NrelAtbDataConfig`](#pudl.settings.NrelAtbDataConfig)                                 | An immutable pydantic model to validate NREL ATB data configuration.              |
| [`Eia923DataConfig`](#pudl.settings.Eia923DataConfig)                                   | An immutable pydantic model to validate EIA 923 data configuration.               |
| [`Eia930DataConfig`](#pudl.settings.Eia930DataConfig)                                   | An immutable pydantic model to validate EIA 930 data configuration.               |
| [`Eia861DataConfig`](#pudl.settings.Eia861DataConfig)                                   | An immutable pydantic model to validate EIA 861 data configuration.               |
| [`Eia860DataConfig`](#pudl.settings.Eia860DataConfig)                                   | An immutable pydantic model to validate EIA 860 data configuration.               |
| [`Eia860mDataConfig`](#pudl.settings.Eia860mDataConfig)                                 | An immutable pydantic model to validate EIA 860m data configuration.              |
| [`Eia757aDataConfig`](#pudl.settings.Eia757aDataConfig)                                 | An immutable pydantic model to validate EIA 757a data configuration.              |
| [`Eia191DataConfig`](#pudl.settings.Eia191DataConfig)                                   | An immutable pydantic model to validate EIA 191 data configuration.               |
| [`Eia176DataConfig`](#pudl.settings.Eia176DataConfig)                                   | An immutable pydantic model to validate EIA 176 data configuration.               |
| [`EiaAeoDataConfig`](#pudl.settings.EiaAeoDataConfig)                                   | An immutable pydantic model to validate EIA AEO data configuration.               |
| [`VceRareDataConfig`](#pudl.settings.VceRareDataConfig)                                 | An immutable pydantic model to validate VCE RARE data configuration.              |
| [`CensusPepDataConfig`](#pudl.settings.CensusPepDataConfig)                             | An immutable pydantic model to validate Census PEP data configuration.            |
| [`GlueDataConfig`](#pudl.settings.GlueDataConfig)                                       | An immutable pydantic model to validate Glue data configuration.                  |
| [`GridPathRaToolkitTechType`](#pudl.settings.GridPathRaToolkitTechType)                 | Enum to constrain GridPath RA Toolkit technology types.                           |
| [`GridPathRaToolkitProcLevel`](#pudl.settings.GridPathRaToolkitProcLevel)               | Enum to constraint GridPath RA Toolkit processing levels.                         |
| [`GridPathRaToolkitDataConfig`](#pudl.settings.GridPathRaToolkitDataConfig)             | An immutable pydantic model to validate GridPath RA Toolkit data configuration.   |
| [`EiaDataConfig`](#pudl.settings.EiaDataConfig)                                         | An immutable pydantic model to validate EIA datasets data configuration.          |
| [`Rus7DataConfig`](#pudl.settings.Rus7DataConfig)                                       | An immutable pydantic model to validate RUS-7 datasets data configuration.        |
| [`Rus12DataConfig`](#pudl.settings.Rus12DataConfig)                                     | An immutable pydantic model to validate RUS Form 12 data configuration.           |
| [`PudlDataConfig`](#pudl.settings.PudlDataConfig)                                       | An immutable pydantic model to validate PUDL Dataset data configuration.          |
| [`FercDbfToSqliteDataConfig`](#pudl.settings.FercDbfToSqliteDataConfig)                 | Base class for all FERC DBF-to-SQLite data config models.                         |
| [`Ferc1DbfToSqliteDataConfig`](#pudl.settings.Ferc1DbfToSqliteDataConfig)               | An immutable Pydantic model to validate FERC 1 to SQLite data config.             |
| [`FercGenericXbrlToSqliteDataConfig`](#pudl.settings.FercGenericXbrlToSqliteDataConfig) | An immutable pydantic model to validate Ferc1 to SQLite data config.              |
| [`Ferc1XbrlToSqliteDataConfig`](#pudl.settings.Ferc1XbrlToSqliteDataConfig)             | An immutable pydantic model to validate Ferc1 to SQLite data config.              |
| [`Ferc2XbrlToSqliteDataConfig`](#pudl.settings.Ferc2XbrlToSqliteDataConfig)             | An immutable pydantic model to validate FERC from 2 XBRL to SQLite data config.   |
| [`Ferc2DbfToSqliteDataConfig`](#pudl.settings.Ferc2DbfToSqliteDataConfig)               | An immutable Pydantic model to validate FERC 2 to SQLite data config.             |
| [`Ferc6DbfToSqliteDataConfig`](#pudl.settings.Ferc6DbfToSqliteDataConfig)               | An immutable Pydantic model to validate FERC 6 to SQLite data config.             |
| [`Ferc6XbrlToSqliteDataConfig`](#pudl.settings.Ferc6XbrlToSqliteDataConfig)             | An immutable pydantic model to validate FERC from 6 XBRL to SQLite data config.   |
| [`Ferc60DbfToSqliteDataConfig`](#pudl.settings.Ferc60DbfToSqliteDataConfig)             | An immutable Pydantic model to validate FERC 60 to SQLite data config.            |
| [`Ferc60XbrlToSqliteDataConfig`](#pudl.settings.Ferc60XbrlToSqliteDataConfig)           | An immutable pydantic model to validate FERC from 60 XBRL to SQLite data config.  |
| [`Ferc714XbrlToSqliteDataConfig`](#pudl.settings.Ferc714XbrlToSqliteDataConfig)         | An immutable pydantic model to validate FERC from 714 XBRL to SQLite data config. |
| [`FercToSqliteDataConfig`](#pudl.settings.FercToSqliteDataConfig)                       | An immutable pydantic model to validate FERC XBRL to SQLite data config.          |
| [`GlobalDataConfig`](#pudl.settings.GlobalDataConfig)                                   | Main settings validation class.                                                   |

## Functions

| [`_zenodo_doi_to_url`](#pudl.settings._zenodo_doi_to_url)(→ pydantic.AnyHttpUrl)   | Create a DOI URL out o a Zenodo DOI.   |
|------------------------------------------------------------------------------------|----------------------------------------|

## Module Contents

### pudl.settings.logger

### *class* pudl.settings.FercForm(\*args, \*\*kwds)

Bases: [`enum.Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Contains full list of supported FERC forms.

#### FORM1 *= 1*

#### FORM2 *= 2*

#### FORM6 *= 6*

#### FORM60 *= 60*

#### FORM714 *= 714*

#### \_\_str_\_()

Format this as `fercX`, the way we use it everywhere else.

### *class* pudl.settings.FrozenBaseModel(/, \*\*data: Any)

Bases: [`pydantic.BaseModel`](https://pydantic.dev/docs/validation/latest/api/pydantic/base_model/#pydantic.BaseModel)

BaseModel with global configuration.

#### model_config *: [pydantic.ConfigDict](https://pydantic.dev/docs/validation/latest/api/pydantic/config/#pydantic.config.ConfigDict)*

Configuration for the model, should be a dictionary conforming to [ConfigDict][pydantic.config.ConfigDict].

### *class* pudl.settings.GenericDataConfig(/, \*\*data: Any)

Bases: [`FrozenBaseModel`](#pudl.settings.FrozenBaseModel)

An abstract pydantic model for generic datasets.

Each dataset must specify working partitions. A dataset can have an arbitrary number
of partitions.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### validate_partitions()

Ensure that partitions and their values are valid.

Checks that:

* all partitions specified by the data source exist,
* partitions are not None
* only known to be working partition values are specified
* no duplicate partition values are specified

#### *property* partitions *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[None](https://docs.python.org/3/library/constants.html#None) | [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str)]]*

Return list of dictionaries representing individual partitions.

Convert a list of partitions into a list of dictionaries of partitions. This is
intended to be used to store partitions in a format that is easy to use with
`pandas.json_normalize()`.

### *class* pudl.settings.Ferc1DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate Ferc1DataConfig.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### *property* dbf_years

Return validated years for which DBF data is available.

#### *property* xbrl_years

Return validated years for which XBRL data is available.

### *class* pudl.settings.Ferc714DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate Ferc714DataConfig.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### *property* csv_years

Return validated years for which CSV data is available.

#### *property* xbrl_years

Return validated years for which XBRL data is available.

### *class* pudl.settings.EpaCemsDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EPA CEMS data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### year_quarters *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The list of years-quarters to validate.

#### *classmethod* allow_all_keyword_year_quarters(year_quarters)

Allow users to specify [‘all’] to get all quarters.

### *class* pudl.settings.PhmsaGasDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate PHMSA data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### *property* extraction_years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to extract.

These are different from the standard [`years`](#pudl.settings.PhmsaGasDataConfig.years) because
the oldest years (1970 - 1989) are published with multiple years
in each tab. Instead of running the extraction step on each year
and filtering on the year from each tab, we extract the whole tab
all at once using the first year in the tab as the partition.

### *class* pudl.settings.Sec10kDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate SEC 10-K data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of valid years for which SEC 10-K data is available.

#### tables *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

### *class* pudl.settings.NrelAtbDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate NREL ATB data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Eia923DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 923 data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Eia930DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 930 data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### half_years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The list of half years to validate.

#### *classmethod* allow_all_keyword_half_years(half_years)

Allow users to specify [‘all’] to get all half-years.

### *class* pudl.settings.Eia861DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 861 data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Eia860DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 860 data configuration.

This model also checks 860m data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### eia860m *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

Whether or not to incorporate an EIA-860m month.

#### all_eia860m_year_months *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The list of all EIA-860m year-months.

#### eia860m_year_months *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= None*

The 860m year-months to incorporate.

#### *classmethod* add_other_860m_years(v, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Find extra years from EIA860m if applicable.

There’s a gap in reporting (after the new year but before the EIA 860 early
release data in June) when we rely on two years worth of EIA860m data instead
of just one. This function adds the last available month_year values for each
year of 860m data that is not yet available in 860.

#### *classmethod* no_repeat_years(v, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Make sure there are no duplicate 860m year values.

#### *classmethod* validate_eia860m_params(v, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Check that the year_month values for eia860m_year_months are valid.

#### *classmethod* only_years_not_in_eia860(v, info: [pydantic.ValidationInfo](https://pydantic.dev/docs/validation/latest/api/pydantic-core/pydantic_core_schema/#pydantic_core.core_schema.ValidationInfo)) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Ensure no EIA860m values are from years already in EIA860.

### *class* pudl.settings.Eia860mDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 860m data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### year_months *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

The 860m year to date.

#### *classmethod* allow_all_keyword_year_months(year_months)

Allow users to specify [‘all’] to get all quarters.

### *class* pudl.settings.Eia757aDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 757a data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Eia191DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 191 data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Eia176DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA 176 data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.EiaAeoDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate EIA AEO data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.VceRareDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate VCE RARE data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### fips *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

Include FIPS codes in VCE RARE Power Dataset.

### *class* pudl.settings.CensusPepDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate Census PEP data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.GlueDataConfig(/, \*\*data: Any)

Bases: [`FrozenBaseModel`](#pudl.settings.FrozenBaseModel)

An immutable pydantic model to validate Glue data configuration.

#### eia *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

Include eia in glue data configuration.

#### ferc1 *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

Include ferc1 in glue data configuration.

### *class* pudl.settings.GridPathRaToolkitTechType

Bases: [`enum.StrEnum`](https://docs.python.org/3/library/enum.html#enum.StrEnum)

Enum to constrain GridPath RA Toolkit technology types.

#### WIND

#### SOLAR

### *class* pudl.settings.GridPathRaToolkitProcLevel

Bases: [`enum.StrEnum`](https://docs.python.org/3/library/enum.html#enum.StrEnum)

Enum to constraint GridPath RA Toolkit processing levels.

#### EXTENDED

### *class* pudl.settings.GridPathRaToolkitDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate GridPath RA Toolkit data configuration.

Note that the default values for technology_types, processing_levels, and
daily_weather are such that by default, all working partitions will be included.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### technology_types *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['wind', 'solar']*

#### processing_levels *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]* *= ['extended']*

#### daily_weather *: [bool](https://docs.python.org/3/library/functions.html#bool)* *= True*

#### *classmethod* deduplicate_list(v)

Deduplicate technology type and processing level values.

#### *classmethod* allowed_technology_types(v: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Ensure that technology types are valid.

#### *classmethod* allowed_processing_levels(v: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]

Ensure that processing levels are valid.

#### *property* parts *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]*

Construct parts from selected technologies, processing levels, and daily weather.

### *class* pudl.settings.EiaDataConfig(/, \*\*data: Any)

Bases: [`FrozenBaseModel`](#pudl.settings.FrozenBaseModel)

An immutable pydantic model to validate EIA datasets data configuration.

#### eia176 *: [Eia176DataConfig](#pudl.settings.Eia176DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia191 *: [Eia191DataConfig](#pudl.settings.Eia191DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia757a *: [Eia757aDataConfig](#pudl.settings.Eia757aDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia860 *: [Eia860DataConfig](#pudl.settings.Eia860DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia860m *: [Eia860mDataConfig](#pudl.settings.Eia860mDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia861 *: [Eia861DataConfig](#pudl.settings.Eia861DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia923 *: [Eia923DataConfig](#pudl.settings.Eia923DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eia930 *: [Eia930DataConfig](#pudl.settings.Eia930DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### eiaaeo *: [EiaAeoDataConfig](#pudl.settings.EiaAeoDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* default_load_all(data: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

If no datasets are specified default to all.

#### *classmethod* check_eia_dependencies(data: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Make sure the dependencies between the eia datasets are satisfied.

Dependencies:
\* eia923 requires eia860 for harvesting purposes.

* **Parameters:**
  **values** (*Dict* *[*[*str*](https://docs.python.org/3/library/stdtypes.html#str) *,* *BaseModel* *]*) – dataset data configuration.
* **Returns:**
  dataset data configuration.
* **Return type:**
  values (Dict[[str](https://docs.python.org/3/library/stdtypes.html#str), BaseModel])

### *class* pudl.settings.Rus7DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate RUS-7 datasets data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Rus12DataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

An immutable pydantic model to validate RUS Form 12 data configuration.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.PudlDataConfig(/, \*\*data: Any)

Bases: [`FrozenBaseModel`](#pudl.settings.FrozenBaseModel)

An immutable pydantic model to validate PUDL Dataset data configuration.

#### eia *: [EiaDataConfig](#pudl.settings.EiaDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### epacems *: [EpaCemsDataConfig](#pudl.settings.EpaCemsDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc1 *: [Ferc1DataConfig](#pudl.settings.Ferc1DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc714 *: [Ferc714DataConfig](#pudl.settings.Ferc714DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### glue *: [GlueDataConfig](#pudl.settings.GlueDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### gridpathratoolkit *: [GridPathRaToolkitDataConfig](#pudl.settings.GridPathRaToolkitDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### nrelatb *: [NrelAtbDataConfig](#pudl.settings.NrelAtbDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### phmsagas *: [PhmsaGasDataConfig](#pudl.settings.PhmsaGasDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### sec10k *: [Sec10kDataConfig](#pudl.settings.Sec10kDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### vcerare *: [VceRareDataConfig](#pudl.settings.VceRareDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### censuspep *: [CensusPepDataConfig](#pudl.settings.CensusPepDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### rus7 *: [Rus7DataConfig](#pudl.settings.Rus7DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### rus12 *: [Rus12DataConfig](#pudl.settings.Rus12DataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* default_load_all(data: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

If no datasets are specified default to all.

* **Parameters:**
  **data** – PUDL data configuration.
* **Returns:**
  Validated PUDL data configuration.

#### *classmethod* add_glue_data_config(data: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Add glue data configuration if ferc1 and eia data are both requested.

* **Parameters:**
  **values** (*Dict* *[*[*str*](https://docs.python.org/3/library/stdtypes.html#str) *,* *BaseModel* *]*) – PUDL data configuration.
* **Returns:**
  PUDL data configuration.
* **Return type:**
  values (Dict[[str](https://docs.python.org/3/library/stdtypes.html#str), BaseModel])

#### get_datasets()

Gets dictionary of PUDL data configuration.

#### make_datasources_table(data_store: [pudl.workspace.datastore.Datastore](../workspace/datastore/index.md#pudl.workspace.datastore.Datastore)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Compile a table of dataset information.

There are three places we can look for information about a dataset:
\* the datastore (for DOIs, working partitions, etc)
\* the PUDL data configuration (for partitions that are used in the ETL)
\* the DataSource info (which is stored within the PUDL data configuration)

The PUDL data configuration and the datastore have different levels of nesting -
and therefore names for datasets. The nesting happens particularly with the EIA
data.

* **Parameters:**
  **data_store** – An initialized PUDL Datastore from which the DOI’s for each raw input
  dataset can be obtained.
* **Returns:**
  a dataframe describing the partitions and DOI’s of each of the datasets in
  this PUDL data config object.

### *class* pudl.settings.FercDbfToSqliteDataConfig(/, \*\*data: Any)

Bases: [`GenericDataConfig`](#pudl.settings.GenericDataConfig)

Base class for all FERC DBF-to-SQLite data config models.

Declares the `years` and `refyear` attributes shared by every FERC DBF
form so that [`FercDbfExtractor`](../extract/dbf/index.md#pudl.extract.dbf.FercDbfExtractor) can be typed
against this base rather than the looser [`GenericDataConfig`](#pudl.settings.GenericDataConfig).

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]* *= []*

Years of DBF data to extract.

#### refyear *: ClassVar[[int](https://docs.python.org/3/library/functions.html#int)]*

Reference year used to build the destination schema; provided by each subclass.

### *class* pudl.settings.Ferc1DbfToSqliteDataConfig(/, \*\*data: Any)

Bases: [`FercDbfToSqliteDataConfig`](#pudl.settings.FercDbfToSqliteDataConfig)

An immutable Pydantic model to validate FERC 1 to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### refyear *: ClassVar[[int](https://docs.python.org/3/library/functions.html#int)]*

The reference year for the dataset.

### *class* pudl.settings.FercGenericXbrlToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`pydantic_settings.BaseSettings`](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.BaseSettings)

An immutable pydantic model to validate Ferc1 to SQLite data config.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Ferc1XbrlToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`FercGenericXbrlToSqliteDataConfig`](#pudl.settings.FercGenericXbrlToSqliteDataConfig)

An immutable pydantic model to validate Ferc1 to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Ferc2XbrlToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`FercGenericXbrlToSqliteDataConfig`](#pudl.settings.FercGenericXbrlToSqliteDataConfig)

An immutable pydantic model to validate FERC from 2 XBRL to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Ferc2DbfToSqliteDataConfig(/, \*\*data: Any)

Bases: [`FercDbfToSqliteDataConfig`](#pudl.settings.FercDbfToSqliteDataConfig)

An immutable Pydantic model to validate FERC 2 to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### refyear *: ClassVar[[int](https://docs.python.org/3/library/functions.html#int)]*

The reference year for the dataset.

### *class* pudl.settings.Ferc6DbfToSqliteDataConfig(/, \*\*data: Any)

Bases: [`FercDbfToSqliteDataConfig`](#pudl.settings.FercDbfToSqliteDataConfig)

An immutable Pydantic model to validate FERC 6 to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### refyear *: ClassVar[[int](https://docs.python.org/3/library/functions.html#int)]*

The reference year for the dataset.

### *class* pudl.settings.Ferc6XbrlToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`FercGenericXbrlToSqliteDataConfig`](#pudl.settings.FercGenericXbrlToSqliteDataConfig)

An immutable pydantic model to validate FERC from 6 XBRL to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Ferc60DbfToSqliteDataConfig(/, \*\*data: Any)

Bases: [`FercDbfToSqliteDataConfig`](#pudl.settings.FercDbfToSqliteDataConfig)

An immutable Pydantic model to validate FERC 60 to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

The DataSource metadata object for this dataset.

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

#### refyear *: ClassVar[[int](https://docs.python.org/3/library/functions.html#int)]*

The reference year for the dataset.

### *class* pudl.settings.Ferc60XbrlToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`FercGenericXbrlToSqliteDataConfig`](#pudl.settings.FercGenericXbrlToSqliteDataConfig)

An immutable pydantic model to validate FERC from 60 XBRL to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.Ferc714XbrlToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`FercGenericXbrlToSqliteDataConfig`](#pudl.settings.FercGenericXbrlToSqliteDataConfig)

An immutable pydantic model to validate FERC from 714 XBRL to SQLite data config.

#### data_source *: ClassVar[[pudl.metadata.classes.DataSource](../metadata/classes/index.md#pudl.metadata.classes.DataSource)]*

#### years *: [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]*

The list of years to validate.

### *class* pudl.settings.FercToSqliteDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`pydantic_settings.BaseSettings`](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.BaseSettings)

An immutable pydantic model to validate FERC XBRL to SQLite data config.

#### ferc1_dbf *: [Ferc1DbfToSqliteDataConfig](#pudl.settings.Ferc1DbfToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc1_xbrl *: [Ferc1XbrlToSqliteDataConfig](#pudl.settings.Ferc1XbrlToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc2_dbf *: [Ferc2DbfToSqliteDataConfig](#pudl.settings.Ferc2DbfToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc2_xbrl *: [Ferc2XbrlToSqliteDataConfig](#pudl.settings.Ferc2XbrlToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc6_dbf *: [Ferc6DbfToSqliteDataConfig](#pudl.settings.Ferc6DbfToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc6_xbrl *: [Ferc6XbrlToSqliteDataConfig](#pudl.settings.Ferc6XbrlToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc60_dbf *: [Ferc60DbfToSqliteDataConfig](#pudl.settings.Ferc60DbfToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc60_xbrl *: [Ferc60XbrlToSqliteDataConfig](#pudl.settings.Ferc60XbrlToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### ferc714_xbrl *: [Ferc714XbrlToSqliteDataConfig](#pudl.settings.Ferc714XbrlToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* default_load_all(data: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

If no datasets are specified default to all.

#### get_data_config(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str) | [FercForm](#pudl.settings.FercForm), data_format: Literal['dbf', 'xbrl']) → [FercGenericXbrlToSqliteDataConfig](#pudl.settings.FercGenericXbrlToSqliteDataConfig) | [FercDbfToSqliteDataConfig](#pudl.settings.FercDbfToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)

Look up extraction settings by dataset (`fercX`) and data format (`dbf` or `xbrl`).

Returns `None` if the dataset/format combination is not configured.

#### get_dataset_years(dataset: [str](https://docs.python.org/3/library/stdtypes.html#str) | [FercForm](#pudl.settings.FercForm), data_format: Literal['dbf', 'xbrl']) → [list](https://docs.python.org/3/library/stdtypes.html#list)[[int](https://docs.python.org/3/library/functions.html#int)]

Look up extraction *years* by dataset (`fercX`) and data format (`dbf` or `xbrl`).

* **Raises:**
  [**ValueError**](https://docs.python.org/3/library/exceptions.html#ValueError) – if the dataset/format combination is not configured.

### *class* pudl.settings.GlobalDataConfig(\_case_sensitive: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_nested_model_default_partial_update: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_prefix_target: pydantic_settings.sources.EnvPrefixTarget | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_file: pydantic_settings.sources.DotenvType | [None](https://docs.python.org/3/library/constants.html#None) = ENV_FILE_SENTINEL, \_env_file_encoding: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_ignore_empty: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_delimiter: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_nested_max_split: [int](https://docs.python.org/3/library/functions.html#int) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_env_parse_enums: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prog_name: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)] | [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[str](https://docs.python.org/3/library/stdtypes.html#str), Ellipsis] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_settings_source: [pydantic_settings.sources.CliSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.CliSettingsSource)[Any] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_parse_none_str: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_hide_none_type: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_avoid_json: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_enforce_required: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_use_class_docs_for_groups: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_exit_on_error: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_prefix: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_flag_prefix_char: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_implicit_flags: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['dual', 'toggle'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_ignore_unknown_args: [bool](https://docs.python.org/3/library/functions.html#bool) | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_kebab_case: [bool](https://docs.python.org/3/library/functions.html#bool) | Literal['all', 'no_enums'] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_cli_shortcuts: [collections.abc.Mapping](https://docs.python.org/3/library/collections.abc.html#collections.abc.Mapping)[[str](https://docs.python.org/3/library/stdtypes.html#str), [str](https://docs.python.org/3/library/stdtypes.html#str) | [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \_secrets_dir: pydantic_settings.sources.PathType | [None](https://docs.python.org/3/library/constants.html#None) = None, \_build_sources: [tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[tuple](https://docs.python.org/3/library/stdtypes.html#tuple)[[pydantic_settings.sources.PydanticBaseSettingsSource](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.PydanticBaseSettingsSource), Ellipsis], [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]] | [None](https://docs.python.org/3/library/constants.html#None) = None, \*\*values: Any)

Bases: [`pydantic_settings.BaseSettings`](https://pydantic.dev/docs/validation/latest/api/pydantic_settings/#pydantic_settings.BaseSettings)

Main settings validation class.

#### ferc_to_sqlite *: [FercToSqliteDataConfig](#pudl.settings.FercToSqliteDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### pudl *: [PudlDataConfig](#pudl.settings.PudlDataConfig) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### name *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### title *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### description *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### version *: [str](https://docs.python.org/3/library/stdtypes.html#str) | [None](https://docs.python.org/3/library/constants.html#None)* *= None*

#### *classmethod* from_yaml(path: [str](https://docs.python.org/3/library/stdtypes.html#str) | [pathlib.Path](https://docs.python.org/3/library/pathlib.html#pathlib.Path)) → [GlobalDataConfig](#pudl.settings.GlobalDataConfig)

Create validated GlobalDataConfig from a local YAML file path.

* **Parameters:**
  **path** – Path to a YAML file. Relative paths are resolved against the
  current working directory and `~` is expanded.
* **Returns:**
  A GlobalDataConfig object.

#### validate_xbrl_years()

Ensure the XBRL years in PudlDataConfig align with FercToSqliteDataConfig.

For each of the FERC forms that we are processing in PUDL, check to ensure
that the years we are trying to process in the PUDL ETL are included in the
XBRL to SQLite data config.

#### *property* pudl_data_config *: [PudlDataConfig](#pudl.settings.PudlDataConfig)*

Return validated PUDL data config or raise if it is unavailable.

### pudl.settings.\_zenodo_doi_to_url(doi: [pudl.workspace.datastore.ZenodoDoi](../workspace/datastore/index.md#pudl.workspace.datastore.ZenodoDoi)) → [pydantic.AnyHttpUrl](https://pydantic.dev/docs/validation/latest/api/pydantic/networks/#pydantic.networks.AnyHttpUrl)

Create a DOI URL out o a Zenodo DOI.
