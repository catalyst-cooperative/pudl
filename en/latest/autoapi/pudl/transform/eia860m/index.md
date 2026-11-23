# pudl.transform.eia860m

Module to perform data cleaning functions on EIA860m data tables.

## Attributes

| [`logger`](#pudl.transform.eia860m.logger)   |    |
|-----------------------------------------------------------|----|

## Functions

| [`_append_puerto_rico_generators`](#pudl.transform.eia860m._append_puerto_rico_generators)(generators, ...) → pandas.DataFrame   | Append a raw Puerto Rico EIA-860M generator page to its mainland counterpart.   |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| [`core_eia860m__changelog_generators`](#pudl.transform.eia860m.core_eia860m__changelog_generators)(...) → pandas.DataFrame           | Changelog of EIA-860M Generators based on operating status.                     |

## Module Contents

### pudl.transform.eia860m.logger

### pudl.transform.eia860m.\_append_puerto_rico_generators(generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), puerto_rico_generators: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Append a raw Puerto Rico EIA-860M generator page to its mainland counterpart.

EIA-860M reports Puerto Rico generators on their own spreadsheet tabs, which are
extracted as separate raw assets. They are combined with the mainland pages in the
same way as in [`pudl.extract.eia860m.append_eia860m()`](../../extract/eia860m/index.md#pudl.extract.eia860m.append_eia860m).

### pudl.transform.eia860m.core_eia860m_\_changelog_generators(raw_eia860m_\_generator_proposed: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860m_\_generator_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860m_\_generator_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860m_\_puerto_rico_generator_proposed: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860m_\_puerto_rico_generator_existing: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame), raw_eia860m_\_puerto_rico_generator_retired: [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)) → [pandas.DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)

Changelog of EIA-860M Generators based on operating status.

The monthly reported EIA-860M tables includes existing, proposed and retired
generators, with Puerto Rico generators reported on separate tabs. This table
combines all monthly reported data, including Puerto Rico, and preserves the first
reported record when any new information about the generator was reported.

We are not putting this table through PUDL’s standard normalization process for EIA
tables (see [`pudl.transform.eia.harvest_entity_tables()`](../eia/index.md#pudl.transform.eia.harvest_entity_tables)). EIA-860M includes
provisional data reported monthly so it changes frequently compared to the more
stable annually reported EIA data. If we fed all of the EIA-860M data into the
harvesting process, we would get failures because the records from EIA-80m are too
inconsistent for our thresholds for harvesting canonical values for entities. A
ramification of this table not being harvested is that if there are any entities
(generators, plants, utilities) that were only ever reported in an older EIA-860M
file, there will be no record of it in the PUDL entity or SCD tables. Therefore,
this asset cannot have foreign key relationships with the rest of the core EIA
tables.
