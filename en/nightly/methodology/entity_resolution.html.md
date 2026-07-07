# Entity Resolution

## Overview

Many of the datasets PUDL processes report the same information about individual
entities in many different places. This is usually done for readability. For example,
it’s nice to list the names of plants or utilities instead of just their IDs. This makes
the original data easier for users to read but it also introduces the potential for
internal inconsistencies ([entity](https://en.wikipedia.org/wiki/Entity_integrity) or
[referential integrity](https://en.wikipedia.org/wiki/Referential_integrity) issues)
which create problems when you’re trying to re-use the data in other applications.

PUDL attempts to identify canonical values (”[golden records](https://en.wikipedia.org/wiki/Master_data_management)”) for entities from
the potentially inconsistent original data through an [entity resolution](https://en.wikipedia.org/wiki/Record_linkage#Entity_resolution) process (often
called “entity harvesting” in the PUDL codebase).

For example, the same plant or generator may appear in many forms, worksheets, and
years. Across those sources, attributes like plant name, associated balancing authority,
geographic coordinates, or operating dates may be:

* omitted in some tables,
* reported with slightly different spellings or codes,
* updated in one place but not another, or
* associated with one year of reporting but not another.

If PUDL exposed every upstream table independently without reconciling those
inconsistencies, many common analyses would require users to manually decide which value
to trust for each entity and year. Instead, PUDL builds normalized entity tables that
aim to provide a canonical record for each entity that includes the attributes that are
expected to be stable over time and where appropriate, a yearly record of attributes
that are expected to change over time.

The entity resolution process is applied most extensively to the
[EIA-860](../data_sources/eia860.md) and [EIA-923](../data_sources/eia923.md)
spreadsheet data, but it’s a more general issue that comes up throughout the data we
process. This page explains the entity resolution process conceptually, why it exists,
and what it means when the PUDL entity tables do not exactly mirror an individual raw
EIA spreadsheet.

### What Entity Resolution Produces

For each entity type, PUDL typically creates two related tables:

* A static entity table such as [core_eia_\_entity_plants](../data_dictionaries/pudl_db.md#core-eia-entity-plants), with one row per entity
  and attributes that are expected to be stable over time. These tables all have
  `entity` in their names.
* A yearly slowly changing dimension (SCD) table such as [core_eia860_\_scd_plants](../data_dictionaries/pudl_db.md#core-eia860-scd-plants),
  with one row per entity per report year and attributes that are expected to vary
  slightly over time. These tables all have `scd` in their names.

PUDL currently resolves four kinds of EIA entities, with these associated entity ID
columns:

* Utilities `(utility_id_eia)`
* Plants `(plant_id_eia)`
* Boilers `(plant_id_eia, boiler_id)`
* Generators `(plant_id_eia, generator_id)`

For forensic purposes, PUDL also publishes tables which includes all values which are
found in the original tables which we use to create these canonical values. These
tables are all have `forensics_entity_resolution` in their names.

### Why A Canonical Record Is Necessary

Users are sometimes surprised to see slight differences between the raw EIA spreadsheets
and analogous PUDL data, but in most of the cases we’ve tracked down over the years,
these differences are expected and intentional.

For example, an EIA plant may be named slightly differently across the plant table,
generator table, ownership table, and emissions-control tables. One table might use an
older balancing authority code. Another might omit latitude and longitude. A generator
operating date might be reported consistently in most years, but differ in one source.

When this happens, PUDL does not treat every reported value as equally authoritative.
Instead, it looks across all relevant upstream tables and determines which value was
reported most consistently for this entity.

This is usually desirable. A normalized analytical database is more useful when it
provides stable, reconciled identifiers and attributes rather than reproducing every
reporting inconsistency exactly as filed.

## How Entity Resolution Works in PUDL

### Where Entity Resolution Happens in the PUDL Data Pipeline

Entity resolution sits in the middle of the EIA transformation pipeline.

First, dataset-specific transforms create a set of unnormalized `_core` tables. These
tables are already cleaned and typed, but they still reflect the original structure of
the source data. Then we scan those tables for entity IDs and their known attributes,
to produce normalized entity tables.

The simplified flow for plants looks like this:

Conceptually, the same pattern is used for all four entity types. The full list of
input tables for all EIA entities can be found in
[`pudl.transform.eia.HARVESTABLE_ASSETS`](../autoapi/pudl/transform/eia/index.md#pudl.transform.eia.HARVESTABLE_ASSETS)

### How PUDL Identifies Existing Entities

The first step is to determine which entities and entity-years exist anywhere in the
upstream transformed EIA data. This is done in
[`pudl.transform.eia._compile_all_entity_records()`](../autoapi/pudl/transform/eia/index.md#pudl.transform.eia._compile_all_entity_records).

To produce the annually varying SCD tables `report_date` is normalized to January 1 of
the report year so that all annual records align even if an upstream table has a
different temporal resolution.

This results in an interim dataframe containing every observed instance of the entity
across all the upstream tables.

The next step is to derive the ID spaces for the entity and annual SCD tables.

* The static entity table gets one row per unique entity ID.
* The yearly SCD table gets one row per unique combination of `entity_id` and
  `report_date` (where `entity_id` is sometimes actually composed of multiple
  columns).

This means an entity-year can enter the yearly resolved table from *any* upstream asset
that reports the entity ID column and `report_date`.

### How PUDL Chooses A Canonical Value

When there are multiple values reported for the same entity, in most cases, PUDL chooses
the most consistent value reported so long as it is found in at least 70% of available
entries. If no observed value occurs more than 70% of the time, PUDL fills in a null
value.

The selection logic looks like this conceptually:

### Special Cases

Some attributes are too messy to reconcile with the default consistency logic alone.
We use different rules for columns with additional requirements:

* Latitude and longitude are floating point values and are frequently not exactly equal,
  so 70% consistency is not attainable very often. In cases without a consistent value
  we round latitude and longitude to the nearest tenth of a degree and repeat the
  process.
* Generator operating date has an unusual pattern of missingness that permits the most
  recently reported operating date to be reliable when 70% consistency cannot otherwise
  be reached.
* We set the consistency threshold to 0% for a few columns so that we always get a
  value. These include plant name, utility name, and prime mover code.

## Interpreting Discrepancies

If you compare a PUDL entity table, SCD table, or any table downstream of them to a raw
EIA spreadsheet, you should expect to see several kinds of discrepancies:

* **A name or code may differ** because PUDL selected the value that was most
  consistently reported across many tables and years.
* **A value may be null in PUDL** because upstream reporting was too inconsistent to
  unambiguously choose a canonical value.
* **An entity-year may exist in PUDL but not in the spreadsheet you are viewing**
  because the plant-year was reported in a different upstream table.

This is especially important when interpreting association tables. A reported
relationship seen in one raw data source may be incomplete, outdated, or inconsistent
with other sources. PUDL’s goal is not to preserve every raw inconsistency in the entity
tables, but to produce a coherent cross-table representation that works well for
analysis.

## Improving PUDL Entity Resolution

The entity resolution process is heuristic and can definitely be improved.

* If there are particular columns that you think would benefit from a domain specific
  consistency metric, please [open a data issue on GitHub](https://github.com/catalyst-cooperative/pudl/issues/new?template=data_bug_report.yml)
  or email us at [hello@catalyst.coop](mailto:hello.catalyst.coop)!
* If you see any static entity attributes that should actually be allowed to vary from
  year to year, it’s easy for us to move them from the entity table to the SCD table.

## Related Tables And Source Documentation

To understand the source data that feed entity resolution, see:

* [EIA Form 860 – Annual Electric Generator Report](../data_sources/eia860.md)
* [EIA Form 923 – Power Plant Operations Report](../data_sources/eia923.md)

For the metadata and code that implement this logic, see:

* [`pudl.metadata.resources.ENTITIES`](../autoapi/pudl/metadata/resources/index.md#pudl.metadata.resources.ENTITIES)
* [`pudl.transform.eia`](../autoapi/pudl/transform/eia/index.md#module-pudl.transform.eia)
* [`pudl.transform.eia.harvested_entity_asset_factory()`](../autoapi/pudl/transform/eia/index.md#pudl.transform.eia.harvested_entity_asset_factory)
* [`pudl.transform.eia.harvest_entity_tables()`](../autoapi/pudl/transform/eia/index.md#pudl.transform.eia.harvest_entity_tables)
* [`pudl.transform.eia._compile_all_entity_records()`](../autoapi/pudl/transform/eia/index.md#pudl.transform.eia._compile_all_entity_records)
* [`pudl.transform.eia.occurrence_consistency()`](../autoapi/pudl/transform/eia/index.md#pudl.transform.eia.occurrence_consistency)
