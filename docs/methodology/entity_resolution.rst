Entity Resolution
===============================================================================

Overview
-------------------------------------------------------------------------------

Many of the datasets PUDL processes report the same information about individual
entities in many different places. This is usually done for readability. For example,
it's nice to list the names of plants or utilities instead of just their IDs. This makes
the original data easier for humans to use but it also introduces the potential for
internal inconsistencies (`entity <https://en.wikipedia.org/wiki/Entity_integrity>`__ or
`referential integrity <https://en.wikipedia.org/wiki/Referential_integrity>`__ issues)
which create problems when you're trying to re-use the data in other applications.

PUDL attempts to identify canonical values ("`golden records
<https://en.wikipedia.org/wiki/Master_data_management>`__") for entities from
the potentially inconsistent original data through an `entity resolution
<https://en.wikipedia.org/wiki/Record_linkage#Entity_resolution>`__ process (often
called "entity harvesting" in the PUDL codebase).

This process is applied most extensively to the :doc:`EIA-860 </data_sources/eia860>`
and :doc:`EIA-923 </data_sources/eia923>` spreadsheet data, but it's a more general
issue that comes up throughout the data we process.

.. note::

  We have **NOT** yet applied this process to the :doc:`EIA-861 </data_sources/eia861>`
  tables, so they still reflect the original, internally inconsistent reporting, and any
  utilities which only appear in the EIA-861 data do not yet show up in the utility
  entity tables.

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

This page explains the entity resolution process conceptually, why it exists, and what
it means when the PUDL entity tables do not exactly mirror an individual raw EIA
spreadsheet.

What Entity Resolution Produces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For each entity type, PUDL typically creates two related tables:

* A static entity table such as :ref:`core_eia__entity_plants`, with one row per entity
  and attributes that are expected to be stable over time.
* A yearly slowly changing dimension (SCD) table such as :ref:`core_eia860__scd_plants`,
  with one row per entity per report year and attributes that are expected to vary
  slightly over time.

PUDL currently resolves four kinds of EIA entities:

* Utilities (``utility_id_eia``)
* Plants (``plant_id_eia``)
* Boilers (``boiler_id``)
* Generators (``generator_id``)

Why A Canonical Record Is Necessary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Users often expect a one-to-one correspondence between a raw EIA spreadsheet row and a
PUDL entity record. That is usually not the right mental model.

For example, an EIA plant may be named slightly differently across the plant table,
generator table, ownership table, and emissions-control tables. One table might use an
older balancing authority code. Another might omit latitude and longitude. A generator
operating date might be reported consistently in most years, but differ in one source.

When this happens, PUDL does not treat every reported value as equally authoritative.
Instead, it looks across all relevant upstream tables and asks: which value is reported
most consistently for this entity?

That consistency-based approach has several practical consequences:

* A PUDL entity attribute may match most upstream sources, but not a specific raw file a
  user happens to be looking at.
* If the upstream values are too inconsistent, PUDL may leave the resolved value null
  rather than constructing an unreliable entity record.
* A plant-year may appear in a resolved yearly table even if it only showed up in one
  of the upstream tables that contribute to the resolution process.
* Conversely, if a table does not expose the expected entity identifier columns and
  ``report_date``, it cannot contribute records to the yearly resolved output.

This is usually desirable. A normalized analytical database is more useful when it
provides stable, reconciled identifiers and attributes rather than reproducing every
reporting inconsistency exactly as filed.

How Entity Resolution Works in PUDL
-------------------------------------------------------------------------------

Where Entity Resolution Happens in the PUDL Data Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Entity resolution sits in the middle of the EIA transformation pipeline.

First, dataset-specific transforms create a set of unnormalized "core" tables. These
tables are already cleaned and typed, but they still largely reflect the structure of
the source data. Then we scan those tables for entity IDs and their known attributes,
producing normalized entity tables.

The simplified flow for plants looks like this:

.. mermaid::

   flowchart LR
       raw[Raw EIA-860 and EIA-923 schedules]
       core1[_core_eia860__plants]
       core2[_core_eia860__generators]
       core3[_core_eia860__ownership]
       core4[_core_eia923__generation]
       core5[_core_eia923__fuel_receipts_costs]
       core6[_core_eia860__emissions_control_equipment]
       harvest[harvested_plants_eia]
       entity[core_eia__entity_plants]
       scd[core_eia860__scd_plants]
       outputs[out_eia__yearly_plants<br/>and other downstream outputs]

       raw --> core1
       raw --> core2
       raw --> core3
       raw --> core4
       raw --> core5
       raw --> core6
       core1 --> harvest
       core2 --> harvest
       core3 --> harvest
       core4 --> harvest
       core5 --> harvest
       core6 --> harvest
       harvest --> entity
       harvest --> scd
       entity --> outputs
       scd --> outputs

The full list of input tables for all EIA entities can be found in
:py:const:`pudl.transform.eia.HARVESTABLE_ASSETS`

Conceptually, the same pattern is used for all four entity types.

How PUDL Identifies Existing Entities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first step is to determine which entities and entity-years exist anywhere in the
upstream transformed EIA data. This is done in
:func:`pudl.transform.eia._compile_all_entity_records`.

To produce the annually varying SCD tables ``report_date`` is normalized to January 1 of
the report year so that all annual records align even if an upstream table has a
different temporal resolution.

This results in an interim dataframe containing every observed instance of the entity
across all the upstream tables.

The next step is to derive the ID spaces for the entity and annual SCD tables.

* The static entity table gets one row per unique entity ID.
* The yearly SCD table gets one row per unique ``(entity_id, report_date)`` pair.

This means an entity-year can enter the yearly resolved table from *any* upstream asset
that reports the entity ID column and ``report_date``.

How PUDL Chooses A Canonical Value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When there are multiple values reported for the same entity, in most cases, PUDL chooses
the most consistent value reported so long as it is found in at least 70% of available
entries. If no observed value occurs more than 70% of the time, PUDL fills in a null
value.

The selection logic looks like this conceptually:

.. mermaid::

   flowchart TD
       a[All reported values for one attribute]
       b{Static or annual?}
       c[Group by entity ID]
       d[Group by entity ID and report_date]
       e[Count entity occurrences]
       f[Count occurrences of each candidate value]
       g{Any value exceeds strictness threshold?}
       h[Keep the most consistent value]
       i[Leave harvested value null]
       j[Merge into core_eia__entity_* or core_eia860__scd_*]

       a --> b
       b -->|Static| c
       b -->|Annual| d
       c --> e
       d --> e
       e --> f
       f --> g
       g -->|Yes| h
       g -->|No| i
       h --> j
       i --> j

Special Cases and Post-Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

The plant-resolution process also applies plant-specific post-processing after the main
reconciliation step:

* adding plants known from :doc:`EPA CEMS </data_sources/epacems>` that may not appear
  in the regular EIA tables,
* assigning time zones based on plant location, and
* filling or correcting balancing authority codes in some cases.

These steps happen after the core consistency-based reconciliation, which means the
resolved tables are not just copied from upstream transformed tables. They are the
result of several layers of normalization and cleanup.

Because we always choose the most consistent prime mover code, even when less than 50%
of occurrences have that value, we occasionally see a tie between two equally consistent
values. In these cases, the resolved value may not be deterministic. Because prime mover
code is part of the primary key in some tables it is possible for some records to appear
or disappear based on which value was chosen. However, this scenario is quite rare.

Interpreting Discrepancies
-------------------------------------------------------------------------------

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
with other sources. PUDL's goal is not to preserve every raw inconsistency in the entity
tables, but to produce a coherent cross-table representation that works well for
analysis.

Improving PUDL Entity Resolution
-------------------------------------------------------------------------------

The entity resolution process is heuristic and can definitely be improved.

* If there are particular columns that you think would benefit from a domain specific
  consistency metric, please let us know!
* If you see any static entity attributes that should actually be allowed to vary from
  year to year, it's easy for us to move them from the entity table to the SCD table.

Related Tables And Source Documentation
-------------------------------------------------------------------------------

To understand the source data that feed entity resolution, see:

* :doc:`/data_sources/eia860`
* :doc:`/data_sources/eia923`

For the metadata and code that implement this logic, see:

* :py:const:`pudl.metadata.resources.ENTITIES`
* :mod:`pudl.transform.eia`
* :func:`pudl.transform.eia.harvested_entity_asset_factory`
* :func:`pudl.transform.eia.harvest_entity_tables`
* :func:`pudl.transform.eia._compile_all_entity_records`
* :func:`pudl.transform.eia.occurrence_consistency`
