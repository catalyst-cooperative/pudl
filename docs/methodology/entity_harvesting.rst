Entity Harvesting
===============================================================================

Overview
-------------------------------------------------------------------------------

Several of PUDL's most important EIA tables are not direct copies of a single EIA
spreadsheet. Instead, they are synthesized from many upstream EIA-860 and EIA-923
tables that report information about the same real-world entities in slightly different
ways.

This process is what PUDL calls *entity harvesting*.

What PUDL Means By "Entity Harvesting"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In database and data-integration terminology, this workflow is closely related to
`record linkage <https://en.wikipedia.org/wiki/Record_linkage>`__ and
`entity resolution <https://en.wikipedia.org/wiki/Record_linkage#Entity_resolution>`__.
In master data management, the output is often described as a
`golden record <https://en.wikipedia.org/wiki/Master_data_management>`__.

PUDL uses the project-specific phrase *entity harvesting* because the source records
are usually already grouped around known EIA identifiers like ``plant_id_eia`` or
``generator_id``. The difficult part is generally not discovering that two unrelated
records refer to the same real-world object. It is reconciling conflicting values from
many schedules and years into:

* one canonical entity record, and
* one yearly record for attributes that legitimately vary over time.

We developed the entity harvesting process because EIA data is not reported as a single
clean master list of plants, utilities, boilers, or generators. The same plant may
appear in many forms, worksheets, and years. Across those sources, attributes like plant
name, balancing authority, geographic coordinates, or operating dates may be:

* omitted in some tables,
* reported with slightly different spellings or codes,
* updated in one place but not another,
* associated with one year of reporting but not another, or
* attached to a related entity in one worksheet and the entity itself in another.

If PUDL exposed every upstream table independently without reconciling those
inconsistencies, many common analyses would require users to manually decide which value
to trust for each entity and year. Instead, PUDL builds normalized entity tables that
aim to provide a canonical or "golden record" for each entity and, where needed, a
yearly record of attributes that legitimately change over time.

This page explains the harvesting process conceptually, why it exists, and what it
means when the PUDL entity tables do not exactly mirror an individual raw EIA
spreadsheet.

What Harvesting Produces
~~~~~~~~~~~~~~~~~~~~~~~~

For each EIA entity type, PUDL creates two related tables:

* A static entity table such as :ref:`core_eia__entity_plants`, with one row per entity
  and attributes that are expected to be mostly stable over time.
* A yearly slowly changing dimension (SCD) table such as :ref:`core_eia860__scd_plants`,
  with one row per entity per report year and attributes that are expected to vary over
  time.

PUDL currently harvests four EIA entity types:

* Utilities (``utility_id_eia``)
* Plants (``plant_id_eia``)
* Boilers (``boiler_id``)
* Generators (``generator_id``)

In code, these are represented by :class:`pudl.transform.eia.EiaEntity`, and the
harvested tables are produced by the Dagster asset factory
:func:`pudl.transform.eia.harvested_entity_asset_factory`.

At a high level, the harvested tables answer two different questions:

* "What is the best canonical record for this entity overall?"
* "Which attributes of this entity appear to vary by report year?"

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
* If the upstream values are too inconsistent, PUDL may leave the harvested value null
  rather than constructing an unreliable entity record.
* A plant-year may appear in a harvested table even if it only showed up in one of the
  many upstream tables that feed the harvester.
* Conversely, if a table does not expose the expected entity identifier columns and
  ``report_date``, it cannot contribute records to the yearly harvested output.

This is generally desirable. A normalized analytical database is more useful when it
provides stable, reconciled identifiers and attributes rather than reproducing every
reporting inconsistency exactly as filed.

How The Harvester Works
-------------------------------------------------------------------------------

Where Harvesting Happens In The Dagster Graph
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Entity harvesting sits in the middle of the EIA transformation pipeline.

First, dataset-specific transforms create a set of unnormalized "core" tables. These
tables are already cleaned and typed, but they still largely reflect the structure of
their source schedules. Then the harvesting asset factory scans those tables for entity
IDs and harvestable attributes, producing normalized entity tables.

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
       harvest[harvested_plants_eia<br/>multi_asset]
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

The actual plant harvester consumes a larger list of upstream assets, including boiler,
generation fuel, energy storage, and association tables. In
:func:`pudl.transform.eia.harvested_entity_asset_factory`, those dependencies are listed
explicitly in the ``harvestable_assets`` tuple.

Conceptually, the same pattern is used for all four entity types.

How The Harvester Builds The Entity Universe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first step is not to choose attribute values. It is simply to determine which
entities, and which entity-years, exist anywhere in the upstream transformed EIA data.

That work is done in :func:`pudl.transform.eia._compile_all_entity_records`.

For a given entity type, the harvester:

1. Looks up the relevant identifier columns and harvestable attribute columns from the
   entity metadata in :py:const:`pudl.metadata.resources.ENTITIES`.
2. Scans each upstream transformed table.
3. Includes a table if it contains the required entity ID columns plus ``report_date``.
4. Pulls out the entity IDs, ``report_date``, and any harvestable attributes present in
   that table.
5. Records the table name as provenance so the source of each reported value is known
   during reconciliation.

For yearly harvesting, ``report_date`` is normalized to January 1 of the report year so
that all annual records align even if an upstream table encodes dates differently.

This produces a compiled staging dataframe containing every observed instance of the
entity across all harvestable upstream tables.

The next step is to derive the ID spaces for the two harvested outputs:

* The static entity table gets one row per unique entity ID.
* The yearly SCD table gets one row per unique ``(entity_id, report_date)`` pair.

This means an entity-year can enter the harvested SCD table from *any* harvestable
upstream asset that reports the right identifiers and ``report_date``.

Static Versus Annual Attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not every entity attribute is treated the same way.  PUDL divides attributes into two
conceptual groups:

* **Static attributes**: values that should usually describe the same real-world
  entity across all years, such as an entity name or latitude/longitude.
* **Annual attributes**: values that can change over time, such as a yearly
  balancing authority assignment or some generator characteristics.

This distinction is encoded in :py:const:`pudl.metadata.resources.ENTITIES`, not
inferred ad hoc during the ETL run.

The distinction matters because consistency is evaluated differently:

* Static attributes are reconciled across all occurrences of the entity.
* Annual attributes are reconciled separately for each ``(entity_id, report_date)``.

For users, this explains why one attribute ends up in ``core_eia__entity_*`` while
another appears in ``core_eia860__scd_*``.

How PUDL Chooses A Canonical Value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After compiling all reported values, PUDL reconciles each harvestable attribute one at
a time using :func:`pudl.transform.eia.harvest_entity_tables` and
:func:`pudl.transform.eia.occurrence_consistency`.

The process is:

1. Count how many times the entity or entity-year appears in the compiled data.
2. Count how many times each candidate value for the attribute appears.
3. Compute a consistency rate for each candidate value.
4. Keep a value only if it exceeds the configured strictness threshold.
5. Merge the accepted values back onto the entity ID table or entity-year table.

By default, a value must be reported consistently more than 70% of the time to be
harvested for a given entity or entity-year. If no candidate exceeds that threshold, the
harvested value remains null.

This is the core reason PUDL's normalized tables are often more stable and
analytically useful than any single EIA worksheet. The harvester is explicitly
designed to prefer the most consistently reported value rather than whichever
source happened to be processed last.

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

Special Cases And Post-Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some attributes are too messy to reconcile with the default consistency logic alone.

The harvester includes special-case handling for a few columns, including latitude,
longitude, and generator operating dates. These special cases exist because some values
are structurally noisy but the usability impacts have *not* choosing any value are too
serious.

The plant harvester also applies plant-specific post-processing after the main harvest:

* adding plants known from EPA CEMS that may not appear in the regular EIA tables,
* assigning time zones based on location, and
* filling or correcting balancing authority codes in some cases.

These steps happen after the core consistency-based reconciliation, which means the
harvested tables are not just copied from upstream transformed tables. They are the
result of several layers of normalization and cleanup.

There are a few cases in which we choose the most consistent value, even if it is not
particularly consistent. When there are only a small number of reported values, this
can result in a tie between two equally consistent values, each with a consistency of
less than or equal to 50%. In these cases, the harvested value may not be deterministic.
However, this scenario is quite rare.

How To Interpret The Outputs
-------------------------------------------------------------------------------

Why A Raw Spreadsheet May Not Match A PUDL Entity Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you compare a harvested PUDL entity table to a raw EIA spreadsheet, several kinds of
differences are expected:

* **A name or code differs** because PUDL selected the value that was most consistently
  reported across many tables and years.
* **A value is null in PUDL** because upstream reporting was too inconsistent
  to choose a canonical value confidently.
* **A plant-year exists in PUDL but not in the plant table you were
  viewing** because the plant-year was reported in a different harvestable
  upstream table.
* **An apparent relationship from a raw sheet is not reproduced exactly** because
  PUDL's entity outputs are normalized around canonical entity IDs and yearly records
  rather than the layout of any single filing schedule.

This is especially important when interpreting association tables. A reported
relationship seen in one worksheet may be incomplete, outdated, or inconsistent with
other schedules. PUDL's goal is not to preserve every raw inconsistency in the entity
tables, but to produce a coherent cross-table representation that works well for
analysis.

What This Means For Users
~~~~~~~~~~~~~~~~~~~~~~~~~

When working with PUDL's harvested EIA entity tables, it helps to keep the following in
mind:

* Treat the harvested entity tables as PUDL's best normalized representation of the EIA
  entities, not as direct extracts from one filing schedule.
* If you need to understand why a value looks surprising, inspect the upstream
  unnormalized ``_core_*`` tables or the source spreadsheets and compare
  reporting across forms and years. (Note that these upstream unnormalized tables are
  only accessible from within the Dagster pipeline, and are not generally published.)
* Apparent discrepancies often reflect real inconsistencies in the source data rather
  than arbitrary transformation choices.
* In many analytical workflows, the canonical harvested value is preferable to manually
  selecting among inconsistent raw reports.

Related Tables And Source Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To understand the source data that feed entity harvesting, see:

* :doc:`/data_sources/eia860`
* :doc:`/data_sources/eia923`

For the metadata and code that implement the harvesting logic, see:

* :py:const:`pudl.metadata.resources.ENTITIES`
* :mod:`pudl.transform.eia`
* :func:`pudl.transform.eia.harvested_entity_asset_factory`
* :func:`pudl.transform.eia.harvest_entity_tables`
* :func:`pudl.transform.eia._compile_all_entity_records`
* :func:`pudl.transform.eia.occurrence_consistency`
