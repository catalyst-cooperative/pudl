# pudl.metadata.resource_helpers

Functions for resource metadata.

These live in pudl.metadata and not in pudl.metadata.resources because we have
machinery that iterates over the contents of pudl.metadata.resources and needs
each module there to actually store resource metadata.

## Attributes

| [`HARVESTED_CORE_TABLES_RUS12`](#pudl.metadata.resource_helpers.HARVESTED_CORE_TABLES_RUS12)         |    |
|------------------------------------------------------------------------------------------------------|----|
| [`HARVESTED_CORE_TABLES_RUS7`](#pudl.metadata.resource_helpers.HARVESTED_CORE_TABLES_RUS7)           |    |
| [`HARVESTING_DETAIL_TEXT_EIA`](#pudl.metadata.resource_helpers.HARVESTING_DETAIL_TEXT_EIA)           |    |
| [`HARVESTING_DETAIL_TEXT_RUS`](#pudl.metadata.resource_helpers.HARVESTING_DETAIL_TEXT_RUS)           |    |
| [`HARVESTING_FORENSIC_DETAIL_TEXT`](#pudl.metadata.resource_helpers.HARVESTING_FORENSIC_DETAIL_TEXT) |    |

## Functions

| [`canonical_harvested_details`](#pudl.metadata.resource_helpers.canonical_harvested_details)(→ str)             | Generate additional details text for one of the eight core harvested tables.                                              |
|-----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| [`inherits_harvested_values_details`](#pudl.metadata.resource_helpers.inherits_harvested_values_details)(→ str) | Generate additional details text for a table which inherits harvested values from one of the eight core harvested tables. |
| [`merge_descriptions`](#pudl.metadata.resource_helpers.merge_descriptions)(→ dict[str, Any])                    | Merge two description dictionaries.                                                                                       |
| [`core_to_out_harvested_resources`](#pudl.metadata.resource_helpers.core_to_out_harvested_resources)(→ dict)    | Make out tables from core resource metadata when extra columns are standard.                                              |

## Module Contents

### pudl.metadata.resource_helpers.HARVESTED_CORE_TABLES_RUS12 *= ['core_rus12_\_yearly_meeting_and_board', 'core_rus12_\_yearly_balance_sheet_assets',...*

### pudl.metadata.resource_helpers.HARVESTED_CORE_TABLES_RUS7 *= ['core_rus7_\_yearly_meeting_and_board', 'core_rus7_\_yearly_balance_sheet_assets',...*

### pudl.metadata.resource_helpers.HARVESTING_DETAIL_TEXT_EIA *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""EIA reports many attributes in many different tables across
EIA-860 and EIA-923. In order to compile tidy, well-normalized database tables, PUDL
collects all instances of these values and and chooses a canonical value. By default,
PUDL chooses the most consistently reported value of a given attribute as long as it
is at least 70% of the given instances reported. If an attribute was reported
inconsistently across the original EIA tables, then it will show up as a
null value. See :doc:`/methodology/entity_resolution` for a conceptual overview of
this process."""
```

</details>

### pudl.metadata.resource_helpers.HARVESTING_DETAIL_TEXT_RUS *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""RUS reports many attributes in many different tables
across throughout RUS-7 and RUS-12. In order to compile tidy, well-normalized database
tables, PUDL collects all instances of these values and and chooses a canonical value.
By default, PUDL chooses the most consistently reported value of a given attribute as
long as it is at least 70% of the given instances reported. For the ``borrower_name_rus``
PUDL chooses the most consistently reported value regardless of if it meets this 70%
threshold so that all borrowers will have a name. We chose this because most name
changes were insignificant (eg. "and" changed to "&" or "coop" changed to "cooperative").
All tables downstream of this one inherit the canonical values established
here."""
```

</details>

### pudl.metadata.resource_helpers.HARVESTING_FORENSIC_DETAIL_TEXT *= Multiline-String*

<details><summary>Show Value</summary>
```python
"""This is a forensic table containing the input values used to
choose canonical values during entity resolution. It is not a cleaned up table - it
is meant for forensic purposes only. If you have a question about why a value is
reported in an ``scd``, ``entity`` or ``out`` table, you can find out all of the inputs
that were used as ingredients to find the canonical value. You can filter by the
column_name and the entity id to find all of the possible input values."""
```

</details>

### pudl.metadata.resource_helpers.canonical_harvested_details(entities: [str](https://docs.python.org/3/library/stdtypes.html#str), is_static: [bool](https://docs.python.org/3/library/functions.html#bool)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Generate additional details text for one of the eight core harvested tables.

We have one core harvested table for each combination of (plants, utilities,
boilers, generators) X (static cols, annual cols):

> * `core_eia__entity_{plants|utilities|boilers|generators}` - static cols
> * `core_eia860__scd_{plants|utilities|boilers|generators}` - annual cols

This text helps users cross reference where the canonical values for each
type of entity come from, and why they may differ from a value they find in a
raw source.

* **Parameters:**
  * **entities** – string containing the plural of an entity type; e.g., “plants”
  * **is_static** – True if the table this text is destined for contains the static cols
    for the entity, False otherwise. Static cols are stored in tables with a name
    like “core_eia_\_entity_X”, and annual cols are stored in tables with a name
    like “core_eia860_\_scd_X”.

### pudl.metadata.resource_helpers.inherits_harvested_values_details(entities: [str](https://docs.python.org/3/library/stdtypes.html#str)) → [str](https://docs.python.org/3/library/stdtypes.html#str)

Generate additional details text for a table which inherits harvested values from one of the eight core harvested tables.

A table inherits harvested values from one of the eight core harvested tables
if it is downstream of one or more tables `core_eia__entity_{plants|utilities|boilers|generators}`
or `core_eia860__scd_{plants|utilities|boilers|generators}` and includes one or
more columns from the static or annual column lists in [`pudl.metadata.resources.ENTITIES`](../resources/index.md#pudl.metadata.resources.ENTITIES).

We have chosen to only add this warning to tables that inherit 3 or more columns from harvested
tables.

* **Parameters:**
  **entities** – a prose string listing which harvested entities contributed
  columns to this table; e.g., “generators and plants” for a table with
  `core_eia860__scd_generators` and `core_eia860__scd_plants` upstream.

### pudl.metadata.resource_helpers.merge_descriptions(left: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any], right: [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)[[str](https://docs.python.org/3/library/stdtypes.html#str), Any]

Merge two description dictionaries.

### pudl.metadata.resource_helpers.core_to_out_harvested_resources(core_table_names: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)], core_table_metadata: [dict](https://docs.python.org/3/library/stdtypes.html#dict), out_cols_to_add: [list](https://docs.python.org/3/library/stdtypes.html#list)[[str](https://docs.python.org/3/library/stdtypes.html#str)]) → [dict](https://docs.python.org/3/library/stdtypes.html#dict)

Make out tables from core resource metadata when extra columns are standard.
