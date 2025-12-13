"""Functions for resource metadata.

These live in pudl.metadata and not in pudl.metadata.resources because we have
machinery that iterates over the contents of pudl.metadata.resources and needs
each module there to actually store resource metadata.
"""

from typing import Any

HARVESTING_DETAIL_TEXT = """EIA reports many attributes in many different tables across
EIA-860 and EIA-923. In order to compile tidy, well-normalized database tables, PUDL
collects all instances of these values and and chooses a canonical value. By default,
PUDL chooses the most consistently reported value of a given attribute as long as it
is at least 70% of the given instances reported. If an attribute was reported
inconsistently across the original EIA tables, then it will show up as a
null value."""


def canonical_harvested_details(entities: str, is_static: bool) -> str:
    """Generate additional details text for one of the eight core harvested tables.

    We have one core harvested table for each combination of (plants, utilities,
    boilers, generators) X (static cols, annual cols):

      * ``core_eia__entity_{plants|utilities|boilers|generators}`` - static cols
      * ``core_eia860__scd_{plants|utilities|boilers|generators}`` - annual cols

    This text helps users cross reference where the canonical values for each
    type of entity come from, and why they may differ from a value they find in a
    raw source.

    Args:
        entities: string containing the plural of an entity type; e.g., "plants"
        is_static: True if the table this text is destined for contains the static cols
            for the entity, False otherwise. Static cols are stored in tables with a name
            like "core_eia__entity_X", and annual cols are stored in tables with a name
            like "core_eia860__scd_X".
    """
    return f"""This is one of two tables where canonical
values for {entities} are set. It contains values which are expected to {"remain fixed" if is_static else "vary slowly"}, while
:ref:`core_eia{"860" if is_static else ""}__{"scd" if is_static else "entity"}_{entities}` contains those {"which may vary from year to year" if is_static else "expected to remain fixed"}.
{HARVESTING_DETAIL_TEXT}
All tables downstream of this one inherit the canonical values established here."""


def inherits_harvested_values_details(entities: str) -> str:
    """Generate additional details text for a table which inherits harvested values from one of the eight core harvested tables.

    A table inherits harvested values from one of the eight core harvested tables
    if it is downstream of one or more tables ``core_eia__entity_{plants|utilities|boilers|generators}``
    or ``core_eia860__scd_{plants|utilities|boilers|generators}`` and includes one or
    more columns from the static or annual column lists in :data:`pudl.metadata.resources.ENTITIES`.

    We have chosen to only add this warning to tables that inherit 3 or more columns from harvested
    tables.

    Args:
        entities: a prose string listing which harvested entities contributed
            columns to this table; e.g., "generators and plants" for a table with
            ``core_eia860__scd_generators`` and ``core_eia860__scd_plants`` upstream.
    """
    return f"""This table inherits canonicalized values for {entities}.
{HARVESTING_DETAIL_TEXT}"""


def merge_descriptions(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Merge two description dictionaries."""
    result = {}
    result.update(left)
    for key in right:
        if key in result:
            if key == "usage_warnings":
                result[key] = result[key] + right[key]
            elif key == "additional_details_text":
                result[key] = f"{result[key]}\n\n{right[key]}"
            else:
                result[key] = f"{result[key]} {right[key]}"
        else:
            result[key] = right[key]
    return result
