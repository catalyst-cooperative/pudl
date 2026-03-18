"""Functions for resource metadata.

These live in pudl.metadata and not in pudl.metadata.resources because we have
machinery that iterates over the contents of pudl.metadata.resources and needs
each module there to actually store resource metadata.
"""

import copy
from typing import Any

HARVESTED_CORE_TABLES_RUS12 = [
    "core_rus12__yearly_meeting_and_board",
    "core_rus12__yearly_balance_sheet_assets",
    "core_rus12__yearly_balance_sheet_liabilities",
    "core_rus12__yearly_long_term_debt",
    "core_rus12__yearly_renewable_plants",
    "core_rus12__yearly_lines_stations_labor_materials_cost",
    "core_rus12__yearly_sources_and_distribution_by_plant_type",
    "core_rus12__yearly_sources_and_distribution",
    "core_rus12__yearly_loans",
    "core_rus12__yearly_plant_labor",
    "core_rus12__yearly_statement_of_operations",
    "core_rus12__yearly_plant_costs",
    "core_rus12__yearly_plant_operations_by_borrower",
    "core_rus12__yearly_plant_operations_by_plant",
    "core_rus12__yearly_investments",
    "core_rus12__yearly_external_financial_risk_ratio",
]

HARVESTED_CORE_TABLES_RUS7 = [
    "core_rus7__yearly_meeting_and_board",
    "core_rus7__yearly_balance_sheet_assets",
    "core_rus7__yearly_balance_sheet_liabilities",
    "core_rus7__yearly_employee_statistics",
    "core_rus7__yearly_energy_efficiency",
    "core_rus7__yearly_power_requirements_electric_customers",
    "core_rus7__yearly_power_requirements_electric_sales",
    "core_rus7__yearly_power_requirements",
    "core_rus7__yearly_investments",
    "core_rus7__yearly_long_term_leases",
    "core_rus7__yearly_patronage_capital",
    "core_rus7__yearly_statement_of_operations",
    "core_rus7__yearly_loans",
    "core_rus7__yearly_external_financial_risk_ratio",
    "core_rus7__yearly_long_term_debt",
]

HARVESTING_DETAIL_TEXT_EIA = """EIA reports many attributes in many different tables across
EIA-860 and EIA-923. In order to compile tidy, well-normalized database tables, PUDL
collects all instances of these values and and chooses a canonical value. By default,
PUDL chooses the most consistently reported value of a given attribute as long as it
is at least 70% of the given instances reported. If an attribute was reported
inconsistently across the original EIA tables, then it will show up as a
null value. See :doc:`/methodology/entity_harvesting` for a conceptual overview of
this process."""


HARVESTING_DETAIL_TEXT_RUS = """RUS reports many attributes in many different tables
across throughout RUS-7 and RUS-12. In order to compile tidy, well-normalized database
tables, PUDL collects all instances of these values and and chooses a canonical value.
By default, PUDL chooses the most consistently reported value of a given attribute as
long as it is at least 70% of the given instances reported. For the ``borrower_name_rus``
PUDL chooses the most consistently reported value regardless of if it meets this 70%
threshold so that all borrowers will have a name. We chose this because most name
changes were insignificant (eg. "and" changed to "&" or "coop" changed to "cooperative").
All tables downstream of this one inherit the canonical values established
here."""


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
{HARVESTING_DETAIL_TEXT_EIA}
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
{HARVESTING_DETAIL_TEXT_EIA}"""


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


def core_to_out_harvested_resources(
    core_table_names: list[str],
    core_table_metadata: dict,
    out_cols_to_add: list[str],
) -> dict:
    """Make out tables from core resource metadata when extra columns are standard."""
    # We are **not** trying to edit the core metadata, just build new stuff from it
    # but dicts are very mutable so we must deepcopy
    copied_table_metadata = copy.deepcopy(core_table_metadata)
    out_resources = {}
    for core_tbl in core_table_names:
        meta_tbl = {}
        for meta_part_name, meta_part in copied_table_metadata[core_tbl].items():
            if meta_part_name == "schema":
                # If we had any table-specific columns we could relatively easily do something like
                # out_cols_to_add = (
                #     special_cols[core_tlb]
                #     if core_tlb in special_cols.keys()
                #     else out_cols_to_add
                # )
                meta_part["fields"] = out_cols_to_add + meta_part["fields"]
            elif meta_part_name == "description":
                meta_part["usage_warnings"] = ["harvested"] + meta_part[
                    "usage_warnings"
                ]
            meta_tbl[meta_part_name] = meta_part
        out_resources[f"out_{core_tbl.removeprefix('core_')}"] = meta_tbl
    return out_resources
