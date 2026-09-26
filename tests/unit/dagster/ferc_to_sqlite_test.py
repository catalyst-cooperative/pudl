"""Tests for FERC-to-SQLite prerequisite wiring and scheduling.

These guard two easy-to-regress properties of the ``raw_ferc_to_sqlite`` assets:

* Downstream transforms that read a FERC SQLite DB via the (unexecutable) raw
  ``AssetSpec`` layer must also declare an explicit ``deps`` on the ``__sqlite``
  asset, so the execution plan actually waits for the DBF/XBRL conversion. Without
  it the ordering is left to chance and a wide executor can start a transform
  before its database exists.
* FERC Forms 2, 6, and 60 have no downstream consumers and get a negative
  ``dagster/priority`` so they backfill idle slots in the serial tail rather than
  stampeding at the start of the run.
"""

import dagster as dg
import pytest

from pudl.definitions import defs
from pudl.transform.ferc1 import FERC1_TFR_CLASSES

ASSET_GRAPH = defs.resolve_asset_graph()

FERC1_DBF_SQLITE = dg.AssetKey("raw_ferc1_dbf__sqlite")
FERC1_XBRL_SQLITE = dg.AssetKey("raw_ferc1_xbrl__sqlite")
FERC714_XBRL_SQLITE = dg.AssetKey("raw_ferc714_xbrl__sqlite")


@pytest.mark.parametrize(
    "table_name",
    [
        "core_ferc714__respondent_id",
        "core_ferc714__hourly_planning_area_demand",
        "core_ferc714__yearly_planning_area_demand_forecast",
    ],
)
def test_core_ferc714_depends_on_xbrl_sqlite(table_name: str) -> None:
    """Each FERC 714 core asset waits for the XBRL SQLite conversion."""
    parents = ASSET_GRAPH.get(dg.AssetKey(table_name)).parent_keys
    assert FERC714_XBRL_SQLITE in parents


@pytest.mark.parametrize("table_name", sorted(FERC1_TFR_CLASSES))
def test_core_ferc1_depends_on_sqlite(table_name: str) -> None:
    """Every FERC 1 transform asset waits for both FERC 1 SQLite conversions."""
    parents = ASSET_GRAPH.get(dg.AssetKey(table_name)).parent_keys
    assert {FERC1_DBF_SQLITE, FERC1_XBRL_SQLITE} <= parents


@pytest.mark.parametrize(
    ("asset_key", "expected_priority"),
    [
        ("raw_ferc1_dbf__sqlite", 10),
        ("raw_ferc1_xbrl__sqlite", 10),
        ("raw_ferc714_xbrl__sqlite", 10),
        ("raw_ferc2_dbf__sqlite", -10),
        ("raw_ferc2_xbrl__sqlite", -10),
        ("raw_ferc6_dbf__sqlite", -10),
        ("raw_ferc6_xbrl__sqlite", -10),
        ("raw_ferc60_dbf__sqlite", -10),
        ("raw_ferc60_xbrl__sqlite", -10),
    ],
)
def test_ferc_to_sqlite_scheduling_priority(
    asset_key: str, expected_priority: int
) -> None:
    """Forms 2/6/60 are deprioritized; the forms with consumers stay high."""
    op_tags = ASSET_GRAPH.get(dg.AssetKey(asset_key)).assets_def.op.tags
    assert int(op_tags["dagster/priority"]) == expected_priority
