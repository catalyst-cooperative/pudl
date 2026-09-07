"""Tests for FERC-to-SQLite prerequisite ordering.

Downstream transforms that read a FERC SQLite DB via the (unexecutable) raw
``AssetSpec`` layer must also declare an explicit ``deps`` on the ``__sqlite``
asset, so the execution plan actually waits for the DBF/XBRL conversion. Without
it the ordering is left to chance and a wide executor can start a transform
before its database exists.
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
