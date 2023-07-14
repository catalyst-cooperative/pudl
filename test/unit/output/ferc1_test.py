"""Tests for the FERC Form 1 output functions.

These need to be recreated to work with the new XbrlCalculationForest implementation.
"""

import json
import logging

import pandas as pd

from pudl.output.ferc1 import NodeId, XbrlCalculationForestFerc1

logger = logging.getLogger(__name__)

EXPLODED_META_IDX = ["table_name", "xbrl_factoid"]
TEST_CALC_1 = [
    {"name": "reported_1", "weight": 1.0, "source_tables": ["table_1"]},
    {"name": "reported_2", "weight": -1.0, "source_tables": ["table_1"]},
]

TEST_CALC_2 = [
    {"name": "reported_1", "weight": 1.0, "source_tables": ["table_1", "table_2"]},
    {"name": "reported_2", "weight": -1.0, "source_tables": ["table_1"]},
]

TEST_CALC_3 = [
    {"name": "reported_1", "weight": 1.0, "source_tables": ["table_1"]},
    {"name": "reported_3", "weight": 1.0, "source_tables": ["table_3"]},
]

TEST_EXPLODED_META: pd.DataFrame = (
    pd.DataFrame(
        columns=["table_name", "xbrl_factoid", "calculations", "xbrl_factoid_original"],
        data=[
            ("table_1", "reported_1", "[]", "reported_original_1"),
            ("table_1", "reported_2", "[]", "reported_original_2"),
            ("table_1", "calc_1", json.dumps(TEST_CALC_1), "calc_original_1"),
            ("table_2", "calc_2", json.dumps(TEST_CALC_2), "calc_original_2"),
            ("table_1", "calc_3", json.dumps(TEST_CALC_3), "calc_original_3"),
        ],
    )
    .convert_dtypes()
    .set_index(EXPLODED_META_IDX)
)

LEAF_NODE_1 = XbrlCalculationForestFerc1(
    exploded_meta=TEST_EXPLODED_META,
    seeds=[NodeId("table_1", "reported_1")],
)
LEAF_NODE_2 = XbrlCalculationForestFerc1(
    exploded_meta=TEST_EXPLODED_META,
    seeds=[NodeId("table_1", "reported_2")],
)
CALC_TREE_1 = XbrlCalculationForestFerc1(
    exploded_meta=TEST_EXPLODED_META,
    seeds=[NodeId("table_1", "calc_1")],
)
CALC_TREE_2 = XbrlCalculationForestFerc1(
    exploded_meta=TEST_EXPLODED_META,
    seeds=[NodeId("table_2", "calc_2")],
)
