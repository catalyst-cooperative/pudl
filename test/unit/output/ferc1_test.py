"""Tests for the FERC Form 1 output functions.

These need to be recreated to work with the new XbrlCalculationForest implementation.

Stuff to test:
- construction of basic tree from input metadata
- do nodes not part of any calculation get orphaned?
- do nodes not in the seeded digraph get pruned?
- does it fail when we have a cycle in the graph?
- does it identify stepparent nodes correctly?
- does it identify stepchild nodes correctly?
- pruning of passthrough nodes & associated corrections
- propagation of weights
- conflicting weights
- propagation of tags
- conflicting tags
- validation of calculations using only leaf-nodes to reproduce root node values

"""

import logging

import pandas as pd

from pudl.output.ferc1 import NodeId, XbrlCalculationForestFerc1

logger = logging.getLogger(__name__)


# TODO: give this a better name once we know what behavior we're actually testing
def test_annotated_forest():
    tags = pd.DataFrame(
        columns=[
            "table_name",
            "xbrl_factoid",
            "utility_type",
            "plant_status",
            "plant_function",
        ]
    )
    parent = NodeId(
        table_name="table_1",
        xbrl_factoid="reported_1",
        utility_type="electric",
        plant_status=pd.NA,
        plant_function=pd.NA,
    )
    child1 = NodeId(
        table_name="table_1",
        xbrl_factoid="reported_1_1",
        utility_type="electric",
        plant_status=pd.NA,
        plant_function=pd.NA,
    )
    child2 = NodeId(
        table_name="table_1",
        xbrl_factoid="reported_1_2",
        utility_type="electric",
        plant_status=pd.NA,
        plant_function=pd.NA,
    )

    edges = [(parent, child1), (parent, child2)]
    records = []
    for parent, child in edges:
        record = {"weight": 1}
        for field in NodeId._fields:
            record[f"{field}_parent"] = parent.__getattribute__(field)
            record[field] = child.__getattribute__(field)
        records.append(record)
    dtype_child = {col: pd.StringDtype() for col in NodeId._fields}
    dtype_parent = {f"{col}_parent": pd.StringDtype() for col in NodeId._fields}
    dtype_weight = {"weight": pd.Int64Dtype()}
    exploded_calcs = pd.DataFrame.from_records(records).astype(
        dtype_child | dtype_parent | dtype_weight
    )
    exploded_meta = pd.DataFrame([parent, child1, child2]).astype(dtype_child)

    simple_forest = XbrlCalculationForestFerc1(
        exploded_meta=exploded_meta,
        exploded_calcs=exploded_calcs,
        seeds=[parent],
        tags=tags,
    )
    assert len(simple_forest.annotated_forest.nodes) == 3
