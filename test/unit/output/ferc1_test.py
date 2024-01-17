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

import networkx as nx
import pandas as pd

from pudl.output.ferc1 import NodeId, XbrlCalculationForestFerc1

logger = logging.getLogger(__name__)


# TODO: combine these into a class because we have a lot of similar method names
# TODO: make graph construction easier with helper functions


def test_annotated_forest_propagates_leafward():
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
    tags = pd.DataFrame([parent]).assign(in_rate_base="yes")
    simple_forest = XbrlCalculationForestFerc1(
        exploded_meta=exploded_meta,
        exploded_calcs=exploded_calcs,
        seeds=[parent],
        tags=tags,
    )
    annotated_forest = simple_forest.annotated_forest
    assert len(annotated_forest.nodes) == 3
    annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
    assert annotated_tags[parent]["in_rate_base"] == "yes"
    assert (
        annotated_tags[parent]["in_rate_base"] == annotated_tags[child1]["in_rate_base"]
    )
    assert (
        annotated_tags[parent]["in_rate_base"] == annotated_tags[child2]["in_rate_base"]
    )


def test_annotated_forest_propagates_rootward():
    pass


def test_annotated_forest_propagates_corrections():
    pass


def test_annotate_forest_propagates_both_dirs_with_corrections():
    pass


def test_annotate_forest_does_not_propagate():
    # if a parent has two disagreeing children
    pass


def test_annoted_forest_does_propagate_null_and_value():
    # if a parent has some children with one value and some with nulls
    pass
