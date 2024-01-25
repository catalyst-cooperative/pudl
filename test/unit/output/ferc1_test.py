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
import unittest

import networkx as nx
import pandas as pd
import pytest

from pudl.output.ferc1 import NodeId, XbrlCalculationForestFerc1

logger = logging.getLogger(__name__)


# TODO: combine these into a class because we have a lot of similar method names
# TODO: make graph construction easier with helper functions


class TestTagPropagation(unittest.TestCase):
    def setUp(self):
        self.parent = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.parent_correction = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1_correction",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.child1 = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1_1",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.child2 = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1_2",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.grand_child11 = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1_1_1",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.grand_child12 = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1_1_2",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.child1_correction = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1_1_correction",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        dtype_node = {col: pd.StringDtype() for col in NodeId._fields}
        self.exploded_meta = pd.DataFrame(
            [
                self.parent,
                self.child1,
                self.child2,
                self.grand_child11,
                self.grand_child12,
                self.child1_correction,
            ]
        ).astype(dtype_node)

    def _exploded_calcs_from_edges(self, edges: list[tuple[NodeId, NodeId]]):
        records = []
        for parent, child in edges:
            record = {"weight": 1}
            for field in NodeId._fields:
                record[f"{field}_parent"] = parent.__getattribute__(field)
                record[field] = child.__getattribute__(field)
            records.append(record)
        dtype_parent = {f"{col}_parent": pd.StringDtype() for col in NodeId._fields}
        dtype_child = {col: pd.StringDtype() for col in NodeId._fields}
        dtype_weight = {"weight": pd.Int64Dtype()}

        return pd.DataFrame.from_records(records).astype(
            dtype_child | dtype_parent | dtype_weight
        )

    def test_leafward_prop_undecided_children(self):
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=["yes", pd.NA, pd.NA]
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )

        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.parent]["in_rate_base"] == "yes"
        assert annotated_tags[self.child1]["in_rate_base"] == "yes"
        assert annotated_tags[self.child2]["in_rate_base"] == "yes"

    def test_leafward_prop_disagreeing_child(self):
        """Don't force the diagreeing child to follow the parent."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=["yes", "no", pd.NA]
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )

        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.parent]["in_rate_base"] == "yes"
        assert annotated_tags[self.child1]["in_rate_base"] == "no"
        assert annotated_tags[self.child2]["in_rate_base"] == "yes"

    def test_leafward_prop_preserve_non_propagating_tags(self):
        """Don't force the diagreeing child to follow the parent."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=["yes", "no", pd.NA],
            in_root_boose=["yus", "nu", "purtiul"],
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )

        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.parent]["in_rate_base"] == "yes"
        assert annotated_tags[self.child1]["in_rate_base"] == "no"
        assert annotated_tags[self.child2]["in_rate_base"] == "yes"
        assert annotated_tags[self.parent]["in_root_boose"] == "yus"
        assert annotated_tags[self.child1]["in_root_boose"] == "nu"
        assert annotated_tags[self.child2]["in_root_boose"] == "purtiul"

    def test_rootward_prop_disagreeing_children(self):
        """Parents should not pick sides between disagreeing children."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=[pd.NA, "no", "yes"]
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )

        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.parent] == {}
        assert annotated_tags[self.child1]["in_rate_base"] == "no"
        assert annotated_tags[self.child2]["in_rate_base"] == "yes"

    @pytest.mark.xfail(
        reason="we haven't implemented this behavior correctly yet", strict=True
    )
    def test_prop_no_tags(self):
        """If no tags, don't propagate anything."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=[pd.NA, pd.NA, pd.NA]
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )

        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.parent] == {}
        assert annotated_tags[self.child1] == {}
        assert annotated_tags[self.child2] == {}

        tags = pd.DataFrame(columns=NodeId._fields).convert_dtypes()

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )

        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.parent] == {}
        assert annotated_tags[self.child1] == {}
        assert annotated_tags[self.child2] == {}

    def test_annotated_forest_propagates_rootward(self):
        edges = [
            (self.parent, self.child1),
            (self.parent, self.child2),
            (self.child1, self.grand_child11),
            (self.child1, self.grand_child12),
        ]
        tags = pd.DataFrame([self.grand_child11, self.grand_child12]).assign(
            in_rate_base=["yes", "yes"]
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )
        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 5
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        # TODO: WHY THO it doesn't show up
        # assert annotated_tags[self.parent] == {}
        assert annotated_tags.get(self.parent, {}) == {}
        assert annotated_tags[self.child1]["in_rate_base"] == "yes"
        assert annotated_tags.get(self.child2, {}) == {}
        assert annotated_tags[self.grand_child11]["in_rate_base"] == "yes"
        assert annotated_tags[self.grand_child12]["in_rate_base"] == "yes"

    def test_annotated_forest_propagates_rootward_disagreeing_sibling(self):
        edges = [
            (self.parent, self.child1),
            (self.parent, self.child2),
            (self.child1, self.grand_child11),
            (self.child1, self.grand_child12),
        ]
        tags = pd.DataFrame([self.grand_child11, self.grand_child12]).assign(
            in_rate_base=["yes", "no"]
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )
        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 5
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags.get(self.parent, {}) == {}
        assert annotated_tags.get(self.child1, {}) == {}
        assert annotated_tags.get(self.child2, {}) == {}
        assert annotated_tags[self.grand_child11]["in_rate_base"] == "yes"
        assert annotated_tags[self.grand_child12]["in_rate_base"] == "no"

    def test_annotated_forest_propagates_rootward_correction(self):
        edges = [
            (self.child1, self.grand_child11),
            (self.child1, self.child1_correction),
        ]
        tags = pd.DataFrame([self.child1]).assign(in_rate_base=["yes"])

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.child1],
            tags=tags,
        )
        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 3
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        assert annotated_tags[self.child1]["in_rate_base"] == "yes"
        assert annotated_tags[self.grand_child11]["in_rate_base"] == "yes"
        assert annotated_tags[self.child1_correction]["in_rate_base"] == "yes"

    @pytest.mark.xfail(
        reason="we haven't implemented this behavior correctly yet", strict=True
    )
    def test_annotated_forest_propagates_rootward_two_layers(self):
        edges = [
            (self.parent, self.child1),
            (self.parent, self.child2),
            (self.child1, self.grand_child11),
            (self.child1, self.grand_child12),
        ]
        pre_assigned_yes_nodes = [self.child2, self.grand_child11, self.grand_child12]
        tags = pd.DataFrame(pre_assigned_yes_nodes).assign(
            in_rate_base=["yes"] * len(pre_assigned_yes_nodes),
        )

        simple_forest = XbrlCalculationForestFerc1(
            exploded_meta=self.exploded_meta,
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.parent],
            tags=tags,
        )
        annotated_forest = simple_forest.annotated_forest
        assert len(annotated_forest.nodes) == 5
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        for pre_yes_node in pre_assigned_yes_nodes:
            assert annotated_tags[pre_yes_node]["in_rate_base"] == "yes"
        for post_yes_node in [self.child1, self.parent]:
            assert annotated_tags[post_yes_node]["in_rate_base"] == "yes"


def test_annotated_forest_propagates_corrections():
    pass


def test_annotate_forest_propagates_both_dirs_with_corrections():
    pass
