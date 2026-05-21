"""Tests for the FERC Form 1 output functions.

Stuff we could test:
- construction of basic tree from input metadata
- do nodes not part of any calculation get orphaned?
- do nodes not in the seeded digraph get pruned?
- does it fail when we have a cycle in the graph?
- does it identify stepparent nodes correctly?
- does it identify stepchild nodes correctly?
- pruning of passthrough nodes & associated corrections
- propagation of weights
- conflicting weights
- conflicting tags
- validation of calculations using only leaf-nodes to reproduce root node values

Stuff we are testing:
- propagation of tags

"""

import logging
from io import StringIO

import networkx as nx
import pandas as pd
import pytest

from pudl.helpers import dedupe_n_flatten_list_of_lists
from pudl.output.ferc1 import (
    NodeId,
    XbrlCalculationForestFerc1,
    disaggregate_null_or_total_tag,
    get_core_ferc1_asset_description,
)

logger = logging.getLogger(__name__)


class TestForestSetup:
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

    def build_forest(
        self, edges: list[tuple[NodeId, NodeId]], tags: pd.DataFrame, seeds=None
    ):
        if not seeds:
            seeds = [self.parent]
        forest = XbrlCalculationForestFerc1(
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=seeds,
            tags=tags,
        )
        return forest

    def build_forest_and_annotated_tags(
        self, edges: list[tuple[NodeId, NodeId]], tags: pd.DataFrame, seeds=None
    ):
        """Build a forest, test forest nodes and return annotated tags.

        Args:
            edges: list of tuples
            tags: dataframe of tags
            seeds: list of seed nodes. Default is None and will assume seed node is
                ``parent``.
        """
        simple_forest = self.build_forest(edges, tags, seeds)
        annotated_forest = simple_forest.annotated_forest
        # ensure no nodes got dropped
        assert len(annotated_forest.nodes) == len(dedupe_n_flatten_list_of_lists(edges))
        annotated_tags = nx.get_node_attributes(annotated_forest, "tags")
        return annotated_tags


class TestPrunedNode(TestForestSetup):
    def setup_method(self):
        self.root = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_1",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.root_child = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_11",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.root_other = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_2",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )
        self.root_other_child = NodeId(
            table_name="table_1",
            xbrl_factoid="reported_21",
            utility_type="electric",
            plant_status=pd.NA,
            plant_function=pd.NA,
        )

    def test_pruned_nodes(self):
        edges = [(self.root, self.root_child), (self.root_other, self.root_other_child)]
        tags = pd.DataFrame(columns=list(NodeId._fields)).convert_dtypes()
        forest = XbrlCalculationForestFerc1(
            exploded_calcs=self._exploded_calcs_from_edges(edges),
            seeds=[self.root],
            tags=tags,
        )
        pruned = forest.pruned
        assert set(pruned) == {self.root_other, self.root_other_child}


class TestTagPropagation(TestForestSetup):
    def setup_method(self):
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

    def test_leafward_prop_undecided_children(self):
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=["yes", pd.NA, pd.NA]
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        assert annotated_tags[self.parent]["in_rate_base"] == "yes"
        for child_node in [self.child1, self.child2]:
            assert (
                annotated_tags[child_node]["in_rate_base"]
                == annotated_tags[self.parent]["in_rate_base"]
            )

    def test_leafward_prop_disagreeing_child(self):
        """Don't force the diagreeing child to follow the parent."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=["yes", "no", pd.NA]
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        assert annotated_tags[self.parent]["in_rate_base"] == "yes"
        assert annotated_tags[self.child1]["in_rate_base"] == "no"
        assert (
            annotated_tags[self.child2]["in_rate_base"]
            == annotated_tags[self.parent]["in_rate_base"]
        )

    def test_leafward_prop_preserve_non_propagating_tags(self):
        """Only propagate tags that actually get inherited - i.e., not `in_root_boose`."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=["yes", "no", pd.NA],
            in_root_boose=["yus", "nu", pd.NA],
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        assert annotated_tags[self.parent]["in_rate_base"] == "yes"
        assert annotated_tags[self.child1]["in_rate_base"] == "no"
        assert (
            annotated_tags[self.child2]["in_rate_base"]
            == annotated_tags[self.parent]["in_rate_base"]
        )
        assert annotated_tags[self.parent]["in_root_boose"] == "yus"
        assert annotated_tags[self.child1]["in_root_boose"] == "nu"
        assert not annotated_tags[self.child2].get("in_root_boose")

    def test_rootward_prop_disagreeing_children(self):
        """Parents should not pick sides between disagreeing children."""
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        tags = pd.DataFrame([self.child1, self.child2]).assign(
            in_rate_base=["no", "yes"]
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        assert not annotated_tags.get(self.parent)
        assert annotated_tags[self.child1]["in_rate_base"] == "no"
        assert annotated_tags[self.child2]["in_rate_base"] == "yes"

    def test_prop_no_tags(self):
        """If no tags, don't propagate anything.

        This also tests whether a fully null tag input behaves the same as an
        empty df. It also checks whether we get the expected behavior when
        the propogated tags are all null but there is another non-propagating
        tag.
        """
        edges = [(self.parent, self.child1), (self.parent, self.child2)]
        null_tag_edges = [self.parent, self.child1, self.child2]
        tags = pd.DataFrame(null_tag_edges).assign(in_rate_base=[pd.NA, pd.NA, pd.NA])
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        for node in null_tag_edges:
            assert not annotated_tags.get(node)

        tags = pd.DataFrame(columns=NodeId._fields).convert_dtypes()
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        for node in null_tag_edges:
            assert not annotated_tags.get(node)

        tags = pd.DataFrame([self.parent, self.child1, self.child2]).assign(
            in_rate_base=[pd.NA, pd.NA, pd.NA],
            a_non_propped_tag=["hi", "hello", "what_am_i_doing_here_even"],
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        for node in null_tag_edges:
            assert "in_rate_base" not in annotated_tags[node]
            # do we still have a non-null value for the non-propped tag
            assert annotated_tags[node].get("a_non_propped_tag")

    def test_annotated_forest_propagates_rootward(self):
        """If two grandchildren have the same tag, their parent does inhert the tag.

        But, the rootward propagation only happens when all of a nodes children have
        the same tag.
        """
        edges = [
            (self.parent, self.child1),
            (self.parent, self.child2),
            (self.child1, self.grand_child11),
            (self.child1, self.grand_child12),
        ]
        tags = pd.DataFrame([self.grand_child11, self.grand_child12]).assign(
            in_rate_base=["yes", "yes"]
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        assert self.parent not in annotated_tags
        assert annotated_tags[self.child1]["in_rate_base"] == "yes"
        assert self.child2 not in annotated_tags
        assert annotated_tags[self.grand_child11]["in_rate_base"] == "yes"
        assert annotated_tags[self.grand_child12]["in_rate_base"] == "yes"

    def test_annotated_forest_propagates_rootward_disagreeing_sibling(self):
        """If two siblings disagree, their parent does not inherit either of their tag values."""
        edges = [
            (self.parent, self.child1),
            (self.parent, self.child2),
            (self.child1, self.grand_child11),
            (self.child1, self.grand_child12),
        ]
        tags = pd.DataFrame([self.grand_child11, self.grand_child12]).assign(
            in_rate_base=["yes", "no"]
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        assert not annotated_tags.get(self.parent)
        assert not annotated_tags.get(self.child1)
        assert not annotated_tags.get(self.child2)
        assert annotated_tags[self.grand_child11]["in_rate_base"] == "yes"
        assert annotated_tags[self.grand_child12]["in_rate_base"] == "no"

    def test_annotated_forest_propagates_rootward_correction(self):
        edges = [
            (self.child1, self.grand_child11),
            (self.child1, self.child1_correction),
        ]
        tags = pd.DataFrame([self.child1]).assign(in_rate_base=["yes"])
        annotated_tags = self.build_forest_and_annotated_tags(
            edges, tags, seeds=[self.child1]
        )
        assert annotated_tags[self.child1]["in_rate_base"] == "yes"
        assert annotated_tags[self.grand_child11]["in_rate_base"] == "yes"
        assert (
            annotated_tags[self.child1_correction]["in_rate_base"]
            == annotated_tags[self.child1]["in_rate_base"]
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
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        for pre_yes_node in pre_assigned_yes_nodes:
            assert annotated_tags[pre_yes_node]["in_rate_base"] == "yes"
        for post_yes_node in [self.child1, self.parent]:
            assert annotated_tags[post_yes_node]["in_rate_base"] == "yes"

    def test_annotated_forest_propagates_rootward_two_layers_plus_corrections(self):
        edges = [
            (self.parent, self.child1),
            (self.parent, self.child2),
            (self.parent, self.parent_correction),
            (self.child1, self.grand_child11),
            (self.child1, self.grand_child12),
            (self.child1, self.child1_correction),
        ]
        pre_assigned_yes_nodes = [self.child2, self.grand_child11, self.grand_child12]
        tags = pd.DataFrame(pre_assigned_yes_nodes).assign(
            in_rate_base=["yes"] * len(pre_assigned_yes_nodes),
        )
        annotated_tags = self.build_forest_and_annotated_tags(edges, tags)
        for pre_yes_node in pre_assigned_yes_nodes:
            assert annotated_tags[pre_yes_node]["in_rate_base"] == "yes"
        for post_yes_node in [
            self.child1,
            self.parent,
            self.child1_correction,
            self.parent_correction,
        ]:
            assert annotated_tags[post_yes_node]["in_rate_base"] == "yes"


def test_get_core_ferc1_asset_description():
    valid_core_ferc1_asset_name = "core_ferc1__yearly_income_statements_sched114"
    valid_core_ferc1_asset_name_result = get_core_ferc1_asset_description(
        valid_core_ferc1_asset_name
    )
    assert valid_core_ferc1_asset_name_result == "income_statements"

    invalid_core_ferc1_asset_name = "core_ferc1__income_statements"
    with pytest.raises(ValueError):
        get_core_ferc1_asset_description(invalid_core_ferc1_asset_name)


def test_disaggregate_null_or_total_tag_standard():
    """Test basic functionality of :func:`disaggregate_null_or_total_tag`."""
    coolest = 10
    cooler = 25
    total = 70
    df = pd.read_csv(
        StringIO(
            f"""
report_year,utility_id_ferc1,xbrl_factoid,cool_tag_col,ending_balance
2010,13,cool_factor,coolest,{coolest}
2010,13,cool_factor,cooler,{cooler}
2010,13,pal_scale,total,{total}
"""
        ),
    )
    out = disaggregate_null_or_total_tag(df, "cool_tag_col")

    cool_factor_total = coolest + cooler
    df_expected = pd.read_csv(
        StringIO(
            f"""
report_year,utility_id_ferc1,xbrl_factoid,cool_tag_col,ending_balance,is_disaggregated_cool_tag_col
2010,13,cool_factor,coolest,{coolest},False
2010,13,cool_factor,cooler,{cooler},False
2010,13,pal_scale,cooler,{total * (cooler / cool_factor_total)},True
2010,13,pal_scale,coolest,{total * (coolest / cool_factor_total)},True
"""
        ),
    ).convert_dtypes()
    pd.testing.assert_frame_equal(df_expected, out, check_like=True)


def test_disaggregate_null_or_total_tag_with_no_non_null_tags():
    """What happens when there are no non-null or non-total tag values?

    The output should have the same ``ending_balance`` value but the
    value for the tag column will be null.
    """
    total = 1000
    only_total = pd.read_csv(
        StringIO(
            f"""
report_year,utility_id_ferc1,xbrl_factoid,cool_tag_col,ending_balance
2010,13,pal_scale,total,{total}
"""
        ),
    )
    out_only_total = disaggregate_null_or_total_tag(only_total, "cool_tag_col")

    expected_only_total = pd.read_csv(
        StringIO(
            f"""
report_year,utility_id_ferc1,xbrl_factoid,cool_tag_col,ending_balance,is_disaggregated_cool_tag_col
2010,13,pal_scale,,{total},True
"""
        ),
    ).convert_dtypes()
    pd.testing.assert_frame_equal(expected_only_total, out_only_total, check_like=True)
