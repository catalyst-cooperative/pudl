"""Unit tests for pudl.dfc module."""
import os
import shutil
import tempfile
import unittest
import uuid
from typing import List

import pandas as pd
import prefect
from prefect import task

from pudl import dfc
from pudl.dfc import DataFrameCollection


class TestDataFrameCollection(unittest.TestCase):
    """Unit tests for pudl.dfc.DataFrameCollection class."""

    def setUp(self):
        """Creates temporary directory for dataframe storage."""
        self.temp_dir = tempfile.mkdtemp()
        self.dfc = DataFrameCollection(storage_path=self.temp_dir)
        prefect.context.data_frame_storage_path = self.temp_dir

    def tearDown(self):
        """Removes temporary directory."""
        shutil.rmtree(self.temp_dir)

    def test_empty_dfc(self):
        """Tests that new DataFrameCollection is devoid of content."""
        self.assertEqual([], self.dfc.get_table_names())
        self.assertEqual([], list(self.dfc.references()))
        self.assertEqual([], list(self.dfc.items()))

    def test_get_throws_key_error(self):
        """Getting frame that does not exist results in KeyError."""
        self.assertRaises(KeyError, self.dfc.get, "unknown_df")

    def test_table_conflicts_are_prevented(self):
        """Checks that subsequent calls to store throw TableExists exceptions."""
        test_df = pd.DataFrame({'x': [1, 2]})
        self.dfc.store("test", test_df)
        self.assertRaises(dfc.TableExists, self.dfc.store, "test", pd.DataFrame())
        pd.testing.assert_frame_equal(test_df, self.dfc.get("test"))

    def test_disk_overwrites_are_prevented(self):
        """Checks that TableExists is thrown if the file already exists on disk."""
        self.dfc.store("first", pd.DataFrame())
        instance_id = dict(self.dfc.references())["first"]
        with open(os.path.join(self.temp_dir, instance_id.hex, "second"), "w") as f:
            f.write("random content")
        self.assertRaises(dfc.TableExists, self.dfc.store, "second", pd.DataFrame())

    def test_simple_df_retrieval(self):
        """Adds single dataframe to DFC and retrieves it."""
        df_in = pd.DataFrame({'x': [1, 2], 'y': [3, 4]})
        self.dfc.store('test_table', df_in)
        self.assertEqual(['test_table'], self.dfc.get_table_names())

        df_out = self.dfc.get('test_table')
        pd.testing.assert_frame_equal(df_in, df_out)

        items = list(self.dfc.items())
        self.assertEqual(1, len(items))

        table_name, df_out = items[0]
        self.assertEqual("test_table", table_name)
        pd.testing.assert_frame_equal(df_in, df_out)

    def test_add_reference(self):
        """Tests that dataframes can be added to a collection by reference."""
        extra_dfc = DataFrameCollection(storage_path=self.temp_dir)

        test_df = pd.DataFrame({"x": [1, 2, 3]})
        extra_dfc.store("test", test_df)
        self.assertEqual([], self.dfc.get_table_names())

        refs = dict(extra_dfc.references())
        self.assertEqual(["test"], list(refs))

        # Check that df added by reference points to the same underlying frame on disk.
        self.dfc.add_reference("test", refs["test"])
        self.assertEqual(["test"], self.dfc.get_table_names())
        pd.testing.assert_frame_equal(test_df, self.dfc.get("test"))
        self.assertEqual(self.dfc.references(), extra_dfc.references())

    def test_add_reference_prevents_collisions(self):
        """Tests that attempts to add_reference to existing table throw TableExists."""
        self.dfc.store("test", pd.DataFrame())
        self.assertRaises(dfc.TableExists, self.dfc.add_reference, "test", uuid.uuid1())

    def test_two_dfc_do_not_collide(self):
        """Test that two dfc storing the same df will not collide."""
        dfc1 = DataFrameCollection(storage_path=self.temp_dir)
        dfc2 = DataFrameCollection(storage_path=self.temp_dir)

        dfc1.store("test", pd.DataFrame({"t1": [1]}))
        dfc2.store("test", pd.DataFrame({"t2": [1]}))

        t1_out = dfc1.get("test")
        t2_out = dfc2.get("test")
        self.assertEqual(["t1"], t1_out.columns)
        self.assertEqual(["t2"], t2_out.columns)

    def test_union_of_two(self):
        """Union of two DFCs contains DataFrames from both."""
        dfc1 = DataFrameCollection(storage_path=self.temp_dir)
        dfc2 = DataFrameCollection(storage_path=self.temp_dir)

        first = pd.DataFrame({'x': [1]})
        second = pd.DataFrame({'y': [2]})

        dfc1.store("first", first)
        dfc2.store("second", second)

        final_dfc = dfc1.union(dfc2)
        self.assertEqual(["first"], dfc1.get_table_names())
        self.assertEqual(["second"], dfc2.get_table_names())
        self.assertEqual(["first", "second"], final_dfc.get_table_names())
        pd.testing.assert_frame_equal(first, final_dfc.get("first"))
        pd.testing.assert_frame_equal(second, final_dfc.get("second"))

    def test_overlapping_union_disallowed(self):
        """Tests that merging two dataframes with the same table is not permitted."""
        dfc1 = DataFrameCollection(storage_path=self.temp_dir)
        dfc2 = DataFrameCollection(storage_path=self.temp_dir)

        dfc1.store("x", pd.DataFrame())
        dfc1.store("y", pd.DataFrame())
        dfc2.store("y", pd.DataFrame())
        dfc2.store("z", pd.DataFrame())
        self.assertRaises(dfc.TableExists, dfc1.union, dfc2)

    def test_to_dict(self):
        """Tests that dictionaries containing dataframes are correctly built from DFCs."""
        first = pd.DataFrame({'x': [1]})
        second = pd.DataFrame({'y': [2]})

        self.dfc.store("first", first)
        self.dfc.store("second", second)

        d = self.dfc.to_dict()
        self.assertEqual(["first", "second"], sorted(d))
        pd.testing.assert_frame_equal(first, d["first"])
        pd.testing.assert_frame_equal(second, d["second"])

    def test_from_dict(self):
        """Tests that DFCs can be built from dictionaries."""
        first = pd.DataFrame({'x': [1]})
        second = pd.DataFrame({'y': [2]})

        test_dfc = DataFrameCollection.from_dict(
            {"first": first, "second": second})
        self.assertEqual(["first", "second"], test_dfc.get_table_names())
        pd.testing.assert_frame_equal(first, test_dfc.get("first"))
        pd.testing.assert_frame_equal(second, test_dfc.get("second"))

    def test_from_dict_fails_with_invalid_types(self):
        """If something other than pd.DataFrame is passed DFC should not be created."""
        self.assertRaises(
            ValueError,
            DataFrameCollection.from_dict,
            {"first": pd.DataFrame(), "second": "just a string"})

    def test_merge_task(self):
        """Test that dfc.merge prefect task works as expected."""
        @task
        def dfc_from_dict(df_name, d):
            return DataFrameCollection(**{df_name: pd.DataFrame(d)})

        l_df_dict = {"x": [1]}
        r_df_dict = {"y": [2]}
        with prefect.Flow("test") as f:
            l_result = dfc_from_dict("left", l_df_dict)
            r_result = dfc_from_dict('right', r_df_dict)
            out_dfc_result = dfc.merge(l_result, r_result)

        final = f.run().result[out_dfc_result].result
        self.assertEqual(["left", "right"], final.get_table_names())
        pd.testing.assert_frame_equal(pd.DataFrame(l_df_dict), final.get("left"))
        pd.testing.assert_frame_equal(pd.DataFrame(r_df_dict), final.get("right"))

    def test_merge_list_task(self):
        """Test that dfc.merge_list task works as expected."""
        @task
        def named_empty_df(df_name):
            return DataFrameCollection(**{df_name: pd.DataFrame()})

        with prefect.Flow("test") as f:
            names = ["a", "b", "c", "d"]
            res = dfc.merge_list(named_empty_df.map(names))
        final = f.run().result[res].result
        self.assertEqual(["a", "b", "c", "d"], final.get_table_names())

    def test_fanout_task(self):
        """Test that dfc.fanout will split dataframes into smaller pieces."""
        @task
        def make_big_dfc(table_names: List[str]):
            dfc = DataFrameCollection()
            for tn in table_names:
                dfc.store(tn, pd.DataFrame())
            return dfc

        with prefect.Flow("test") as f:
            big_one = make_big_dfc(["a", "b", "c", "d", "e", "f"])
            chunk_1 = dfc.fanout(big_one, chunk_size=1)
            chunk_4 = dfc.fanout(big_one, chunk_size=4)
        status = f.run()

        dfc1 = status.result[chunk_1].result
        self.assertEqual(6, len(dfc1))
        self.assertEqual(
            [["a"], ["b"], ["c"], ["d"], ["e"], ["f"]],
            [x.get_table_names() for x in dfc1])

        dfc4 = status.result[chunk_4].result
        self.assertEqual(2, len(dfc4))
        self.assertEqual(
            [["a", "b", "c", "d"], ["e", "f"]],
            [x.get_table_names() for x in dfc4])

    def test_filter_by_name(self):
        """Test that filter_by_name task properly splits DataFrameCollections."""
        my_dfc = DataFrameCollection.from_dict({
            'a_positive': pd.DataFrame(),
            'b_positive': pd.DataFrame(),
            'a_negative': pd.DataFrame(),
            'b_negative': pd.DataFrame(),
        })
        with prefect.Flow("test") as f:
            a_frames = dfc.filter_by_name(my_dfc, lambda name: name.startswith("a_"))
            positive_frames = dfc.filter_by_name(
                my_dfc, lambda name: 'positive' in name)
        status = f.run()
        self.assertEqual(
            ['a_negative', 'a_positive'],
            status.result[a_frames].result.get_table_names())
        self.assertEqual(
            ['a_positive', 'b_positive'],
            status.result[positive_frames].result.get_table_names())
