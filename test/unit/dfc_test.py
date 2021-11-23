"""Unit tests for pudl.dfc module."""
import os
import tempfile
import uuid
from typing import List

import pandas as pd
import prefect
import pytest
from prefect import task

from pudl import dfc
from pudl.dfc import DataFrameCollection


class TestDataFrameCollection:
    """Unit tests for pudl.dfc.DataFrameCollection class."""

    @pytest.fixture
    def temp_dir(self):
        """Setup temporary directory."""
        temp_dir = tempfile.mkdtemp()
        prefect.context.data_frame_storage_path = temp_dir
        return temp_dir

    @pytest.fixture
    def dfc_fixture(self, temp_dir):
        """Create a DataFrameCollection fixture."""
        return DataFrameCollection(storage_path=temp_dir)

    def test_empty_dfc(self, dfc_fixture):
        """Tests that new DataFrameCollection is devoid of content."""
        dfc_fixture.get_table_names() == []
        list(dfc_fixture.references()) == []
        list(dfc_fixture.items()) == []

    def test_get_throws_key_error(self, dfc_fixture):
        """Getting frame that does not exist results in KeyError."""
        with pytest.raises(KeyError):
            dfc_fixture.get("unknown_df")

    def test_table_conflicts_are_prevented(self, dfc_fixture):
        """Checks that subsequent calls to store throw TableExistsError exceptions."""
        test_df = pd.DataFrame({'x': [1, 2]})
        dfc_fixture.store("test", test_df)
        with pytest.raises(dfc.TableExistsError):
            dfc_fixture.store("test", pd.DataFrame())

        pd.testing.assert_frame_equal(test_df, dfc_fixture.get("test"))

    def test_disk_overwrites_are_prevented(self, dfc_fixture, temp_dir):
        """Checks that TableExistsError is thrown if the file already exists on disk."""
        dfc_fixture.store("first", pd.DataFrame())
        instance_id = dict(dfc_fixture.references())["first"]
        with open(os.path.join(temp_dir, instance_id.hex, "second"), "w") as f:
            f.write("random content")

        with pytest.raises(dfc.TableExistsError):
            dfc_fixture.store("second", pd.DataFrame())

    def test_simple_df_retrieval(self, dfc_fixture):
        """Adds single dataframe to DFC and retrieves it."""
        df_in = pd.DataFrame({'x': [1, 2], 'y': [3, 4]})
        dfc_fixture.store('test_table', df_in)
        assert dfc_fixture.get_table_names() == ['test_table']

        df_out = dfc_fixture.get('test_table')
        pd.testing.assert_frame_equal(df_in, df_out)

        items = list(dfc_fixture.items())
        assert len(items) == 1

        table_name, df_out = items[0]
        assert table_name == "test_table"
        pd.testing.assert_frame_equal(df_in, df_out)

    def test_add_reference(self, dfc_fixture, temp_dir):
        """Tests that dataframes can be added to a collection by reference."""
        extra_dfc = DataFrameCollection(storage_path=temp_dir)

        test_df = pd.DataFrame({"x": [1, 2, 3]})
        extra_dfc.store("test", test_df)
        assert dfc_fixture.get_table_names() == []

        refs = dict(extra_dfc.references())
        assert list(refs) == ["test"]

        # Check that df added by reference points to the same underlying frame on disk.
        dfc_fixture.add_reference("test", refs["test"])
        dfc_fixture.get_table_names() == ["test"]
        pd.testing.assert_frame_equal(test_df, dfc_fixture.get("test"))
        extra_dfc.references() == dfc_fixture.references()

    def test_add_reference_prevents_collisions(self, dfc_fixture):
        """Tests that attempts to add_reference to existing table throw TableExistsError."""
        dfc_fixture.store("test", pd.DataFrame())
        with pytest.raises(dfc.TableExistsError):
            dfc_fixture.add_reference("test", uuid.uuid1())

    def test_two_dfc_do_not_collide(self, temp_dir):
        """Test that two dfc storing the same df will not collide."""
        dfc1 = DataFrameCollection(storage_path=temp_dir)
        dfc2 = DataFrameCollection(storage_path=temp_dir)

        dfc1.store("test", pd.DataFrame({"t1": [1]}))
        dfc2.store("test", pd.DataFrame({"t2": [1]}))

        t1_out = dfc1.get("test")
        t2_out = dfc2.get("test")

        assert t1_out.columns == ["t1"]
        assert t2_out.columns == ["t2"]

    def test_union_of_two(self, temp_dir):
        """Union of two DFCs contains DataFrames from both."""
        dfc1 = DataFrameCollection(storage_path=temp_dir)
        dfc2 = DataFrameCollection(storage_path=temp_dir)

        first = pd.DataFrame({'x': [1]})
        second = pd.DataFrame({'y': [2]})

        dfc1.store("first", first)
        dfc2.store("second", second)

        final_dfc = dfc1.union(dfc2)
        assert dfc1.get_table_names() == ["first"]
        assert dfc2.get_table_names() == ["second"]
        assert final_dfc.get_table_names() == ["first", "second"]

        pd.testing.assert_frame_equal(first, final_dfc.get("first"))
        pd.testing.assert_frame_equal(second, final_dfc.get("second"))

    def test_overlapping_union_disallowed(self, temp_dir):
        """Tests that merging two dataframes with the same table is not permitted."""
        dfc1 = DataFrameCollection(storage_path=temp_dir)
        dfc2 = DataFrameCollection(storage_path=temp_dir)

        dfc1.store("x", pd.DataFrame())
        dfc1.store("y", pd.DataFrame())
        dfc2.store("y", pd.DataFrame())
        dfc2.store("z", pd.DataFrame())

        with pytest.raises(dfc.TableExistsError):
            dfc1.union(dfc1)

    def test_to_dict(self, dfc_fixture):
        """Tests that dictionaries containing dataframes are correctly built from DFCs."""
        first = pd.DataFrame({'x': [1]})
        second = pd.DataFrame({'y': [2]})

        dfc_fixture.store("first", first)
        dfc_fixture.store("second", second)

        d = dfc_fixture.to_dict()
        assert sorted(d) == ["first", "second"]
        pd.testing.assert_frame_equal(first, d["first"])
        pd.testing.assert_frame_equal(second, d["second"])

    def test_from_dict(self, temp_dir):
        """Tests that DFCs can be built from dictionaries."""
        first = pd.DataFrame({'x': [1]})
        second = pd.DataFrame({'y': [2]})

        test_dfc = DataFrameCollection.from_dict(
            {"first": first, "second": second, "storage_path": temp_dir})
        assert test_dfc.get_table_names() == ["first", "second"]
        pd.testing.assert_frame_equal(first, test_dfc.get("first"))
        pd.testing.assert_frame_equal(second, test_dfc.get("second"))

    def test_from_dict_fails_with_invalid_types(self, temp_dir):
        """If something other than pd.DataFrame is passed DFC should not be created."""
        with pytest.raises(ValueError):
            DataFrameCollection.from_dict(
                {"first": pd.DataFrame(), "second": "just a string", "storage_path": temp_dir})

    def test_merge_task(self, temp_dir):
        """Test that dfc.merge prefect task works as expected."""
        @task
        def dfc_from_dict(df_name, d):
            return DataFrameCollection(**{df_name: pd.DataFrame(d), "storage_path": temp_dir})

        l_df_dict = {"x": [1]}
        r_df_dict = {"y": [2]}
        with prefect.Flow("test") as f:
            l_result = dfc_from_dict("left", l_df_dict)
            r_result = dfc_from_dict('right', r_df_dict)
            out_dfc_result = dfc.merge(l_result, r_result)

        final = f.run().result[out_dfc_result].result
        assert final.get_table_names() == ["left", "right"]
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
        assert final.get_table_names() == ["a", "b", "c", "d"]

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
        assert 6 == len(dfc1)
        assert [x.get_table_names() for x in dfc1] == [
            ["a"], ["b"], ["c"], ["d"], ["e"], ["f"]]

        dfc4 = status.result[chunk_4].result
        assert len(dfc4) == 2
        assert [x.get_table_names() for x in dfc4] == [["a", "b", "c", "d"], ["e", "f"]]

    def test_extract_table(self, temp_dir):
        """Test that extract_table task properly retrieves the relevant frames."""
        left = pd.DataFrame({'x': [1, 2, 3]})
        right = pd.DataFrame({'y': [4]})
        my_dfc = DataFrameCollection.from_dict(
            dict(left=left, right=right, storage_path=temp_dir))

        with prefect.Flow("test") as f:
            lf = dfc.extract_table(my_dfc, "left")
            rf = dfc.extract_table(my_dfc, "right")
            invalid = dfc.extract_table(my_dfc, "unknown")

        status = f.run()
        pd.testing.assert_frame_equal(left, status.result[lf].result)
        pd.testing.assert_frame_equal(right, status.result[rf].result)

        assert isinstance(status.result[invalid].result, KeyError)
