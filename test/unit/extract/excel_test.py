"""Unit tests for pudl.extract.excel module."""
import unittest
from unittest import mock as mock
from unittest.mock import patch

import pandas as pd
import pytest
from dagster import build_op_context

from pudl.extract import excel as excel
from pudl.settings import DatasetsSettings


class TestMetadata(unittest.TestCase):
    """Tests basic operation of the excel.Metadata object."""

    def setUp(self):
        """Cosntructs test metadata instance for testing."""
        self._metadata = excel.Metadata("test")

    def test_basics(self):
        """Test that basic API method return expected results."""
        self.assertEqual("test", self._metadata.get_dataset_name())
        self.assertListEqual(
            ["books", "boxes", "shoes"], self._metadata.get_all_pages()
        )
        self.assertListEqual(
            ["author", "pages", "title"], self._metadata.get_all_columns("books")
        )
        self.assertDictEqual(
            {"book_title": "title", "name": "author", "pages": "pages"},
            self._metadata.get_column_map("books", year=2010),
        )
        self.assertEqual(10, self._metadata.get_skiprows("boxes", year=2011))
        self.assertEqual(1, self._metadata.get_sheet_name("boxes", year=2011))


class FakeExtractor(excel.GenericExtractor):
    """Test friendly fake extractor returns strings instead of files."""

    def __init__(self, *args, **kwargs):
        """It's a Fake extractor.

        Good thing flake demanded this.
        """
        self.METADATA = excel.Metadata("test")
        self.BLACKLISTED_PAGES = ["shoes"]
        super().__init__(ds=None)

    def load_excel_file(self, page, **partition):
        """Returns fake file contents for given page and partition."""
        return f'{page}-{partition["year"]}'


def _fake_data_frames(page_name, **kwargs):
    """Returns panda.DataFrames.

    This is suitable mock for pd.read_excel method when used together with
    FakeExtractor.
    """
    fake_data = {
        "books-2010": pd.DataFrame.from_dict(
            {"book_title": ["Tao Te Ching"], "name": ["Laozi"], "pages": [0]}
        ),
        "books-2011": pd.DataFrame.from_dict(
            {
                "title_of_book": ["The Tao of Pooh"],
                "author": ["Benjamin Hoff"],
                "pages": [158],
            }
        ),
        "boxes-2010": pd.DataFrame.from_dict(
            {"composition": ["cardboard"], "size_inches": [10]}
        ),
        "boxes-2011": pd.DataFrame.from_dict(
            {"composition": ["metal"], "size_cm": [99]}
        ),
    }
    return fake_data[page_name]


class TestGenericExtractor(unittest.TestCase):
    """Test operation of the excel.GenericExtractor class."""

    @staticmethod
    @patch("pudl.extract.excel.pd.read_excel")
    def test_read_excel_calls(mock_read_excel):
        """Verifies that read_excel method is called with expected arguments."""
        mock_read_excel.return_value = pd.DataFrame()

        FakeExtractor("/blah").extract(year=[2010, 2011])
        expected_calls = [
            mock.call("books-2010", sheet_name=0, skiprows=0, skipfooter=0, dtype={}),
            mock.call("books-2011", sheet_name=0, skiprows=1, skipfooter=1, dtype={}),
            mock.call("boxes-2010", sheet_name=1, skiprows=0, skipfooter=0, dtype={}),
            mock.call("boxes-2011", sheet_name=1, skiprows=10, skipfooter=10, dtype={}),
        ]
        mock_read_excel.assert_has_calls(expected_calls, any_order=True)

    # @patch('pudl.extract.excel.pd.read_excel', _fake_data_frames)
    # def test_resulting_dataframes(self):
    #     """Checks that pages across years are merged and columns are translated."""
    #     dfs = FakeExtractor().extract([2010, 2011], testing=True)
    #     self.assertEqual(set(['books', 'boxes']), set(dfs.keys()))
    #     pd.testing.assert_frame_equal(
    #         pd.DataFrame(data={
    #             'author': ['Laozi', 'Benjamin Hoff'],
    #             'pages': [0, 158],
    #             'title': ['Tao Te Ching', 'The Tao of Pooh'],
    #         }),
    #         dfs['books'])
    #     pd.testing.assert_frame_equal(
    #         pd.DataFrame(data={
    #             'material': ['cardboard', 'metal'],
    #             'size': [10, 99],
    #         }),
    #         dfs['boxes'])

    # TODO(rousik@gmail.com): need to figure out how to test process_$x methods.
    # TODO(rousik@gmail.com): we should test that empty columns are properly added.


@pytest.mark.parametrize(
    "dataset, expected_years",
    (
        ("eia860", set(range(2001, 2022))),
        ("eia861", set(range(2001, 2022))),
        ("eia923", set(range(2001, 2022))),
    ),
)
def test_years_from_settings(dataset, expected_years):
    years_from_settings = excel.years_from_settings_factory(dataset)

    with build_op_context(
        resources={"dataset_settings": DatasetsSettings()}
    ) as context:
        # Assert actual years are a superset of expected. Instead of doing
        # an equality check, this avoids having to update expected years
        # every time a new year is added to the datasets
        assert {
            output.value for output in years_from_settings(context)
        } >= expected_years


def test_merge_yearly_dfs():
    pages = ["page1", "page2", "page3"]
    dfs_1 = {page: pd.DataFrame({"df": [1], "page": [page]}) for page in pages}
    dfs_2 = {page: pd.DataFrame({"df": [2], "page": [page]}) for page in pages}

    merged_dfs = excel.merge_yearly_dfs([dfs_1, dfs_2])
    assert list(merged_dfs.keys()) == pages
    for page in pages:
        pd.testing.assert_frame_equal(
            merged_dfs[page],
            pd.DataFrame({"df": [1, 2], "page": [page, page]}, index=[0, 0]),
        )
