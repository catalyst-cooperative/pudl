"""Unit tests for pudl.extract.excel module."""

import unittest
from unittest import mock as mock

import pandas as pd

from pudl.extract import excel


class TestMetadata(unittest.TestCase):
    """Tests basic operation of the excel.Metadata object."""

    def setUp(self):
        """Cosntructs test metadata instance for testing."""
        self._metadata = excel.ExcelMetadata("test")

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


class FakeExtractor(excel.ExcelExtractor):
    """Test friendly fake extractor returns strings instead of files."""

    def __init__(self, *args, **kwargs):
        """It's a Fake extractor.

        Good thing flake demanded this.
        """
        self.METADATA = excel.ExcelMetadata("test")
        self.BLACKLISTED_PAGES = ["shoes"]
        super().__init__(ds=None)

    def load_source(self, page, **partition):
        """Returns fake file contents for given page and partition."""
        page_name = f'{page}-{partition["year"]}'
        return _fake_data_frames(page_name)


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


class TestExtractor(unittest.TestCase):
    """Test operation of the excel.Extractor class."""

    @staticmethod
    def test_extract():
        extractor = FakeExtractor()
        res = extractor.extract(year=[2010, 2011])
        expected_books = {
            "author": {0: "Laozi", 1: "Benjamin Hoff"},
            "data_maturity": {0: "final", 1: "final"},
            "pages": {0: 0, 1: 158},
            "title": {0: "Tao Te Ching", 1: "The Tao of Pooh"},
        }
        assert expected_books == res["books"].to_dict()

        expected_boxes = {
            "data_maturity": {0: "final", 1: "final"},
            "material": {0: "cardboard", 1: "metal"},
            "size": {0: 10, 1: 99},
        }
        assert expected_boxes == res["boxes"].to_dict()

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
