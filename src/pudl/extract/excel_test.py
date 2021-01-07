"""Unit tests for pudl.extract.excel module."""
import unittest
from unittest import mock as mock
from unittest.mock import patch

import pandas as pd

from pudl.extract import excel as excel
from pudl.workspace import datastore


class TestMetadata(unittest.TestCase):
    """Tests basic operation of the excel.Metadata object."""

    def setUp(self):
        """Cosntructs test metadata instance for testing."""
        self._metadata = excel.Metadata('test')

    def test_basics(self):
        """Test that basic API method return expected results."""
        self.assertEqual('test', self._metadata.get_dataset_name())
        self.assertListEqual(
            ['books', 'boxes', 'shoes'],
            self._metadata.get_all_pages())
        self.assertListEqual(
            ['author', 'pages', 'title'],
            self._metadata.get_all_columns('books'))
        self.assertDictEqual(
            {'book_title': 'title', 'name': 'author', 'pages': 'pages'},
            self._metadata.get_column_map(2010, 'books'))
        self.assertEqual(10, self._metadata.get_skiprows(2011, 'boxes'))
        self.assertEqual(1, self._metadata.get_sheet_name(2011, 'boxes'))


class FakeExtractor(excel.GenericExtractor):
    """Test friendly fake extractor returns strings instead of files."""

    def __init__(self, *args, **kwargs):
        """It's a Fake extractor.  Good thing flake demanded this."""
        ds = datastore.Datastore(sandbox=True)
        self.METADATA = excel.Metadata('test', ds)
        self.BLACKLISTED_PAGES = ['shoes']
        super().__init__()

    def _load_excel_file(self, year, page):
        return f'{page}-{year}'


def _fake_data_frames(page_name, **kwargs):
    """Returns panda.DataFrames.

    This is suitable mock for pd.read_excel method when used together with FakeExtractor.
    """
    fake_data = {
        'books-2010': pd.DataFrame.from_dict(
            {'book_title': ['Tao Te Ching'], 'name': ['Laozi'], 'pages': [0]}),
        'books-2011': pd.DataFrame.from_dict(
            {'title_of_book': ['The Tao of Pooh'], 'author': ['Benjamin Hoff'], 'pages': [158]}),
        'boxes-2010': pd.DataFrame.from_dict(
            {'composition': ['cardboard'], 'size_inches': [10]}),
        'boxes-2011': pd.DataFrame.from_dict(
            {'composition': ['metal'], 'size_cm': [99]}),
    }
    return fake_data[page_name]


class TestGenericExtractor(unittest.TestCase):
    """Test operation of the excel.GenericExtractor class."""

    @staticmethod
    @patch('pudl.extract.excel.pd.read_excel')
    def test_read_excel_calls(mock_read_excel):
        """Verifies that read_excel method is called with expected arguments."""
        mock_read_excel.return_value = pd.DataFrame()

        FakeExtractor('/blah').extract([2010, 2011])
        expected_calls = [
            mock.call('books-2010', sheet_name=0, skiprows=0, dtype={}),
            mock.call('books-2011', sheet_name=0, skiprows=1, dtype={}),
            mock.call('boxes-2010', sheet_name=1, skiprows=0, dtype={}),
            mock.call('boxes-2011', sheet_name=1, skiprows=10, dtype={})
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
