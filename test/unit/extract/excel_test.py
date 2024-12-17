"""Unit tests for pudl.extract.excel module."""

import pandas as pd
import pytest

from pudl.extract import excel


@pytest.fixture
def metadata():
    """Constructs test metadata instance for testing."""
    return excel.ExcelMetadata("test")


@pytest.fixture
def extractor():
    """Constructs test extractor instance for testing."""
    return FakeExtractor()


class TestMetadata:
    """Tests basic operation of the excel.Metadata object."""

    def test_basics(self, metadata):
        """Test that basic API method return expected results."""
        assert metadata.get_dataset_name() == "test"
        assert metadata.get_all_pages() == ["books", "boxes", "shoes"]
        assert metadata.get_all_columns("books") == ["author", "pages", "title"]
        assert metadata.get_column_map("books", year=2010) == {
            "book_title": "title",
            "name": "author",
            "pages": "pages",
        }
        assert metadata.get_skiprows("boxes", year=2011) == 10
        assert metadata.get_sheet_name("boxes", year=2011) == 1

    def test_metadata_methods(self, metadata):
        """Test various metadata methods."""
        assert metadata.get_all_columns("books") == ["author", "pages", "title"]
        assert metadata.get_column_map("books", year=2010) == {
            "book_title": "title",
            "name": "author",
            "pages": "pages",
        }
        assert metadata.get_skiprows("boxes", year=2011) == 10
        assert metadata.get_sheet_name("boxes", year=2011) == 1


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


class TestExtractor:
    """Test operation of the excel.Extractor class."""

    def test_extract(self, extractor):
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
