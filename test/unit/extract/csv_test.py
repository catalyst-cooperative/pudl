"""Unit tests for pudl.extract.csv module."""

from unittest.mock import MagicMock, patch

import pandas as pd
from pytest import raises

from pudl.extract.csv import CsvExtractor
from pudl.extract.extractor import GenericMetadata

DATASET = "eia176"
PAGE = "data"
PARTITION_SELECTION = 2023
PARTITION = {"year": PARTITION_SELECTION}
CSV_FILENAME = f"{DATASET}_{PARTITION_SELECTION}.csv"


class FakeExtractor(CsvExtractor):
    def __init__(self):
        # TODO: Make these tests independent of the eia176 implementation
        self.METADATA = GenericMetadata("eia176")
        super().__init__(ds=MagicMock())


def test_source_filename_valid_partition():
    extractor = FakeExtractor()
    assert extractor.source_filename(PAGE, **PARTITION) == CSV_FILENAME


def test_source_filename_multipart_partition():
    extractor = FakeExtractor()
    multipart_partition = PARTITION.copy()
    multipart_partition["month"] = 12
    with raises(AssertionError):
        extractor.source_filename(PAGE, **multipart_partition)


def test_source_filename_multiple_selections():
    extractor = FakeExtractor()
    multiple_selections = {"year": [PARTITION_SELECTION, 2024]}
    with raises(AssertionError):
        extractor.source_filename(PAGE, **multiple_selections)


@patch("pudl.extract.csv.pd")
def test_load_source(mock_pd):
    extractor = FakeExtractor()

    assert extractor.load_source(PAGE, **PARTITION) == mock_pd.read_csv.return_value
    extractor.ds.get_zipfile_resource.assert_called_once_with(DATASET, **PARTITION)
    zipfile = extractor.ds.get_zipfile_resource.return_value.__enter__.return_value
    zipfile.open.assert_called_once_with(CSV_FILENAME)
    file = zipfile.open.return_value.__enter__.return_value
    mock_pd.read_csv.assert_called_once_with(file)


def test_extract():
    extractor = FakeExtractor()
    # Create a sample of data we could expect from an EIA CSV
    company_field = "company"
    company_data = "Total of All Companies"
    df = pd.DataFrame([company_data])
    df.columns = [company_field]

    # TODO: Once FakeExtractor is independent of eia176, mock out populating _column_map for PARTITION_SELECTION;
    #  Also include negative tests, i.e., for partition selections not in the _column_map
    with (
        patch.object(CsvExtractor, "load_source", return_value=df),
        patch.object(
            # Testing the rename
            GenericMetadata,
            "get_column_map",
            return_value={company_field: "company_rename"},
        ),
        patch.object(
            # Transposing the df here to get the orientation we expect get_page_cols to return
            CsvExtractor,
            "get_page_cols",
            return_value=df.T.index,
        ),
    ):
        res = extractor.extract(**PARTITION)
    assert len(res) == 1  # Assert only one page extracted
    assert list(res.keys()) == [PAGE]  # Assert it is named correctly
    assert (
        res[PAGE]["company_rename"][0] == company_data
    )  # Assert that column correctly renamed and data is there.
