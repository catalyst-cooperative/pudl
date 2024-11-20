"""Pytest for pudl.extract.csv module."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

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


@pytest.fixture
def extractor():
    # Create an instance of the CsvExtractor class
    return FakeExtractor()


def test_source_filename_valid_partition(extractor):
    assert extractor.source_filename(PAGE, **PARTITION) == CSV_FILENAME


def test_source_filename_multipart_partition(extractor):
    multipart_partition = PARTITION.copy()
    multipart_partition["month"] = 12
    with pytest.raises(AssertionError):
        extractor.source_filename(PAGE, **multipart_partition)


def test_source_filename_multiple_selections(extractor):
    multiple_selections = {"year": [PARTITION_SELECTION, 2024]}
    with pytest.raises(AssertionError):
        extractor.source_filename(PAGE, **multiple_selections)


def test_load_source(mocker, extractor):
    mock_pd = mocker.patch("pudl.extract.csv.pd")
    mock_ds = mocker.patch.object(extractor, "ds")
    mock_zipfile = mocker.MagicMock()
    mock_file = mocker.MagicMock()

    mock_ds.get_zipfile_resource.return_value.__enter__.return_value = mock_zipfile
    mock_zipfile.open.return_value.__enter__.return_value = mock_file

    assert extractor.load_source(PAGE, **PARTITION) == mock_pd.read_csv.return_value
    mock_ds.get_zipfile_resource.assert_called_once_with(DATASET, **PARTITION)
    mock_zipfile.open.assert_called_once_with(CSV_FILENAME)
    mock_pd.read_csv.assert_called_once_with(mock_file)


def test_extract(mocker, extractor):
    # Create a sample of data we could expect from an EIA CSV
    company_field = "company"
    company_data = "Total of All Companies"
    df = pd.DataFrame([company_data])
    df.columns = [company_field]

    # TODO: Once FakeExtractor is independent of eia176, mock out populating _column_map for PARTITION_SELECTION;
    #  Also include negative tests, i.e., for partition selections not in the _column_map
    with (
        mocker.patch.object(CsvExtractor, "load_source", return_value=df),
        mocker.patch.object(
            GenericMetadata,
            "get_column_map",
            return_value={"company_rename": company_field},
        ),
        mocker.patch.object(CsvExtractor, "get_page_cols", return_value=df.T.index),
    ):
        res = extractor.extract(**PARTITION)
        assert len(res) == 1  # Assert only one page extracted
        assert list(res.keys()) == [PAGE]  # Assert it is named correctly
        assert (
            res[PAGE][company_field][0] == company_data
        )  # Assert the data is extracted correctly


def test_validate_exact_columns(mocker, extractor):
    # Mock the partition selection and page columns
    extractor.get_page_cols = mocker.MagicMock(return_value={"col1", "col2"})

    # Create a DataFrame with the exact expected columns
    df = pd.DataFrame(columns=["col1", "col2"])

    # Call the validate method. No exceptions should be raised.
    extractor.validate(df, "page1", partition="partition1")


def test_validate_extra_columns(mocker, extractor):
    # Mock the partition selection and page columns
    extractor.get_page_cols = mocker.MagicMock(return_value={"col1", "col2"})

    # Create a DataFrame with extra columns
    df = pd.DataFrame(columns=["col1", "col2", "col3"])

    # Call the validate method and check for ValueError
    with pytest.raises(ValueError, match="Extra columns found in extracted table"):
        extractor.validate(df, "page1", partition="partition1")


def test_validate_missing_columns(mocker, extractor):
    # Mock the partition selection and page columns
    extractor.get_page_cols = mocker.MagicMock(return_value={"col1", "col2"})

    # Create a DataFrame with missing columns
    df = pd.DataFrame(columns=["col1"])

    # Call the validate method and check for ValueError
    with pytest.raises(
        ValueError, match="Expected columns not found in extracted table"
    ):
        extractor.validate(df, "page1", partition="partition1")


def test_validate_extra_and_missing_columns(mocker, extractor):
    # Mock the partition selection and page columns
    extractor.get_page_cols = mocker.MagicMock(return_value={"col1", "col2"})

    # Create a DataFrame with both extra and missing columns
    df = pd.DataFrame(columns=["col1", "col3"])

    # Call the validate method and check for ValueError
    with pytest.raises(ValueError, match="Extra columns found in extracted table"):
        extractor.validate(df, "page1", partition="partition1")

    # Adjust the DataFrame to only have missing columns
    df = pd.DataFrame(columns=["col1"])

    # Call the validate method and check for ValueError
    with pytest.raises(
        ValueError, match="Expected columns not found in extracted table"
    ):
        extractor.validate(df, "page1", partition="partition1")
