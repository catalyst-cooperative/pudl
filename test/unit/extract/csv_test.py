"""Unit tests for pudl.extract.csv module."""
from unittest.mock import MagicMock, patch

from pudl.extract.csv import CsvExtractor

TABLE_NAME = "company"

FILENAME = "all_company_176.csv"
TABLE_FILE_MAP = {TABLE_NAME: FILENAME}

DATASET = "eia176"


def get_csv_extractor():
    datastore = MagicMock()
    return CsvExtractor(datastore, DATASET)


def test_get_table_names():
    extractor = get_csv_extractor()
    table_names = extractor.get_table_names()
    assert [TABLE_NAME] == table_names


@patch("pudl.extract.csv.pd")
def test_csv_extractor_read_source(mock_pd):
    extractor = get_csv_extractor()
    res = extractor.extract_one(TABLE_NAME)
    mock_zipfile = extractor._zipfile
    mock_zipfile.open.assert_called_once_with(FILENAME)
    f = mock_zipfile.open.return_value.__enter__.return_value
    mock_pd.read_csv.assert_called_once_with(f)
    df = mock_pd.read_csv()
    assert df == res


def test_csv_extractor_extract():
    extractor = get_csv_extractor()
    df = MagicMock()
    with patch.object(CsvExtractor, "extract_one", return_value=df) as mock_read_source:
        raw_dfs = extractor.extract_all()
    mock_read_source.assert_called_once_with(TABLE_NAME)
    assert {TABLE_NAME: df} == raw_dfs
