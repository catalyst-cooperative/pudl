"""Unit tests for pudl.extract.csv module."""
from unittest.mock import MagicMock, patch

from pudl.extract.csv import CsvExtractor, get_table_file_map, open_csv_resource

DATASET = "eia176"
BASE_FILENAME = "table_file_map.csv"
TABLE_NAME = "company"
FILENAME = "all_company_176.csv"
TABLE_FILE_MAP = {TABLE_NAME: FILENAME}


def get_csv_extractor():
    zipfile = MagicMock()
    return CsvExtractor(zipfile, TABLE_FILE_MAP)


def test_open_csv_resource():
    csv_resource = open_csv_resource(DATASET, BASE_FILENAME)
    assert csv_resource.fieldnames == ["table", "filename"]


def test_get_table_file_map():
    table_file_map = get_table_file_map(DATASET)
    assert table_file_map == TABLE_FILE_MAP


def test_get_table_names():
    extractor = get_csv_extractor()
    table_names = extractor.get_table_names()
    assert table_names == [TABLE_NAME]


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
    assert raw_dfs == {TABLE_NAME: df}
