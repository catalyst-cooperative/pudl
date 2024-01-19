"""Unit tests for pudl.extract.csv module."""
from unittest.mock import MagicMock, call, patch

from pudl.extract.csv import CsvExtractor, get_table_file_map, open_csv_resource

DATASET = "eia176"
BASE_FILENAME = "table_file_map.csv"
TABLE_NAME = ["company", "data", "other"]
FILE_NAME = ["all_company_176.csv", "all_data_176.csv", "all_other_176.csv"]
TABLE_FILE_MAP = {TABLE_NAME[i]: FILE_NAME[i] for i in range(len(TABLE_NAME))}


def get_csv_extractor():
    zipfile = MagicMock()
    return CsvExtractor(zipfile, TABLE_FILE_MAP)


def test_open_csv_resource():
    csv_resource = open_csv_resource(DATASET, BASE_FILENAME)
    assert ["table", "filename"] == csv_resource.fieldnames


def test_get_table_file_map():
    table_file_map = get_table_file_map(DATASET)
    assert table_file_map == TABLE_FILE_MAP


def test_get_table_names():
    extractor = get_csv_extractor()
    table_names = extractor.get_table_names()
    assert table_names == TABLE_NAME


@patch("pudl.extract.csv.pd")
def test_csv_extractor_read_source(mock_pd):
    extractor = get_csv_extractor()
    res = extractor.extract_one(TABLE_NAME[0])
    mock_zipfile = extractor._zipfile
    mock_zipfile.open.assert_called_once_with(FILE_NAME[0])
    f = mock_zipfile.open.return_value.__enter__.return_value
    mock_pd.read_csv.assert_called_once_with(f)
    df = mock_pd.read_csv()
    assert df == res


def test_csv_extractor_extract():
    extractor = get_csv_extractor()
    df = MagicMock()
    with patch.object(CsvExtractor, "extract_one", return_value=df) as mock_read_source:
        raw_dfs = extractor.extract_all()
    mock_read_source.assert_has_calls(
        calls=[call(table_name) for table_name in TABLE_NAME]
    )
    assert {table_name: df for table_name in TABLE_NAME} == raw_dfs
