"""Unit tests for pudl.extract.csv module."""
from unittest.mock import MagicMock, patch

from pudl.extract.csv import CsvArchive, CsvExtractor, CsvReader

TABLE_NAME = "company"

FILENAME = "all_company_176.csv"
TABLE_FILE_MAP = {TABLE_NAME: FILENAME}

DATASET = "eia176"
DATABASE_NAME = f"{DATASET}.sqlite"


class FakeCsvExtractor(CsvExtractor):
    DATASET = DATASET
    DATABASE_NAME = DATABASE_NAME


def get_csv_extractor():
    datastore = MagicMock()
    return FakeCsvExtractor(datastore)


@patch("pudl.extract.csv.pd")
@patch("pudl.extract.csv.ZipFile")
def test_csv_archive_load_table(mock_zipfile, mock_pd):
    archive = CsvArchive(mock_zipfile)
    res = archive.load_table(FILENAME)
    mock_zipfile.open.assert_called_once_with(FILENAME)
    f = mock_zipfile.open.return_value.__enter__.return_value
    mock_pd.read_csv.assert_called_once_with(f)
    df = mock_pd.read_csv()
    assert df == res


def test_csv_reader_get_table_names():
    datastore = MagicMock()
    reader = CsvReader(datastore, DATASET)
    assert reader._table_file_map == TABLE_FILE_MAP
    assert [TABLE_NAME] == reader.get_table_names()


def test_csv_reader_get_archive():
    datastore = MagicMock()
    reader = CsvReader(datastore, DATASET)
    archive = reader.get_archive()
    zipfile = datastore.get_zipfile_resource(DATASET)
    assert zipfile == archive.zipfile


def test_csv_extractor_get_csv_reader():
    extractor = get_csv_extractor()
    datastore = MagicMock()
    reader = extractor.get_csv_reader(datastore)
    assert datastore == reader.datastore
    assert reader.dataset == DATASET


@patch("pudl.extract.csv.CsvArchive")
def test_csv_extractor_extract(mock_archive):
    extractor = get_csv_extractor()
    raw_dfs = extractor.extract()
    mock_archive.return_value.load_table.assert_called_once_with(FILENAME)
    df = mock_archive.return_value.load_table.return_value
    assert {TABLE_NAME: df} == raw_dfs
