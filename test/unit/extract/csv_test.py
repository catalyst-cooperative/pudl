"""Unit tests for pudl.extract.csv module."""
from unittest.mock import MagicMock, patch

import pandas as pd

from pudl.extract.csv import CsvExtractor

DATASET = "eia176"
PARTITION = 2023
CSV_FILENAME = f"{DATASET}-{PARTITION}.csv"
PAGE = "data"


def get_csv_extractor():
    mock_ds = MagicMock()
    return CsvExtractor(mock_ds, DATASET)


def test_csv_filename():
    extractor = get_csv_extractor()
    assert extractor.csv_filename(PARTITION) == CSV_FILENAME


@patch("pudl.extract.csv.pd")
def test_load_csv_file(mock_pd):
    extractor = get_csv_extractor()

    assert mock_pd.read_csv.return_value == extractor.load_csv_file(year=PARTITION)
    extractor.ds.get_zipfile_resource.assert_called_once_with(DATASET)
    zipfile = extractor.ds.get_zipfile_resource.return_value.__enter__.return_value
    zipfile.open.assert_called_once_with(CSV_FILENAME)
    file = zipfile.open.return_value.__enter__.return_value
    mock_pd.read_csv.assert_called_once_with(file)


def test_extract():
    extractor = get_csv_extractor()
    dummy_field = "company"
    dummy_value = "Total of All Companies"
    df = pd.DataFrame([dummy_value])
    df.columns = [dummy_field]
    with patch.object(CsvExtractor, "load_csv_file", return_value=df):
        res = extractor.extract(year=PARTITION)
    assert len(res) == 1
    print(res)
    assert dummy_value == res[PAGE][dummy_field][0]
