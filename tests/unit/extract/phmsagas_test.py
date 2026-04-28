import pandas as pd
import pytest

from pudl.extract.excel import ExcelMetadata
from pudl.extract.phmsagas import Extractor


class FakeExtractor(Extractor):
    def __init__(self, mocker):
        self.METADATA = ExcelMetadata("phmsagas")
        super().__init__(ds=mocker.Mock())
        self._metadata = mocker.Mock()


@pytest.fixture
def extractor(mocker):
    # Create an instance of the CsvExtractor class
    return FakeExtractor(mocker)


def test_process_renamed_drop_columns(mocker, extractor):
    mock_logger = mocker.patch("pudl.extract.phmsagas.logger")
    # Mock metadata methods
    extractor._metadata.get_form.return_value = "gas_transmission_gathering"
    extractor._metadata.get_all_columns.return_value = ["col1", "col2"]

    # Create a DataFrame with extra columns
    data = {"col1": [1, 2], "col2": [3, 4], "extra_col": [5, 6]}
    df = pd.DataFrame(data)

    # Call the method
    result = extractor.process_renamed(df, "some_page", year=2009)

    # Check that the extra column was dropped
    assert "extra_col" not in result.columns
    mock_logger.info.assert_called_once()


def test_process_renamed_keep_columns(mocker, extractor):
    mock_logger = mocker.patch("pudl.extract.phmsagas.logger")
    # Mock metadata methods
    extractor._metadata.get_form.return_value = "gas_transmission_gathering"
    extractor._metadata.get_all_columns.return_value = ["col1", "col2"]

    # Create a DataFrame without extra columns
    data = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data)

    # Call the method
    result = extractor.process_renamed(df, "some_page", year=2009)

    # Check that no columns were dropped
    assert list(result.columns) == ["col1", "col2"]
    mock_logger.info.assert_not_called()


def test_process_renamed_drop_unnamed_columns(mocker, extractor):
    mock_logger = mocker.patch("pudl.extract.phmsagas.logger")
    # Mock metadata methods
    extractor._metadata.get_form.return_value = "some_form"
    extractor._metadata.get_all_columns.return_value = ["col1", "col2"]

    # Create a DataFrame with unnamed columns
    data = {"col1": [1, 2], "col2": [3, 4], "unnamed_0": [5, 6]}
    df = pd.DataFrame(data)

    # Call the method
    result = extractor.process_renamed(df, "yearly_distribution", year=2000)

    # Check that the unnamed column was dropped
    assert "Unnamed: 0" not in result.columns
    mock_logger.warning.assert_not_called()


def test_process_renamed_warn_unnamed_columns(mocker, extractor):
    mock_logger = mocker.patch("pudl.extract.phmsagas.logger")
    # Mock metadata methods
    extractor._metadata.get_form.return_value = "some_form"
    extractor._metadata.get_all_columns.return_value = ["col1", "col2"]

    # Create a DataFrame with unnamed columns
    data = {"col1": [1, 2], "col2": [3, 4], "unnamed_0": [5, 6]}
    df = pd.DataFrame(data)

    # Call the method
    result = extractor.process_renamed(df, "some_page", year=2011)

    # Check that the unnamed column was not dropped but a warning was logged
    assert "unnamed_0" in result.columns
    mock_logger.warning.assert_called_once()
