from unittest import mock
from unittest.mock import MagicMock, patch

import sqlalchemy as sa

from pudl.extract.csv import CsvTableSchema

TABLE_NAME = "e176_company"

COL1_NAME = "COMPANY_ID"
COL1_TYPE = sa.String

COL2_NAME = "ACTIVITY_STATUS"
COL2_TYPE = sa.String
COL2_SHORT_NAME = "status"


def get_csv_table_schema():
    table_schema = CsvTableSchema(TABLE_NAME)
    table_schema.add_column(COL1_NAME, COL1_TYPE)
    table_schema.add_column(COL2_NAME, COL2_TYPE, COL2_SHORT_NAME)
    return table_schema


def test_csv_table_schema_get_columns():
    table_schema = get_csv_table_schema()
    assert table_schema.name == TABLE_NAME
    assert [(COL1_NAME, COL1_TYPE), (COL2_NAME, COL2_TYPE)] == list(
        table_schema.get_columns()
    )


def test_csv_table_schema_get_column_names():
    table_schema = get_csv_table_schema()
    assert {COL1_NAME, COL2_NAME} == table_schema.get_column_names()


def test_csv_table_schema_get_column_rename_map():
    table_schema = get_csv_table_schema()
    assert {COL2_SHORT_NAME: COL2_NAME} == table_schema.get_column_rename_map()


@patch("pudl.extract.csv.sa.Table")
@patch("pudl.extract.csv.sa.Column")
def test_csv_table_schema_create_sa_table(mock_column, mock_table):
    sa_meta = MagicMock(sa.MetaData)
    table_schema = get_csv_table_schema()
    table = table_schema.create_sa_table(sa_meta)
    assert table == mock_table.return_value
    mock_table.assert_called_once_with(TABLE_NAME, sa_meta)

    expected_calls = [
        mock.call(mock_column(COL1_NAME, COL1_TYPE)),
        mock.call(mock_column(COL2_NAME, COL2_TYPE)),
    ]

    table.append_column.assert_has_calls(expected_calls)
