"""Unit tests for pudl.extract.csv module."""
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import sqlalchemy as sa
from pytest import raises

from pudl.extract.csv import CsvArchive, CsvExtractor, CsvReader, CsvTableSchema

TABLE_NAME = "e176_company"

COL1_NAME = "COMPANY_ID"
COL1_TYPE = sa.String

COL2_NAME = "ACTIVITY_STATUS"
COL2_TYPE = sa.String
COL2_SHORT_NAME = "status"

FILENAME = "all_company_176.csv"
TABLE_FILE_MAP = {TABLE_NAME: FILENAME}

COLUMN_TYPES = {
    TABLE_NAME: {
        COL1_NAME: COL1_TYPE,
        COL2_NAME: COL2_TYPE,
    },
}

DATASET = "eia176"
DATABASE_NAME = f"{DATASET}.sqlite"
PATHNAME = "fakepath"


class FakeCsvExtractor(CsvExtractor):
    DATASET = DATASET
    DATABASE_NAME = DATABASE_NAME
    COLUMN_TYPES = COLUMN_TYPES


def get_csv_table_schema():
    table_schema = CsvTableSchema(TABLE_NAME)
    table_schema.add_column(COL1_NAME, COL1_TYPE)
    table_schema.add_column(COL2_NAME, COL2_TYPE, COL2_SHORT_NAME)
    return table_schema


def get_csv_extractor():
    datastore = MagicMock()
    path = Path(PATHNAME)
    return FakeCsvExtractor(datastore, path)


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


@patch("pudl.extract.csv.DictReader")
@patch("pudl.extract.csv.TextIOWrapper")
@patch("pudl.extract.csv.ZipFile")
def test_csv_archive_get_table_schema_valid(
    mock_zipfile, mock_text_io_wrapper, mock_dict_reader
):
    mock_dict_reader.return_value.fieldnames = [COL1_NAME, COL2_NAME]
    zipfile = MagicMock(ZipFile)
    archive = CsvArchive(zipfile, TABLE_FILE_MAP, COLUMN_TYPES)
    schema = archive.get_table_schema(TABLE_NAME)
    assert [(COL1_NAME, COL1_TYPE), (COL2_NAME, COL2_TYPE)] == list(
        schema.get_columns()
    )


@patch("pudl.extract.csv.DictReader")
@patch("pudl.extract.csv.TextIOWrapper")
@patch("pudl.extract.csv.ZipFile")
def test_csv_archive_get_table_schema_invalid(
    mock_zipfile, mock_text_io_wrapper, mock_dict_reader
):
    mock_dict_reader.return_value.fieldnames = [COL1_NAME]
    zipfile = MagicMock(ZipFile)
    archive = CsvArchive(zipfile, TABLE_FILE_MAP, COLUMN_TYPES)
    with raises(ValueError):
        archive.get_table_schema(TABLE_NAME)


@patch("pudl.extract.csv.pd")
@patch("pudl.extract.csv.ZipFile")
def test_csv_archive_load_table(mock_zipfile, mock_pd):
    archive = CsvArchive(mock_zipfile, TABLE_FILE_MAP, COLUMN_TYPES)
    res = archive.load_table(FILENAME)
    mock_zipfile.open.assert_called_once_with(FILENAME)
    f = mock_zipfile.open.return_value.__enter__.return_value
    mock_pd.read_csv.assert_called_once_with(f)
    df = mock_pd.read_csv()
    assert df == res


def test_csv_reader_get_table_names():
    datastore = MagicMock()
    reader = CsvReader(datastore, DATASET, COLUMN_TYPES)
    assert reader._table_file_map == TABLE_FILE_MAP
    assert [TABLE_NAME] == reader.get_table_names()


def test_csv_reader_get_archive():
    datastore = MagicMock()
    reader = CsvReader(datastore, DATASET, COLUMN_TYPES)
    archive = reader.get_archive()
    zipfile = datastore.get_zipfile_resource(DATASET)
    assert zipfile == archive.zipfile
    assert archive._table_file_map == TABLE_FILE_MAP
    assert archive._column_types == COLUMN_TYPES


def test_csv_extractor_get_db_path():
    extractor = get_csv_extractor()
    assert f"sqlite:///{PATHNAME}/{DATABASE_NAME}" == extractor.get_db_path()


def test_csv_extractor_get_csv_reader():
    extractor = get_csv_extractor()
    datastore = MagicMock()
    reader = extractor.get_csv_reader(datastore)
    assert datastore == reader.datastore
    assert reader.dataset == DATASET
    assert reader._column_types == COLUMN_TYPES


@patch("pudl.extract.csv.sa.create_engine")
@patch("pudl.extract.csv.CsvExtractor.finalize_schema")
@patch("pudl.extract.csv.sa.MetaData")
@patch("pudl.extract.csv.CsvArchive")
def test_csv_extractor_create_sqlite_tables(
    mock_archive, mock_metadata, mock_finalize_schema, mock_create_engine
):
    extractor = get_csv_extractor()
    extractor.create_sqlite_tables()
    sqlite_meta = mock_metadata.return_value
    get_table_schema = mock_archive.return_value.get_table_schema
    get_table_schema.assert_called_once_with(TABLE_NAME)
    get_table_schema.return_value.create_sa_table.assert_called_once_with(sqlite_meta)
    mock_finalize_schema.assert_called_once_with(sqlite_meta)
    sqlite_engine = mock_create_engine(extractor.get_db_path())
    mock_metadata.return_value.create_all.assert_called_once_with(sqlite_engine)


@patch("pudl.extract.csv.sa.create_engine")
@patch("pudl.extract.csv.sa.MetaData")
@patch("pudl.extract.csv.CsvArchive")
def test_csv_extractor_load_table_data(mock_archive, mock_metadata, mock_create_engine):
    extractor = get_csv_extractor()
    extractor.load_table_data()
    mock_archive.return_value.load_table.assert_called_once_with(FILENAME)
    df = extractor.csv_reader.get_archive().load_table()
    sqlite_meta = mock_metadata.return_value
    sqlite_engine = mock_create_engine.return_value
    coltypes = {col.name: col.type for col in sqlite_meta.tables[TABLE_NAME].c}
    df.to_sql.assert_called_once_with(
        TABLE_NAME,
        sqlite_engine,
        if_exists="append",
        chunksize=100000,
        dtype=coltypes,
        index=False,
    )


@patch("pudl.extract.csv.CsvExtractor.postprocess")
@patch("pudl.extract.csv.CsvExtractor.load_table_data")
@patch("pudl.extract.csv.CsvExtractor.create_sqlite_tables")
@patch("pudl.extract.csv.CsvExtractor.delete_schema")
def test_csv_extractor_execute(
    mock_delete_schema,
    mock_create_sqlite_tables,
    mock_load_table_data,
    mock_postprocess,
):
    extractor = get_csv_extractor()
    extractor.execute()
    mock_delete_schema.assert_called_once()
    mock_create_sqlite_tables.assert_called_once()
    mock_load_table_data.assert_called_once()
    mock_postprocess.assert_called_once()
