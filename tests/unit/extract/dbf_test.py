"""Unit tests for the schema handling in pudl.extract.dbf."""

from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest
import sqlalchemy as sa
from dbfread import FieldParser

from pudl.extract.dbf import DBF_TYPES, UNSUPPORTED_DBF_TYPES, FercDbfArchive


def _archive_with_field(mocker, field_type: str) -> FercDbfArchive:
    """Make an archive with one table holding a single field of the given type."""
    archive = FercDbfArchive(
        zipfile=mocker.Mock(spec=ZipFile),
        dbc_path=Path("dbc/f1.dbc"),
        table_file_map={"f1_test": "TEST.DBF"},
        partition={"year": 2020},
        field_parser=FieldParser,
    )
    mocker.patch.object(
        archive, "get_db_schema", return_value={"f1_test": ["test_field"]}
    )
    dbf = SimpleNamespace(
        fields=[SimpleNamespace(name="test_fiel", type=field_type, length=10)]
    )
    mocker.patch.object(archive, "get_table_dbf", return_value=dbf)
    return archive


def test_dbf_types_and_unsupported_dbf_types_are_disjoint():
    """A field type is either mapped to an SQLAlchemy type or unsupported."""
    assert not set(DBF_TYPES) & set(UNSUPPORTED_DBF_TYPES)


def test_table_schema_maps_string_fields_with_their_length(mocker):
    """String fields are mapped to a String that is as long as the field."""
    schema = _archive_with_field(mocker, "C").get_table_schema("f1_test")
    column_type = schema.create_sa_table(sa.MetaData()).c.test_field.type
    assert isinstance(column_type, sa.String)
    assert column_type.length == 10


@pytest.mark.parametrize("field_type", sorted(UNSUPPORTED_DBF_TYPES))
def test_table_schema_rejects_unsupported_field_types(mocker, field_type):
    """Fields of types that are not mapped are reported as not supported."""
    archive = _archive_with_field(mocker, field_type)
    with pytest.raises(NotImplementedError, match="f1_test is not supported"):
        archive.get_table_schema("f1_test")
