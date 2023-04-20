import sqlalchemy as sa

from pudl.cli import reset_db


def test_reset_db_no_preexisting(tmp_path):
    sqlite_path = tmp_path / "test_db.sqlite"

    # Define the metadata for the test database
    metadata = sa.MetaData()
    sa.Table("foo", metadata, sa.Column("bar", sa.Integer))

    reset_db.reset_db(sqlite_path=sqlite_path, metadata=metadata)

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    reflected_metadata = sa.MetaData()
    reflected_metadata.reflect(bind=engine)

    assert len(metadata.tables) == len(reflected_metadata.tables)
    assert "foo" in reflected_metadata.tables
    assert len(metadata.tables["foo"].columns) == len(
        reflected_metadata.tables["foo"].columns
    )
    assert "bar" in reflected_metadata.tables["foo"].columns


def test_reset_db_clobbers_preexisting(tmp_path):
    sqlite_path = tmp_path / "test_db.sqlite"

    # initial schema - we'll overwrite this later
    metadata = sa.MetaData()
    sa.Table("foo", metadata, sa.Column("bar", sa.Integer))
    reset_db.reset_db(sqlite_path=sqlite_path, metadata=metadata)

    # actual schema
    metadata = sa.MetaData()
    sa.Table("foo", metadata, sa.Column("baz", sa.Integer))
    reset_db.reset_db(sqlite_path=sqlite_path, metadata=metadata)

    engine = sa.create_engine(f"sqlite:///{sqlite_path}")
    reflected_metadata = sa.MetaData()
    reflected_metadata.reflect(bind=engine)

    assert len(metadata.tables) == len(reflected_metadata.tables)
    assert "foo" in reflected_metadata.tables
    assert len(metadata.tables["foo"].columns) == len(
        reflected_metadata.tables["foo"].columns
    )
    assert "baz" in reflected_metadata.tables["foo"].columns
