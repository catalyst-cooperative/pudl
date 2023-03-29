"""Test Dagster IO Managers."""
import pandas as pd
import pytest
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.io_managers import (
    ForeignKeyError,
    ForeignKeyErrors,
    MetadataDiffError,
    SQLiteIOManager,
)


@pytest.fixture
def db_metadata() -> MetaData:
    """Create a sample metadata fixture for io manager tests."""
    md = MetaData()
    artist = Table(  # noqa: F841
        "artist",
        md,
        Column("artistid", Integer, primary_key=True),
        Column("artistname", String(16), nullable=False),
    )
    track = Table(  # noqa: F841
        "track",
        md,
        Column("trackid", Integer, primary_key=True),
        Column("trackname", String(16), nullable=False),
        Column("trackartist", Integer, ForeignKey("artist.artistid")),
    )
    return md


@pytest.fixture
def sqlite_io_manager_fixture(tmp_path, db_metadata):
    """Create a SQLiteIOManager fixture with a simple database schema."""
    return SQLiteIOManager(base_dir=tmp_path, db_name="pudl", md=db_metadata)


def test_metadata_change_error(sqlite_io_manager_fixture):
    """Test a MetadataDiffError is raised when the metadata changes."""
    md = MetaData()
    artist = Table(  # noqa: F841
        "artist",
        md,
        Column("artistid", Integer, primary_key=True),
        Column("artistname", String(16), nullable=False),
    )
    base_dir = sqlite_io_manager_fixture.base_dir

    with pytest.raises(MetadataDiffError):
        SQLiteIOManager(base_dir=base_dir, db_name="pudl", md=md)


def test_unchanged_metadata(sqlite_io_manager_fixture, db_metadata):
    """Test a MetadataDiffError isn't raised when the metadata doesn't change."""
    base_dir = sqlite_io_manager_fixture.base_dir

    SQLiteIOManager(base_dir=base_dir, db_name="pudl", md=db_metadata)


def test_unchanged_metadata_with_view(sqlite_io_manager_fixture, db_metadata):
    """Test a MetadataDiffError isn't raised when a view is created."""
    query = """CREATE VIEW tracks_view AS SELECT * FROM track;"""

    engine = sqlite_io_manager_fixture.engine
    with engine.connect() as conn:
        conn.execute(query)
        tracks_view = pd.read_sql_table("tracks_view", conn)
    assert tracks_view.shape == (0, 3)

    base_dir = sqlite_io_manager_fixture.base_dir

    SQLiteIOManager(base_dir=base_dir, db_name="pudl", md=db_metadata)


def test_sqlite_io_manager_delete_stmt(sqlite_io_manager_fixture):
    """Test we are replacing the data without dropping the table schema."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    # Read the table back into pandas
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    returned_df = manager.load_input(input_context)
    assert len(returned_df) == 1

    # Rerun the asset
    # Load the dataframe to a sqlite table
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    # Read the table back into pandas
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    returned_df = manager.load_input(input_context)
    assert len(returned_df) == 1


def test_foreign_key_failure(sqlite_io_manager_fixture):
    """Ensure ForeignKeyErrors are raised when there are foreign key errors."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, artist)

    asset_key = "track"
    track = pd.DataFrame(
        {"trackid": [1], "trackname": ["FERC Ya!"], "trackartist": [2]}
    )
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, track)

    with pytest.raises(ForeignKeyErrors) as excinfo:
        manager.check_foreign_keys()

    assert excinfo.value[0] == ForeignKeyError(
        child_table="track",
        parent_table="artist",
        foreign_key="(artistid)",
        rowids=[1],
    )


def test_extra_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when there is an extra column in the dataframe."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {"artistid": [1], "artistname": ["Co-op Mop"], "artistmanager": [1]}
    )
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(OperationalError):
        manager.handle_output(output_context, artist)


def test_missing_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a dataframe is missing a column in the schema."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {
            "artistid": [1],
        }
    )
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(RuntimeError):
        manager.handle_output(output_context, artist)


def test_nullable_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a non nullable column is missing data."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1, 2], "artistname": ["Co-op Mop", pd.NA]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))

    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


@pytest.mark.xfail(reason="SQLite autoincrement behvior is breaking this test.")
def test_null_primary_key_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a primary key contains a nullable value."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame(
        {"artistid": [1, pd.NA], "artistname": ["Co-op Mop", "Cxtxlyst"]}
    )
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_primary_key_column_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a primary key is violated."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1, 1], "artistname": ["Co-op Mop", "Cxtxlyst"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_incorrect_type_error(sqlite_io_manager_fixture):
    """Ensure an error is thrown when a dataframe's type doesn't match the table
    schema."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": ["abc"], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_missing_schema_error(sqlite_io_manager_fixture):
    """Test a RuntimeError is raised when a table without a schema is loaded."""
    manager = sqlite_io_manager_fixture

    asset_key = "venues"
    venue = pd.DataFrame({"venueid": [1], "venuename": "Vans Dive Bar"})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(RuntimeError):
        manager.handle_output(output_context, venue)
