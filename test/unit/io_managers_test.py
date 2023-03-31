"""Test Dagster IO Managers."""
import pandas as pd
import pytest
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.io_managers import (
    ForeignKeyError,
    ForeignKeyErrors,
    PudlSQLiteIOManager,
    SQLiteIOManager,
)
from pudl.metadata.classes import Package, Resource


@pytest.fixture
def test_pkg() -> Package:
    """Create a test metadata package for the io manager tests."""
    fields = [
        {"name": "artistid", "type": "integer"},
        {"name": "artistname", "type": "string", "constraints": {"required": True}},
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    artist_resource = Resource(name="artist", schema=schema)

    fields = [
        {"name": "artistid", "type": "integer"},
        {"name": "artistname", "type": "string", "constraints": {"required": True}},
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    view_resource = Resource(
        name="artist_view", schema=schema, include_in_database=False
    )

    fields = [
        {"name": "trackid", "type": "integer"},
        {"name": "trackname", "type": "string", "constraints": {"required": True}},
        {"name": "trackartist", "type": "integer"},
    ]
    fkeys = [
        {
            "fields": ["trackartist"],
            "reference": {"resource": "artist", "fields": ["artistid"]},
        }
    ]
    schema = {"fields": fields, "primary_key": ["trackid"], "foreign_keys": fkeys}
    track_resource = Resource(name="track", schema=schema)
    return Package(
        name="music", resources=[track_resource, artist_resource, view_resource]
    )


@pytest.fixture
def sqlite_io_manager_fixture(tmp_path, test_pkg):
    """Create a SQLiteIOManager fixture with a simple database schema."""
    md = test_pkg.to_sql()
    return SQLiteIOManager(base_dir=tmp_path, db_name="pudl", md=md)


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
    with pytest.raises(ValueError):
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
    """Ensure an error is thrown when dataframe type doesn't match the table schema."""
    manager = sqlite_io_manager_fixture

    asset_key = "artist"
    artist = pd.DataFrame({"artistid": ["abc"], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(IntegrityError):
        manager.handle_output(output_context, artist)


def test_missing_schema_error(sqlite_io_manager_fixture):
    """Test a ValueError is raised when a table without a schema is loaded."""
    manager = sqlite_io_manager_fixture

    asset_key = "venues"
    venue = pd.DataFrame({"venueid": [1], "venuename": "Vans Dive Bar"})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        manager.handle_output(output_context, venue)


@pytest.fixture
def pudl_sqlite_io_manager_fixture(tmp_path, test_pkg):
    """Create a SQLiteIOManager fixture with a PUDL database schema."""
    return PudlSQLiteIOManager(base_dir=tmp_path, db_name="pudl", package=test_pkg)


def test_error_when_handling_view_without_metadata(pudl_sqlite_io_manager_fixture):
    """Make sure an error is thrown when a user creates a view without metadata."""
    asset_key = "track_view"
    sql_stmt = "CREATE VIEW track_view AS SELECT * FROM track;"
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        pudl_sqlite_io_manager_fixture.handle_output(output_context, sql_stmt)


def test_handling_view_with_metadata(pudl_sqlite_io_manager_fixture):
    """Make sure an users can create and load views when it has metadata."""
    # Create some sample data
    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    pudl_sqlite_io_manager_fixture.handle_output(output_context, artist)

    # create the view
    asset_key = "artist_view"
    sql_stmt = "CREATE VIEW artist_view AS SELECT * FROM artist;"
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    pudl_sqlite_io_manager_fixture.handle_output(output_context, sql_stmt)

    # read the view data as a dataframe
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    pudl_sqlite_io_manager_fixture.load_input(input_context)


def test_error_when_reading_view_without_metadata(pudl_sqlite_io_manager_fixture):
    """Make sure and error is thrown when a user loads a view without metadata."""
    asset_key = "track_view"
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        pudl_sqlite_io_manager_fixture.load_input(input_context)
