"""Test Dagster IO Managers."""
import pandas as pd
import pytest
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.io_managers import (
    ForeignKeyError,
    ForeignKeyErrors,
    SQLiteIOManager,
    pudl_sqlite_io_manager,
)


@pytest.fixture
def sqlite_io_manager_fixture(tmp_path):
    """Create a SQLiteIOManager fixture with a simple database schema."""
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


def test_exclude_tables_param(tmp_path):
    """Test ability to exclude tables from being created in the database."""
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
    io_manager = SQLiteIOManager(
        base_dir=tmp_path, db_name="pudl", md=md, exclude_tables=("track")
    )
    with io_manager.engine.connect() as con:
        table_names = pd.read_sql(
            "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';",
            con,
        )

    # Make sure the track table is in the metadata object but not created in the database
    assert "track" in io_manager.md.tables
    assert "track" not in table_names["name"]


def test_view_exclusion() -> None:
    io_manager = pudl_sqlite_io_manager(None)
    with io_manager.engine.connect() as con:
        table_names = pd.read_sql(
            "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';",
            con,
        )
    # Make sure the track table is in the metadata object but not created in the database
    assert "denorm_plants_utils_ferc1" in io_manager.md.tables
    assert "denorm_plants_utils_ferc1" not in table_names["name"]


def test_handle_output_view_raises_error_when_missing_metadata(
    sqlite_io_manager_fixture,
):
    """Make sure an error is thrown if we try to add a view that doesn't have
    metadata."""
    io_manager = sqlite_io_manager_fixture

    asset_key = "artist_view"
    artist_view = "CREATE VIEW artist_view AS SELECT * FROM artist;"
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(RuntimeError):
        io_manager.handle_output(output_context, artist_view)


def test_load_input_view_raises_error_when_missing_metadata(sqlite_io_manager_fixture):
    """Make sure an error is thrown if we try to read from a view that doesn't have
    metadata."""
    io_manager = sqlite_io_manager_fixture

    asset_key = "artist_view"
    artist_view = "CREATE VIEW artist_view AS SELECT * FROM artist;"
    # Create view directly instead of using handle_output()
    with io_manager.engine.connect() as con:
        con.execute(artist_view)

    # Read the table back into pandas
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    with pytest.raises(RuntimeError):
        io_manager.load_input(input_context)
