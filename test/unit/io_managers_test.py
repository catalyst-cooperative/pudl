"""Test Dagster IO Managers."""
import pandas as pd
import pytest
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table

from pudl.io_managers import ForeignKeyError, ForeignKeyErrors, SQLiteIOManager


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
    artist = pd.DataFrame({"artistid": [1], "artistname": "Co-op Mop"})
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
    artist = pd.DataFrame({"artistid": [1], "artistname": "Co-op Mop"})
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
