"""Test Dagster IO Managers."""

from pathlib import Path

import alembic.config
import pandas as pd
import pytest
import sqlalchemy as sa
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.etl.check_foreign_keys import (
    ForeignKeyError,
    ForeignKeyErrors,
    check_foreign_keys,
)
from pudl.io_managers import (
    FercXBRLSQLiteIOManager,
    PudlSQLiteIOManager,
    SQLiteIOManager,
)
from pudl.metadata import PUDL_PACKAGE
from pudl.metadata.classes import Package, Resource


@pytest.fixture
def test_pkg() -> Package:
    """Create a test metadata package for the io manager tests."""
    fields = [
        {"name": "artistid", "type": "integer", "description": "artistid"},
        {
            "name": "artistname",
            "type": "string",
            "constraints": {"required": True},
            "description": "artistid",
        },
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    artist_resource = Resource(name="artist", schema=schema, description="Artist")

    fields = [
        {"name": "artistid", "type": "integer", "description": "artistid"},
        {
            "name": "artistname",
            "type": "string",
            "constraints": {"required": True},
            "description": "artistname",
        },
    ]
    schema = {"fields": fields, "primary_key": ["artistid"]}
    view_resource = Resource(
        name="artist_view",
        schema=schema,
        description="Artist view",
        create_database_schema=False,
    )

    fields = [
        {"name": "trackid", "type": "integer", "description": "trackid"},
        {
            "name": "trackname",
            "type": "string",
            "constraints": {"required": True},
            "description": "trackname",
        },
        {"name": "trackartist", "type": "integer", "description": "trackartist"},
    ]
    fkeys = [
        {
            "fields": ["trackartist"],
            "reference": {"resource": "artist", "fields": ["artistid"]},
        }
    ]
    schema = {"fields": fields, "primary_key": ["trackid"], "foreign_keys": fkeys}
    track_resource = Resource(name="track", schema=schema, description="Track")
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
        check_foreign_keys(manager.engine)

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
def fake_pudl_sqlite_io_manager_fixture(tmp_path, test_pkg, monkeypatch):
    """Create a SQLiteIOManager fixture with a fake database schema."""
    db_path = tmp_path / "fake.sqlite"

    # Create the database and schemas
    engine = sa.create_engine(f"sqlite:///{db_path}")
    md = test_pkg.to_sql()
    md.create_all(engine)
    return PudlSQLiteIOManager(base_dir=tmp_path, db_name="fake", package=test_pkg)


def test_pudl_sqlite_io_manager_delete_stmt(fake_pudl_sqlite_io_manager_fixture):
    """Test we are replacing the data without dropping the table schema."""
    manager = fake_pudl_sqlite_io_manager_fixture

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


@pytest.mark.slow
def test_migrations_match_metadata(tmp_path, monkeypatch):
    """If you create a `PudlSQLiteIOManager` that points at a non-existing
    `pudl.sqlite` - it will initialize the DB based on the `package`.

    If you create a `PudlSQLiteIOManager` that points at an existing
    `pudl.sqlite`, like one initialized via `alembic upgrade head`, it
    will compare the existing db schema with the db schema in `package`.

    We want to make sure that the schema defined in `package` is the same as
    the one we arrive at by applying all the migrations.
    """
    # alembic wants current directory to be the one with `alembic.ini` in it
    monkeypatch.chdir(Path(__file__).parent.parent.parent)
    # alembic knows to use PudlPaths().pudl_db - so we need to set PUDL_OUTPUT env var
    monkeypatch.setenv("PUDL_OUTPUT", str(tmp_path))
    # run all the migrations on a fresh DB at tmp_path/pudl.sqlite
    alembic.config.main(["upgrade", "head"])

    PudlSQLiteIOManager(base_dir=tmp_path, db_name="pudl", package=PUDL_PACKAGE)

    # all we care about is that it didn't raise an error
    assert True


def test_error_when_handling_view_without_metadata(fake_pudl_sqlite_io_manager_fixture):
    """Make sure an error is thrown when a user creates a view without metadata."""
    asset_key = "track_view"
    sql_stmt = "CREATE VIEW track_view AS SELECT * FROM track;"
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, sql_stmt)


def test_empty_read_fails(fake_pudl_sqlite_io_manager_fixture):
    """Reading empty table fails."""
    with pytest.raises(AssertionError):
        context = build_input_context(asset_key=AssetKey("artist"))
        fake_pudl_sqlite_io_manager_fixture.load_input(context)


def test_replace_on_insert(fake_pudl_sqlite_io_manager_fixture):
    """Tests that two runs of the same asset overwrite existing contents."""
    artist_df = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey("artist"))
    input_context = build_input_context(asset_key=AssetKey("artist"))

    # Write then read.
    fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, artist_df)
    read_df = fake_pudl_sqlite_io_manager_fixture.load_input(input_context)
    pd.testing.assert_frame_equal(artist_df, read_df, check_dtype=False)
    # check_dtype=False, because int64 != Int64. /o\

    # Rerunning the asset overrwrites contents, leaves only
    # one artist in the database.
    new_artist_df = pd.DataFrame({"artistid": [2], "artistname": ["Cxtxlyst"]})
    fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, new_artist_df)
    read_df = fake_pudl_sqlite_io_manager_fixture.load_input(input_context)
    pd.testing.assert_frame_equal(new_artist_df, read_df, check_dtype=False)


@pytest.mark.skip(reason="SQLAlchemy is not finding the view. Debug or remove.")
def test_handling_view_with_metadata(fake_pudl_sqlite_io_manager_fixture):
    """Make sure an users can create and load views when it has metadata."""
    # Create some sample data
    asset_key = "artist"
    artist = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, artist)

    # create the view
    asset_key = "artist_view"
    sql_stmt = "CREATE VIEW artist_view AS SELECT * FROM artist;"
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    fake_pudl_sqlite_io_manager_fixture.handle_output(output_context, sql_stmt)

    # read the view data as a dataframe
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    # print(input_context)
    # This is failing, not sure why
    # sqlalchemy.exc.InvalidRequestError: Could not reflect: requested table(s) not available in
    # Engine(sqlite:////private/var/folders/pg/zrqnq8l113q57bndc5__h2640000gn/
    # # T/pytest-of-nelsonauner/pytest-38/test_handling_view_with_metada0/pudl.sqlite): (artist_view)
    fake_pudl_sqlite_io_manager_fixture.load_input(input_context)


def test_error_when_reading_view_without_metadata(fake_pudl_sqlite_io_manager_fixture):
    """Make sure and error is thrown when a user loads a view without metadata."""
    asset_key = "track_view"
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        fake_pudl_sqlite_io_manager_fixture.load_input(input_context)


def test_report_year_fixing_instant():
    instant_df = pd.DataFrame.from_records(
        [
            {
                "entity_id": "123",
                "date": "2020-07-01",
                "report_year": 3021,
                "factoid": "replace report year with date year",
            },
        ]
    )

    observed = FercXBRLSQLiteIOManager.refine_report_year(
        instant_df, xbrl_years=[2021, 2022]
    ).report_year
    expected = pd.Series([2020])
    assert (observed == expected).all()


def test_report_year_fixing_duration():
    duration_df = pd.DataFrame.from_records(
        [
            {
                "entity_id": "123",
                "start_date": "2004-01-01",
                "end_date": "2004-12-31",
                "report_year": 3021,
                "factoid": "filter out since the report year is out of bounds",
            },
            {
                "entity_id": "123",
                "start_date": "2021-01-01",
                "end_date": "2021-12-31",
                "report_year": 3021,
                "factoid": "replace report year with date year",
            },
        ]
    )

    observed = FercXBRLSQLiteIOManager.refine_report_year(
        duration_df, xbrl_years=[2021, 2022]
    ).report_year
    expected = pd.Series([2021])
    assert (observed == expected).all()


@pytest.mark.parametrize(
    "df, match",
    [
        (
            pd.DataFrame.from_records(
                [
                    {"entity_id": "123", "report_year": 3021, "date": ""},
                ]
            ),
            "date has null values",
        ),
        (
            pd.DataFrame.from_records(
                [
                    {
                        "entity_id": "123",
                        "report_year": 3021,
                        "start_date": "",
                        "end_date": "2020-12-31",
                    },
                ]
            ),
            "start_date has null values",
        ),
        (
            pd.DataFrame.from_records(
                [
                    {
                        "entity_id": "123",
                        "report_year": 3021,
                        "start_date": "2020-06-01",
                        "end_date": "2021-05-31",
                    },
                ]
            ),
            "start_date and end_date are in different years",
        ),
        (
            pd.DataFrame.from_records(
                [
                    {
                        "entity_id": "123",
                        "report_year": 3021,
                    },
                ]
            ),
            "Attempted to read a non-instant, non-duration table",
        ),
    ],
)
def test_report_year_fixing_bad_values(df, match):
    with pytest.raises(ValueError, match=match):
        FercXBRLSQLiteIOManager.refine_report_year(df, xbrl_years=[2021, 2022])
