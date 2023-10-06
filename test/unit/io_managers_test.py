"""Test Dagster IO Managers."""
import datetime
import json

import hypothesis
import pandas as pd
import pandera as pa
import pytest
import sqlalchemy as sa
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy.exc import IntegrityError, OperationalError

from pudl.io_managers import (
    FercXBRLSQLiteIOManager,
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
        name="artist_view", schema=schema, create_database_schema=False
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
    db_path = tmp_path / "pudl.sqlite"

    # Create the database and schemas
    engine = sa.create_engine(f"sqlite:///{db_path}")
    md = test_pkg.to_sql()
    md.create_all(engine)
    return PudlSQLiteIOManager(base_dir=tmp_path, db_name="pudl", package=test_pkg)


def test_error_when_handling_view_without_metadata(pudl_sqlite_io_manager_fixture):
    """Make sure an error is thrown when a user creates a view without metadata."""
    asset_key = "track_view"
    sql_stmt = "CREATE VIEW track_view AS SELECT * FROM track;"
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        pudl_sqlite_io_manager_fixture.handle_output(output_context, sql_stmt)


@pytest.mark.skip(reason="SQLAlchemy is not finding the view. Debug or remove.")
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
    # print(input_context)
    # This is failing, not sure why
    # sqlalchemy.exc.InvalidRequestError: Could not reflect: requested table(s) not available in
    # Engine(sqlite:////private/var/folders/pg/zrqnq8l113q57bndc5__h2640000gn/
    # # T/pytest-of-nelsonauner/pytest-38/test_handling_view_with_metada0/pudl.sqlite): (artist_view)
    pudl_sqlite_io_manager_fixture.load_input(input_context)


def test_error_when_reading_view_without_metadata(pudl_sqlite_io_manager_fixture):
    """Make sure and error is thrown when a user loads a view without metadata."""
    asset_key = "track_view"
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    with pytest.raises(ValueError):
        pudl_sqlite_io_manager_fixture.load_input(input_context)


def test_ferc_xbrl_sqlite_io_manager_dedupes(mocker, tmp_path):
    db_path = tmp_path / "test_db.sqlite"
    datapackage = json.dumps(
        {
            "profile": "tabular-data-package",
            "name": "test_db",
            "title": "Ferc1 data extracted from XBRL filings",
            "resources": [
                {
                    "path": f"sqlite:///{db_path}",
                    "profile": "tabular-data-resource",
                    "name": "test_table_instant",
                    "format": "sqlite",
                    "mediatype": "application/vnd.sqlite3",
                    "schema": {
                        "fields": [
                            {
                                "name": "entity_id",
                                "type": "string",
                            },
                            {
                                "name": "utility_type_axis",
                                "type": "string",
                            },
                            {
                                "name": "filing_name",
                                "type": "string",
                            },
                            {
                                "name": "publication_time",
                                "type": "datetime",
                            },
                            {
                                "name": "date",
                                "type": "date",
                            },
                            {
                                "name": "str_factoid",
                                "type": "string",
                            },
                        ],
                        "primary_key": [
                            "entity_id",
                            "filing_name",
                            "publication_time",
                            "date",
                            "utility_type_axis",
                        ],
                    },
                }
            ],
        }
    )

    datapackage_path = tmp_path / "test_db_datapackage.json"
    with datapackage_path.open("w") as f:
        f.write(datapackage)

    df = pd.DataFrame.from_records(
        [
            {
                "entity_id": "C000001",
                "utility_type_axis": "electric",
                "filing_name": "Utility_Co_0001",
                "date": datetime.date(2021, 12, 31),
                "publication_time": datetime.datetime(2022, 2, 1, 0, 0, 0),
                "str_factoid": "original 2021 EOY value",
            },
            {
                "entity_id": "C000001",
                "utility_type_axis": "electric",
                "filing_name": "Utility_Co_0002",
                "date": datetime.date(2021, 12, 31),
                "publication_time": datetime.datetime(2022, 2, 1, 1, 1, 1),
                "str_factoid": "updated 2021 EOY value",
            },
        ]
    )

    id_table = pd.DataFrame.from_records(
        [
            {"filing_name": "Utility_Co_0001", "report_year": 2021},
            {"filing_name": "Utility_Co_0002", "report_year": 2021},
        ]
    )

    conn = sa.create_engine(f"sqlite:///{db_path}")
    df.to_sql("test_table_instant", conn)
    id_table.to_sql("identification_001_duration", conn)

    input_context = build_input_context(
        asset_key=AssetKey("test_table_instant"),
        resources={
            "dataset_settings": mocker.MagicMock(
                ferc1=mocker.MagicMock(xbrl_years=[2021])
            )
        },
    )
    io_manager = FercXBRLSQLiteIOManager(base_dir=tmp_path, db_name="test_db")
    observed_table = io_manager.load_input(input_context)

    assert len(observed_table) == 1
    assert observed_table.str_factoid.to_numpy().item() == "updated 2021 EOY value"


example_schema = pa.DataFrameSchema(
    {
        "entity_id": pa.Column(str),
        "date": pa.Column("datetime64[ns]"),
        "utility_type": pa.Column(
            str, pa.Check.isin(["electric", "gas", "total", "other"])
        ),
        "publication_time": pa.Column("datetime64[ns]"),
        "int_factoid": pa.Column(int),
        "float_factoid": pa.Column(float),
        "str_factoid": pa.Column("str"),
    }
)


@hypothesis.given(example_schema.strategy(size=3))
def test_get_unique_row_per_context(df):
    context_cols = ["entity_id", "date", "utility_type"]
    deduped = FercXBRLSQLiteIOManager.use_latest_filing_for_context(df, context_cols)
    example_schema.validate(deduped)

    # every post-deduplication row exists in the original rows
    assert (deduped.merge(df, how="left", indicator=True)._merge != "left_only").all()
    # for every [entity_id, utility_type, date] - th"true"e is only one row
    assert (~deduped.duplicated(subset=context_cols)).all()
    # for every *context* in the input there is a corresponding row in the output
    original_contexts = df.groupby(context_cols, as_index=False).last()
    paired_by_context = original_contexts.merge(
        deduped, on=context_cols, how="outer", suffixes=["_in", "_out"], indicator=True
    ).set_index(context_cols)
    assert (paired_by_context._merge == "both").all()

    # for every row in the output - its publication time is greater than or equal to all of the other ones for that [entity_id, utility_type, date] in the input data
    assert (
        paired_by_context["publication_time_out"]
        >= paired_by_context["publication_time_in"]
    ).all()


def test_latest_filing():
    first_2021_filing = [
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2020-12-31",
            "publication_time": "2022-02-02T01:02:03Z",
            "factoid_1": 10.0,
            "factoid_2": 20.0,
        },
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2021-12-31",
            "publication_time": "2022-02-02T01:02:03Z",
            "factoid_1": 11.0,
            "factoid_2": 21.0,
        },
    ]

    second_2021_filing = [
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2020-12-31",
            "publication_time": "2022-02-02T01:05:03Z",
            "factoid_1": 10.1,
            "factoid_2": 20.1,
        },
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2021-12-31",
            "publication_time": "2022-02-02T01:05:03Z",
            "factoid_1": 11.1,
            "factoid_2": 21.1,
        },
    ]

    first_2022_filing = [
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2021-12-31",
            "publication_time": "2023-04-02T01:05:03Z",
            "factoid_1": 110.0,
            "factoid_2": 120.0,
        },
        {
            "entity_id": "C123456",
            "utility_type": "electric",
            "date": "2022-12-31",
            "publication_time": "2023-04-02T01:05:03Z",
            "factoid_1": 111.0,
            "factoid_2": 121.0,
        },
    ]

    test_df = pd.DataFrame.from_records(
        first_2021_filing + first_2022_filing + second_2021_filing
    ).convert_dtypes()
    context_cols = ["entity_id", "date", "utility_type"]
    deduped = FercXBRLSQLiteIOManager.use_latest_filing_for_context(
        test_df, context_cols
    )

    # for every [entity_id, utility_type, date] - there is only one row
    assert (~deduped.duplicated(subset=context_cols)).all()

    # for every *context* in the input there is a corresponding row in the output
    input_contexts = test_df.groupby(context_cols, as_index=False).last()
    inputs_paired_with_outputs = input_contexts.merge(
        deduped, on=context_cols, how="outer", suffixes=["_in", "_out"], indicator=True
    ).set_index(context_cols)
    assert (inputs_paired_with_outputs._merge == "both").all()

    # for every row in the output - its publication time is greater than or equal to all of the other ones for that [entity_id, utility_type, date] in the input data
    assert (
        inputs_paired_with_outputs["publication_time_out"]
        >= inputs_paired_with_outputs["publication_time_in"]
    ).all()
