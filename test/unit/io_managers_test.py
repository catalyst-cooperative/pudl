"""Test Dagster IO Managers."""
import datetime
import json
import tempfile
import unittest
from pathlib import Path
from sqlite3 import OperationalError

import alembic.config
import hypothesis
import pandas as pd
import pandera
import pytest
import sqlalchemy as sa
from dagster import AssetKey, build_input_context, build_output_context
from sqlalchemy.exc import IntegrityError

from pudl.io_managers import (
    FercXBRLSQLiteIOManager,
    ForeignKeyError,
    PudlSQLiteIOManager,
)
from pudl.metadata.classes import Field, ForeignKey, Package, Resource, Schema

TEST_PACKAGE = Package(
    name="music",
    resources=[
        Resource(
            name="artist",
            schema=Schema(
                primary_key=["artistid"],
                fields=[
                    Field(name="artistid", type="integer"),
                    Field(
                        name="artistname", type="string", constraints={"required": True}
                    ),
                ],
            ),
        ),
        Resource(
            name="artist_view",
            schema=Schema(
                primary_key=["artistid"],
                fields=[
                    Field(name="artistid", type="integer"),
                    Field(
                        name="artistname", type="string", constraints={"required": True}
                    ),
                ],
            ),
            create_database_schema=False,
        ),
        Resource(
            name="track",
            schema=Schema(
                primary_key=["trackid"],
                foreign_keys=[
                    ForeignKey(
                        fields=["trackartist"],
                        reference={"resource": "artist", "fields": ["artistid"]},
                    ),
                ],
                fields=[
                    Field(name="trackid", type="integer"),
                    Field(
                        name="trackname", type="string", constraints={"required": True}
                    ),
                    Field(name="trackartist", type="integer"),
                ],
            ),
        ),
    ],
)
"""Sample package with artist, artist_view and track resources."""


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

    pkg = Package.from_resource_ids()
    PudlSQLiteIOManager(base_dir=tmp_path, db_name="pudl", package=pkg)

    # all we care about is that it didn't raise an error
    assert True


# TODO(rousik): is there difference between fake_sqlite_io_manager
# and real sqlite_io_manager in terms of functionality?!!
class PudlSQLiteIOManagerTest(unittest.TestCase):
    """Tests for the PudlSQLiteIOManager."""

    def setUp(self):
        """Creates temporary database with a fake schema."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".sqlite")
        engine = sa.create_engine(f"sqlite:///{self.temp_db.name}")
        md = TEST_PACKAGE.to_sql()
        md.create_all(engine)
        self.io_manager = PudlSQLiteIOManager(
            base_dir=Path(self.temp_db.name).parent,
            db_name=Path(self.temp_db.name).stem,
            package=TEST_PACKAGE,
        )

    def test_replace_on_insert(self):
        """Tests that two runs of the same asset overwrite existing contents."""
        artist_df = pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]})
        output_context = build_output_context(asset_key=AssetKey("artist"))
        input_context = build_input_context(asset_key=AssetKey("artist"))

        # Write then read.
        self.io_manager.handle_output(output_context, artist_df)
        read_df = self.io_manager.load_input(input_context)
        pd.testing.assert_frame_equal(artist_df, read_df, check_dtype=False)
        # check_dtype=False, because int64 != Int64. /o\

        # Rerunning the asset overrwrites contents, leaves only
        # one artist in the database.
        new_artist_df = pd.DataFrame({"artistid": [2], "artistname": ["Cxtxlyst"]})
        self.io_manager.handle_output(output_context, new_artist_df)
        read_df = self.io_manager.load_input(input_context)
        pd.testing.assert_frame_equal(new_artist_df, read_df, check_dtype=False)

    def test_error_when_handling_view_without_metadata(self):
        """Make sure an error is thrown when a user creates a view without metadata."""
        with self.assertRaises(ValueError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("track_view")),
                "CREATE VIEW track_view AS SELECT * FROM track;",
            )

    @unittest.skip("SQLAlchemy is not finding the view. Debug or remove.")
    def test_handling_view_with_metadata(self):
        """Make sure an users can create and load views when it has metadata."""
        # Create some sample data
        self.io_manager.handle_output(
            build_output_context(asset_key=AssetKey("artist")),
            pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]}),
        )

        # create the view
        self.io_manager.handle_output(
            build_output_context(asset_key=AssetKey("artist_view")),
            "CREATE VIEW artist_view AS SELECT * FROM artist;",
        )
        # Read the view
        self.io_manager.load_input(
            build_input_context(asset_key=AssetKey("artist_view"))
        )
        # This is failing, not sure why
        # sqlalchemy.exc.InvalidRequestError: Could not reflect: requested table(s) not available in
        # Engine(sqlite:////private/var/folders/pg/zrqnq8l113q57bndc5__h2640000gn/
        # # T/pytest-of-nelsonauner/pytest-38/test_handling_view_with_metada0/pudl.sqlite): (artist_view)

    def test_error_when_reading_view_without_metadata(self):
        """Make sure and error is thrown when a user loads a view without metadata."""
        with self.assertRaises(ValueError):
            self.io_manager.load_input(
                build_input_context(asset_key=AssetKey("track_view"))
            )

    @unittest.expectedFailure
    def test_foreign_key_failure(self):
        """Ensure ForeignKeyErrors are raised when there are foreign key errors."""
        self.io_manager.handle_output(
            build_output_context(asset_key=AssetKey("artist")),
            pd.DataFrame({"artistid": [1], "artistname": ["Co-op Mop"]}),
        )
        self.io_manager.handle_output(
            build_output_context(asset_key=AssetKey("track")),
            pd.DataFrame(
                {"trackid": [1], "trackname": ["FERC Ya!"], "trackartist": [2]}
            ),
        )
        with self.assertRaises(ForeignKeyError) as cm:
            self.io_manager.check_foreign_keys()
        self.assertEqual(
            ForeignKeyError(
                child_table="track",
                parent_table="artist",
                foreign_key="(artistid)",
                rowids=[1],
            ),
            cm.exception,
        )

    @unittest.skip("Inserting with unknown columns should fail but it is not?!")
    def test_extra_column_error(self):
        """Ensure an error is thrown when there is an extra column in the dataframe."""
        with self.assertRaises(OperationalError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("artist")),
                pd.DataFrame(
                    {"artistid": [1], "artistname": ["Co-op Mop"], "artistmanager": [1]}
                ),
            )
            # TODO(rousik): The above should not pass, but it seems to be? Unsure why.
            read_df = self.io_manager.load_input(
                build_input_context(asset_key=AssetKey("artist"))
            )
            self.assertListEqual(["artistid", "artistname"], list(read_df.columns))
            print(read_df)

    def test_missing_column_error(self):
        """Ensure an error is thrown when a dataframe is missing a column in the schema."""
        with self.assertRaises(ValueError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("artist")),
                pd.DataFrame({"artistid": [1]}),
            )

    def test_nullable_column_error(self):
        """Ensure an error is thrown when a non nullable column is missing data."""
        with self.assertRaises(IntegrityError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("artist")),
                pd.DataFrame({"artistid": [1, 2], "artistname": ["Co-op Mop", pd.NA]}),
            )

    def test_null_primary_key_column_error(self):
        """Ensure an error is thrown when a primary key contains a nullable value."""
        with self.assertRaises(ValueError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("artist")),
                pd.DataFrame(
                    {"artistid": [1, pd.NA], "artistname": ["Co-op Mop", "Cxtxlyst"]}
                ),
            )

    def test_primary_key_column_error(self):
        """Ensure an error is thrown when a primary key is violated."""
        with self.assertRaises(ValueError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("artist")),
                pd.DataFrame(
                    {"artistid": [1, 1], "artistname": ["Co-op Mop", "Cxtxlyst"]}
                ),
            )

    def test_incorrect_type_error(self):
        """Ensure an error is thrown when dataframe type doesn't match the table schema."""
        with self.assertRaises(ValueError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("artist")),
                pd.DataFrame({"artistid": ["abc"], "artistname": ["Co-op Mop"]}),
            )

    def test_missing_schema_error(self):
        """Test a ValueError is raised when a table without a schema is loaded."""
        with self.assertRaises(ValueError):
            self.io_manager.handle_output(
                build_output_context(asset_key=AssetKey("venues")),
                pd.DataFrame({"venueid": [1], "venuename": "Vans Dive Bar"}),
            )


def test_ferc_xbrl_sqlite_io_manager_dedupes(mocker, tmp_path):
    db_path = tmp_path / "test_db.sqlite"
    # fake datapackage descriptor just to see if we can find the primary keys -
    # lots of optional stuff dropped.
    datapackage = json.dumps(
        {
            "name": "test_db",
            "title": "Ferc1 data extracted from XBRL filings",
            "resources": [
                {
                    "path": f"{db_path}",
                    "name": "test_table_instant",
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

    conn = sa.create_engine(f"sqlite:///{db_path}")
    df.to_sql("test_table_instant", conn)
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


example_schema = pandera.DataFrameSchema(
    {
        "entity_id": pandera.Column(
            str, pandera.Check.isin("C0123456789"), nullable=False
        ),
        "date": pandera.Column("datetime64[ns]", nullable=False),
        "utility_type": pandera.Column(
            str,
            pandera.Check.isin(["electric", "gas", "total", "other"]),
            nullable=False,
        ),
        "publication_time": pandera.Column("datetime64[ns]", nullable=False),
        "int_factoid": pandera.Column(int),
        "float_factoid": pandera.Column(float),
        "str_factoid": pandera.Column(str),
    }
)


# ridiculous deadline - dataframe generation is always slow and sometimes
# *very* slow
@pytest.mark.slow
@hypothesis.settings(print_blob=True, deadline=2_000)
@hypothesis.given(example_schema.strategy(size=3))
def test_filter_for_freshest_data(df):
    # XBRL context is the identifying metadata for reported values
    xbrl_context_cols = ["entity_id", "date", "utility_type"]
    filing_metadata_cols = ["publication_time", "filing_name"]
    primary_key = xbrl_context_cols + filing_metadata_cols
    deduped = FercXBRLSQLiteIOManager.filter_for_freshest_data(
        df, primary_key=primary_key
    )
    example_schema.validate(deduped)

    # every post-deduplication row exists in the original rows
    assert (deduped.merge(df, how="left", indicator=True)._merge != "left_only").all()
    # for every [entity_id, utility_type, date] - there is only one row
    assert (~deduped.duplicated(subset=xbrl_context_cols)).all()
    # for every *context* in the input there is a corresponding row in the output
    original_contexts = df.groupby(xbrl_context_cols, as_index=False).last()
    paired_by_context = original_contexts.merge(
        deduped,
        on=xbrl_context_cols,
        how="outer",
        suffixes=["_in", "_out"],
        indicator=True,
    ).set_index(xbrl_context_cols)
    hypothesis.note(
        f"Found these contexts ({xbrl_context_cols}) in input data:\n{original_contexts[xbrl_context_cols]}"
    )
    hypothesis.note(f"The freshest data:\n{deduped}")
    hypothesis.note(f"Paired by context:\n{paired_by_context}")
    assert (paired_by_context._merge == "both").all()

    # for every row in the output - its publication time is greater than or equal to all of the other ones for that [entity_id, utility_type, date] in the input data
    assert (
        paired_by_context["publication_time_out"]
        >= paired_by_context["publication_time_in"]
    ).all()


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
