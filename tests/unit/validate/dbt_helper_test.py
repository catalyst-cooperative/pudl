from collections import namedtuple
from io import StringIO
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from pudl.dbt_schema import (
    DbtColumn,
    DbtSchema,
    DbtSource,
    DbtTable,
)
from pudl.scripts.dbt_helper import (
    UpdateResult,
    _calculate_row_counts,
    _combine_row_counts,
    _extract_row_count_partitions,
    _get_existing_row_counts,
    insert_data_source,
    update_row_counts,
    update_table_schema,
    update_tables,
)

# Test helper machinery

GivenExpect = namedtuple("GivenExpect", ["given", "expect"])

TEMPLATE = {
    "data_col": "data",
    "query": "",
    "weight_col": "weight",
}


@pytest.fixture
def schema_factory():
    def _make_schema(
        table_name,
        columns,
        partition_expr=None,
        add_row_count_test=True,
        source_name="pudl",
    ):
        data_tests = None
        if add_row_count_test and partition_expr:
            data_tests = [
                {
                    "check_row_counts_per_partition": {
                        "arguments": {
                            "table_name": table_name,
                            "partition_expr": partition_expr,
                        }
                    }
                }
            ]
        return DbtSchema(
            version=2,
            models=None,
            sources=[
                DbtSource(
                    name=source_name,
                    tables=[
                        DbtTable(
                            name=table_name,
                            data_tests=data_tests,
                            columns=[DbtColumn(name=col) for col in columns],
                        )
                    ],
                )
            ],
        )

    return _make_schema


def with_name(mock, name):
    """Helper function to set the name attribute on a Mock without it getting sucked into the Mock internals (for repr, etc)."""
    mock.name = name
    return mock


# dbt helper helpers


@pytest.mark.parametrize(
    ("table_name", "expected_data_source"),
    [("_core_eia__foo", "eia"), ("out_ferc__bar", "ferc")],
)
def test_insert_data_source(table_name: str, expected_data_source: str, tmp_path: Path):
    """Test that our regex can correctly extract source names from normal-looking table names."""
    observed = insert_data_source(tmp_path, table_name)
    expected = tmp_path / expected_data_source / table_name
    assert observed == expected


@pytest.mark.parametrize("table_name", ["out", "_a_b_c", "", "____"])
def test_insert_data_source_invalid(table_name: str, tmp_path: Path):
    """Test that our regex fails on bogus table names."""
    with pytest.raises(ValueError):
        insert_data_source(tmp_path, table_name)


# row counts


def test__get_existing_row_counts(mocker):
    # Simulate a CSV file in memory
    csv_data = """table_name,partition,row_count
my_table,2023,100
"""
    string_io = StringIO(csv_data)

    # Save reference to the real pandas.read_csv
    real_read_csv = pd.read_csv

    # Patch pd.read_csv in the module under test
    mocker.patch(
        "pudl.scripts.dbt_helper.pd.read_csv",
        side_effect=lambda path, *args, **kwargs: real_read_csv(
            string_io, *args, **kwargs
        ),
    )

    # Call the function
    result = _get_existing_row_counts()

    # Expected output
    expected = pd.DataFrame(
        data={
            "table_name": ["my_table"],
            "partition": ["2023"],
            "row_count": [100],
        },
    ).astype(
        {
            "table_name": "string",
            "partition": "string",
        }
    )

    # Check values and types
    pd.testing.assert_frame_equal(result, expected)

    # Explicitly assert string type
    assert result["partition"].apply(type).eq(str).all()


@pytest.mark.parametrize(
    "data_tests, expected",
    [
        (
            [
                {
                    "check_row_counts_per_partition": {
                        "arguments": {
                            "table_name": "plants",
                            "partition_expr": "report_year",
                        }
                    }
                }
            ],
            ["report_year"],
        ),
        (
            [
                {
                    "check_row_counts_per_partition": {
                        "arguments": {
                            "table_name": "plants",
                            "partition_expr": None,
                        }
                    }
                },
            ],
            [None],
        ),
        (
            [
                {"some_other_test": {}},
            ],
            [],
        ),
        (
            [
                {"test_without_parameters"},
            ],
            [],
        ),
        (
            None,
            [],
        ),
    ],
)
def test__extract_row_count_partitions(data_tests, expected):
    table = DbtTable(
        name="plants",
        columns=[DbtColumn(name="some_column")],
        data_tests=data_tests,
    )
    assert _extract_row_count_partitions(table) == expected


ROW_COUNT_TEST_CASES = [
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\n",
            "table_name": "test_foo__table_name",
            "partition_expr": "report_year",
            "new_counts_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\ntest_foo__table_name,2021,120\n",
            "clobber": True,
            "has_test": True,
            "should_write": True,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\ntest_foo__table_name,2021,120\n",
            "result": UpdateResult(
                success=True,
                message="Successfully updated row counts for test_foo__table_name, partitioned by report_year.",
            ),
        },
    ),
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\n",
            "table_name": "test_foo__table_name",
            "partition_expr": "report_year",
            "new_counts_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\ntest_foo__table_name,2021,120\n",
            "clobber": False,
            "has_test": True,
            "should_write": False,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\ntest_foo__table_name,2021,120\n",
            "result": UpdateResult(
                success=False,
                message="Row counts for test_foo__table_name already exist. Use clobber to overwrite.",
            ),
        },
    ),
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\n",
            "table_name": "test_foo__table_name",
            "partition_expr": "report_year",
            "new_counts_csv": None,  # no new counts if test is missing
            "clobber": False,
            "has_test": False,
            "should_write": False,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\n",
            "result": UpdateResult(
                success=False,
                message="Row counts exist for test_foo__table_name, but no row count test is defined. Use clobber to remove.",
            ),
        },
    ),
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\n",
            "table_name": "test_foo__table_name",
            "partition_expr": "report_year",
            "new_counts_csv": None,  # no new counts if test is missing
            "clobber": False,
            "has_test": False,
            "should_write": False,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\ntest_foo__table_name,2020,100\n",
            "result": UpdateResult(
                success=False,
                message="Row counts exist for test_foo__table_name, but no row count test is defined. Use clobber to remove.",
            ),
        },
    ),
]


@pytest.mark.parametrize("case", ROW_COUNT_TEST_CASES)
def test_update_row_counts(case, schema_factory, mocker, tmp_path):
    given = case.given
    expect = case.expect

    existing_df = pd.read_csv(StringIO(given["existing_csv"]))

    if given["new_counts_csv"]:
        new_df = pd.read_csv(StringIO(given["new_counts_csv"]))
    else:
        new_df = pd.DataFrame(columns=["table_name", "partition", "row_count"])

    schema = schema_factory(
        table_name=given["table_name"],
        columns=["report_year", "id"],
        partition_expr=given["partition_expr"],
        add_row_count_test=given["has_test"],
    )

    mocker.patch(
        "pudl.scripts.dbt_helper._get_existing_row_counts", return_value=existing_df
    )
    mocker.patch("pudl.scripts.dbt_helper._calculate_row_counts", return_value=new_df)
    mock_write = mocker.patch("pudl.scripts.dbt_helper._write_row_counts")
    mocker.patch("pudl.scripts.dbt_helper.DbtSchema.from_yaml", return_value=schema)
    mocker.patch(
        "pudl.scripts.dbt_helper._extract_row_count_partitions",
        return_value=[given["partition_expr"]] if given["has_test"] else [],
    )

    # NOTE (2026-05-08): since we mock out the DbtSchema.from_yaml, and dbt_root
    # is only used for reading the schema to check if there is a row count test
    # to update, we can give any value for dbt_root here and the code won't
    # care.
    result = update_row_counts(
        table_name=given["table_name"],
        dbt_root=tmp_path,
        clobber=given["clobber"],
    )

    # Assert the expected result object
    assert result == expect["result"]

    # If we expect a write, assert what would be written
    if given["should_write"]:
        expected_df = pd.read_csv(StringIO(expect["expected_csv"]))
        mock_write.assert_called_once()
        pd.testing.assert_frame_equal(
            mock_write.call_args[0][0]
            .reset_index(drop=True)
            .astype({"partition": "int", "row_count": "int"}),
            expected_df.reset_index(drop=True),
            check_dtype=False,
        )
    else:
        mock_write.assert_not_called()


COMBINE_ROW_COUNT_TEST_CASES = [
    GivenExpect(
        given={
            "existing_df": pd.DataFrame(
                {
                    "table_name": ["foo"],
                    "partition": ["2020"],
                    "row_count": [100],
                }
            ),
            "new_df": pd.DataFrame(
                {
                    "table_name": ["foo", "foo"],
                    "partition": ["2020", "2021"],
                    "row_count": [100, 120],
                }
            ),
        },
        expect={
            "expected_df": pd.DataFrame(
                {
                    "table_name": ["foo", "foo"],
                    "partition": ["2020", "2021"],
                    "row_count": [100, 120],
                }
            )
            .sort_values(["table_name", "partition"])
            .reset_index(drop=True),
        },
    ),
    GivenExpect(
        given={
            "existing_df": pd.DataFrame(
                {
                    "table_name": ["foo"],
                    "partition": ["2020"],
                    "row_count": [99],
                }
            ),
            "new_df": pd.DataFrame(
                {
                    "table_name": ["foo"],
                    "partition": ["2020"],
                    "row_count": [100],
                }
            ),
        },
        expect={
            "expected_df": pd.DataFrame(
                {
                    "table_name": ["foo"],
                    "partition": ["2020"],
                    "row_count": [100],
                }
            ),
        },
    ),
]


@pytest.mark.parametrize("case", COMBINE_ROW_COUNT_TEST_CASES)
def test__combine_row_counts(case):
    # Given
    existing_df = case.given["existing_df"]
    new_df = case.given["new_df"]

    # When
    result_df = _combine_row_counts(existing_df, new_df).reset_index(drop=True)

    # Then
    pd.testing.assert_frame_equal(
        result_df.sort_values(["table_name", "partition"]).reset_index(drop=True),
        case.expect["expected_df"],
    )


CALCULATE_ROW_COUNTS_CASES = [
    GivenExpect(
        given={
            "table_name": "foo",
            "partition_expr": "report_year",
            "mocked_path": "fake.parquet",
            "mocked_duckdb_df": pd.DataFrame(
                {
                    "partition": ["2020", "2021"],
                    "row_count": [10, 15],
                }
            ),
        },
        expect={
            "expected_df": pd.DataFrame(
                {
                    "table_name": ["foo", "foo"],
                    "partition": ["2020", "2021"],
                    "row_count": [10, 15],
                }
            ).astype(
                {
                    "table_name": "string",
                    "partition": "string",
                }
            ),
        },
    ),
]


@pytest.mark.parametrize("case", CALCULATE_ROW_COUNTS_CASES)
def test__calculate_row_counts_(case, mocker):
    mock_sql = mocker.patch("pudl.scripts.dbt_helper.duckdb.sql")
    mock_sql.return_value.df.return_value = case.given["mocked_duckdb_df"]

    # When
    result = _calculate_row_counts(
        case.given["table_name"], case.given["partition_expr"]
    )

    # Then: Verify output
    pd.testing.assert_frame_equal(
        result.sort_values("partition").reset_index(drop=True),
        case.expect["expected_df"].sort_values("partition").reset_index(drop=True),
    )

    # Then: Verify SQL input
    mock_sql.assert_called_once()
    sql_arg = mock_sql.call_args[0][0]
    assert case.given["partition_expr"] in sql_arg, (
        f"The partition column '{case.given['partition_expr']}' "
        f"was not used in the SQL: {sql_arg}"
    )
    assert "COUNT(*)" in sql_arg.upper(), f"SQL is missing a row count: {sql_arg}"


GENERATE_QUANTILE_BOUNDS = [
    GivenExpect(
        given=[dict(title="Hi only", hi_q=0, hi_bound=1, **TEMPLATE)],
        expect=[
            {
                "expect_quantile_constraints": {
                    "row_condition": "",
                    "weight_column": "weight",
                    "constraints": [
                        {
                            "quantile": 0,
                            "max_value": 1,
                        }
                    ],
                }
            }
        ],
    ),
    GivenExpect(
        given=[dict(title="Low only", low_q=0, low_bound=1, **TEMPLATE)],
        expect=[
            {
                "expect_quantile_constraints": {
                    "row_condition": "",
                    "weight_column": "weight",
                    "constraints": [
                        {
                            "quantile": 0,
                            "min_value": 1,
                        }
                    ],
                }
            }
        ],
    ),
    GivenExpect(
        given=[
            dict(title="Both", low_q=0, low_bound=1, hi_q=2, hi_bound=3, **TEMPLATE)
        ],
        expect=[
            {
                "expect_quantile_constraints": {
                    "row_condition": "",
                    "weight_column": "weight",
                    "constraints": [
                        {
                            "quantile": 0,
                            "min_value": 1,
                        },
                        {
                            "quantile": 2,
                            "max_value": 3,
                        },
                    ],
                }
            },
        ],
    ),
    GivenExpect(
        given=[
            dict(title="Median", low_q=0, low_bound=1, hi_q=0, hi_bound=3, **TEMPLATE)
        ],
        expect=[
            {
                "expect_quantile_constraints": {
                    "row_condition": "",
                    "weight_column": "weight",
                    "constraints": [
                        {
                            "quantile": 0,
                            "min_value": 1,
                            "max_value": 3,
                        }
                    ],
                }
            },
        ],
    ),
    GivenExpect(
        given=[
            dict(
                title="One-tailed",
                low_q=False,
                low_bound=False,
                hi_q=0,
                hi_bound=1,
                **TEMPLATE,
            )
        ],
        expect=[
            {
                "expect_quantile_constraints": {
                    "row_condition": "",
                    "weight_column": "weight",
                    "constraints": [
                        {
                            "quantile": 0,
                            "max_value": 1,
                        }
                    ],
                }
            }
        ],
    ),
]


@pytest.mark.parametrize(
    ["partition_definition", "test_data", "old_row_counts", "expected_row_counts"],
    [
        # The normal case -- just update with new row counts.
        (
            "partition_expr: year",
            pd.read_csv(
                StringIO(
                    "year, fake_data\n"
                    "2020, 1.23456\n"
                    "2021, 2.34567\n"
                    "2021, 4.56789\n"
                    "2022, 3.14159\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,2020,1\n"
                    "test_source__table_name,2021,1\n"
                    "test_source__table_name,2022,1\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,2020,1\n"
                    "test_source__table_name,2021,2\n"
                    "test_source__table_name,2022,1\n"
                )
            ),
        ),
        # A partition column with null values
        (
            "partition_expr: year",
            pd.read_csv(
                StringIO(
                    "year, fake_data\n"
                    "2020, 1.23456\n"
                    "2021, 2.34567\n"
                    "2021, 4.56789\n"
                    ", 3.14159\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,2020,1\n"
                    "test_source__table_name,2021,1\n"
                    "test_source__table_name,2022,1\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,2020,1\n"
                    "test_source__table_name,2021,2\n"
                    "test_source__table_name,,1\n"
                )
            ),
        ),
        # No partitioning -- count up all rows in the table.
        (
            "",
            pd.read_csv(
                StringIO(
                    "year,state,fake_data\n"
                    "2020,CA,1.23456\n"
                    "2021,TX,2.34567\n"
                    "2021,FL,4.56789\n"
                    "2022,CA,3.14159\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,2020,1\n"
                    "test_source__table_name,2021,2\n"
                    "test_source__table_name,2022,1\n"
                )
            ),
            pd.read_csv(
                StringIO("table_name,partition,row_count\ntest_source__table_name,,4\n")
            ),
        ),
        # Change which column we're partitioning by.
        (
            "partition_expr: state",
            pd.read_csv(
                StringIO(
                    "year,state,fake_data\n"
                    "2020,CA,1.23456\n"
                    "2021,TX,2.34567\n"
                    "2021,FL,4.56789\n"
                    "2022,CA,3.14159\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,2020,1\n"
                    "test_source__table_name,2021,2\n"
                    "test_source__table_name,2022,1\n"
                )
            ),
            pd.read_csv(
                StringIO(
                    "table_name,partition,row_count\n"
                    "test_source__table_name,CA,2\n"
                    "test_source__table_name,FL,1\n"
                    "test_source__table_name,TX,1\n"
                )
            ),
        ),
    ],
)
def test_update_table_row_counts_clobber(
    partition_definition,
    test_data,
    old_row_counts,
    expected_row_counts,
    tmp_path,
    mocker,
    pudl_test_paths,
):
    # make test data
    test_schema = f"""
sources:
  - name: pudl_test
    tables:
      - name: test_source__table_name
        data_tests:
          - check_row_counts_per_partition:
              arguments:
                table_name: test_source__table_name
                {partition_definition}
        columns:
          - name: year
          - name: state
          - name: fake_data
"""
    # set up test paths (patch dbt_dir to a tmp path, add schema and rowcounts directories)
    # don't need to make temporary PUDL_OUT because we do that already in conftest.py
    dbt_dir = tmp_path / "dbt"
    schema_path = (
        dbt_dir / "models" / "source" / "test_source__table_name" / "schema.yml"
    )
    row_count_csv_path = dbt_dir / "seeds" / "etl_full_row_counts.csv"
    parquet_path = pudl_test_paths.parquet_path("test_source__table_name")

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    row_count_csv_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # write out test data to disk
    test_data.to_parquet(parquet_path)
    old_row_counts.to_csv(row_count_csv_path, index=False)
    with schema_path.open("w") as f:
        f.write(test_schema)

    # patch out PUDL_DBT_PATH so we use our lovely test schema + rowcounts
    mocker.patch("pudl.scripts.dbt_helper.PUDL_DBT_PATH", new=dbt_dir)
    # patch out ALL_TABLES so that we're allowed to run tests
    mocker.patch("pudl.scripts.dbt_helper.ALL_TABLES", new=["test_source__table_name"])
    runner = CliRunner()

    logger_mock = mocker.patch("pudl.scripts.dbt_helper.logger.info")
    runner.invoke(
        update_tables,
        [
            "test_source__table_name",
            "--clobber",
            "--row-counts",
        ],
    )
    assert (
        "Successfully updated row counts for test_source__table_name"
        in logger_mock.call_args[0][0]
    )

    # read out data & compare
    observed_row_counts = pd.read_csv(row_count_csv_path)
    index = ["table_name", "partition"]
    pd.testing.assert_frame_equal(
        observed_row_counts.set_index(index).sort_index(),
        expected_row_counts.set_index(index).sort_index(),
    )


# update-tables --schema

# 2026-05: this is a real table because we're using real PUDL_PACKAGE.
# future us could mock out / dependency-inject PUDL_PACKAGE and then we could use a fake table.
PUDL_TABLE = "core_pudl__codes_datasources"


def test_update_schema_no_human(tmp_path):
    """Make sure we handle schema updates properly when there is no human-coded file."""
    schema_inputs_path = tmp_path / "schema_inputs" / "pudl" / PUDL_TABLE
    schema_inputs_path.mkdir(parents=True)
    output_path = tmp_path / "models" / "pudl" / PUDL_TABLE
    output_path.mkdir(parents=True)

    update_table_schema(PUDL_TABLE, tmp_path)

    observed_schema = DbtSchema.from_yaml(output_path / "schema.yml")
    expected_schema = DbtSchema.from_table_name(PUDL_TABLE)

    assert observed_schema == expected_schema


def test_update_schema_empty_human(tmp_path):
    """Make sure we handle schema updates properly when the human-coded file is empty."""
    schema_inputs_path = tmp_path / "schema_inputs" / "pudl" / PUDL_TABLE
    schema_inputs_path.mkdir(parents=True)
    (schema_inputs_path / "schema.human.yml").touch()

    output_path = tmp_path / "models" / "pudl" / PUDL_TABLE
    output_path.mkdir(parents=True)

    update_table_schema(PUDL_TABLE, tmp_path)

    observed_schema = DbtSchema.from_yaml(output_path / "schema.yml")
    expected_schema = DbtSchema.from_table_name(PUDL_TABLE)

    assert observed_schema == expected_schema


def test_update_schema(tmp_path):
    """Make sure we handle schema updates properly when the human-coded file has something normal in it."""
    schema_inputs_path = tmp_path / "schema_inputs" / "pudl" / PUDL_TABLE
    schema_inputs_path.mkdir(parents=True)

    human_yaml = f"""
version: 2
sources:
  - name: pudl
    tables:
      - name: {PUDL_TABLE}
        data_tests:
          - fake_table_test:
              arguments:
                arg: true
        columns:
          - name: datasource
            data_tests:
              - fake_column_test:
                  arguments:
                    - arg: true
    """
    with (schema_inputs_path / "schema.human.yml").open("w") as f:
        f.write(human_yaml)

    output_path = tmp_path / "models" / "pudl" / PUDL_TABLE
    output_path.mkdir(parents=True)

    update_table_schema(PUDL_TABLE, tmp_path)

    observed_schema = DbtSchema.from_yaml(output_path / "schema.yml")
    expected_table = DbtTable.from_table_name(PUDL_TABLE)
    expected_table.data_tests = [{"fake_table_test": {"arguments": {"arg": True}}}]
    expected_table.columns[0].data_tests = [
        {"fake_column_test": {"arguments": [{"arg": True}]}}
    ]
    expected_schema = DbtSchema(sources=[DbtSource(tables=[expected_table])])
    assert observed_schema == expected_schema
