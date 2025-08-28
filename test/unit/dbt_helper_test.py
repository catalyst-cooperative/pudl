import unittest
from collections import namedtuple
from dataclasses import dataclass
from io import StringIO

import pandas as pd
import pytest
from click.testing import CliRunner

from pudl.scripts.dbt_helper import (
    DbtColumn,
    DbtSchema,
    DbtSource,
    DbtTable,
    UpdateResult,
    _calculate_row_counts,
    _combine_row_counts,
    _extract_row_count_partitions,
    _get_existing_row_counts,
    _get_local_table_path,
    _get_model_path,
    _schema_diff_summary,
    get_data_source,
    schema_has_removals_or_modifications,
    update_row_counts,
    update_table_schema,
    update_tables,
)
from pudl.workspace.setup import PudlPaths

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


def test_get_data_source():
    assert get_data_source("_core_eia__some_table_name") == "eia"
    assert get_data_source("another_dude__omfg") == "dude"


def test__get_local_table_path(mocker):
    mock_pudlpaths = mocker.Mock()
    mocker.patch("pudl.scripts.dbt_helper.PudlPaths").return_value = mock_pudlpaths
    _get_local_table_path(mocker.sentinel.table)
    mock_pudlpaths.parquet_path.assert_called_once_with(mocker.sentinel.table)


def test__get_model_path():
    assert "models" in str(_get_model_path("", ""))


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
            "existing_csv": "table_name,partition,row_count\nfoo,2020,100\n",
            "table_name": "foo",
            "partition_expr": "report_year",
            "new_counts_csv": "table_name,partition,row_count\nfoo,2020,100\nfoo,2021,120\n",
            "clobber": True,
            "has_test": True,
            "should_write": True,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\nfoo,2020,100\nfoo,2021,120\n",
            "result": UpdateResult(
                success=True,
                message="Successfully updated row counts for foo, partitioned by report_year.",
            ),
        },
    ),
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\nfoo,2020,100\n",
            "table_name": "foo",
            "partition_expr": "report_year",
            "new_counts_csv": "table_name,partition,row_count\nfoo,2020,100\nfoo,2021,120\n",
            "clobber": False,
            "has_test": True,
            "should_write": False,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\nfoo,2020,100\nfoo,2021,120\n",
            "result": UpdateResult(
                success=False,
                message="Row counts for foo already exist. Use clobber to overwrite.",
            ),
        },
    ),
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\nfoo,2020,100\n",
            "table_name": "foo",
            "partition_expr": "report_year",
            "new_counts_csv": None,  # no new counts if test is missing
            "clobber": False,
            "has_test": False,
            "should_write": False,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\nfoo,2020,100\n",
            "result": UpdateResult(
                success=False,
                message="Row counts exist for foo, but no row count test is defined. Use clobber to remove.",
            ),
        },
    ),
    GivenExpect(
        given={
            "existing_csv": "table_name,partition,row_count\nfoo,2020,100\n",
            "table_name": "foo",
            "partition_expr": "report_year",
            "new_counts_csv": None,  # no new counts if test is missing
            "clobber": False,
            "has_test": False,
            "should_write": False,
        },
        expect={
            "expected_csv": "table_name,partition,row_count\nfoo,2020,100\n",
            "result": UpdateResult(
                success=False,
                message="Row counts exist for foo, but no row count test is defined. Use clobber to remove.",
            ),
        },
    ),
]


@pytest.mark.parametrize("case", ROW_COUNT_TEST_CASES)
def test_update_row_counts(case, schema_factory, mocker):
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
    mock_model_path = mocker.patch("pudl.scripts.dbt_helper._get_model_path")
    mock_model_path.return_value.__truediv__.return_value = "fake/schema.yml"
    mocker.patch("pudl.scripts.dbt_helper.DbtSchema.from_yaml", return_value=schema)
    mocker.patch(
        "pudl.scripts.dbt_helper._extract_row_count_partitions",
        return_value=[given["partition_expr"]] if given["has_test"] else [],
    )

    result = update_row_counts(
        table_name=given["table_name"],
        data_source="pudl",
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
    # Given
    mocker.patch(
        "pudl.scripts.dbt_helper._get_local_table_path",
        return_value=case.given["mocked_path"],
    )

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


@dataclass
class DbtSchemaMocks:
    field: unittest.mock.Mock
    resource: unittest.mock.Mock
    pudl_package: unittest.mock.Mock
    table_name: str
    partition_expr: str
    schema: DbtSchema
    yaml: str
    has_row_count_test: bool
    load_method: str


@pytest.fixture(
    params=[
        ("from_table_name", False),
        ("from_yaml", False),
        ("from_yaml", True),
    ],
    ids=[
        "table_name__no_row_count_test",
        "yaml__no_row_count_test",
        "yaml__with_row_count_test",
    ],
)
def dbt_schema_mocks(request, mocker):
    """Set up mocks to check two ways of making the same basic schema: from metadata, and from yaml."""
    load_method, has_row_count_test = request.param

    field_name = str(mocker.sentinel.field_name)
    table_name = str(mocker.sentinel.table_name)
    partition_expr = str(mocker.sentinel.partition_expr)

    mocked_field = mocker.Mock()
    mocked_field.name = field_name
    mocked_resource = mocker.Mock()
    mocked_resource.schema.fields = [
        mocked_field,
    ]
    mocked_ppkg = mocker.patch("pudl.scripts.dbt_helper.PUDL_PACKAGE")
    mocked_ppkg.get_resource.return_value = mocked_resource

    data_tests = (
        [
            {
                "check_row_counts_per_partition": {
                    "arguments": {
                        "table_name": table_name,
                        "partition_expr": partition_expr,
                    }
                }
            }
        ]
        if has_row_count_test
        else None
    )

    schema = DbtSchema(
        version=2,
        models=None,
        sources=[
            DbtSource(
                name="pudl",
                tables=[
                    DbtTable(
                        name=table_name,
                        data_tests=data_tests,
                        columns=[
                            DbtColumn(
                                name=field_name,
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )

    yaml_tests = (
        f"""\
        data_tests:
          - check_row_counts_per_partition:
              arguments:
                table_name: {table_name}
                partition_expr: {partition_expr}
        """
        if has_row_count_test
        else ""
    )

    yaml = f"""\
version: 2
sources:
  - name: pudl
    tables:
      - name: {table_name}
{yaml_tests.rstrip()}
        columns:
          - name: {field_name}
"""
    return DbtSchemaMocks(
        field=mocked_field,
        resource=mocked_resource,
        pudl_package=mocked_ppkg,
        table_name=table_name,
        partition_expr=partition_expr,
        schema=schema,
        yaml=yaml,
        has_row_count_test=has_row_count_test,
        load_method=load_method,
    )


def test_dbt_schema_loading(dbt_schema_mocks, mocker):
    if dbt_schema_mocks.load_method == "from_table_name":
        actual = DbtSchema.from_table_name(dbt_schema_mocks.table_name)
    else:
        mock_path = mocker.Mock()
        mock_path.open.return_value = StringIO(dbt_schema_mocks.yaml)
        actual = DbtSchema.from_yaml(mock_path)

    assert actual == dbt_schema_mocks.schema
    assert actual.model_dump(exclude_none=True) == dbt_schema_mocks.schema.model_dump(
        exclude_none=True
    )


QUANTILE_TESTS = [
    GivenExpect(
        given=[
            dict(
                title="one entry, median",
                low_q=0,
                low_bound=1,
                hi_q=0,
                hi_bound=3,
                **TEMPLATE,
            )
        ],
        expect={
            "data": [
                {
                    "expect_quantile_constraints": {
                        "row_condition": "",
                        "weight_column": "weight",
                        "constraints": [
                            {
                                "quantile": 0,
                                "min_value": 1,
                                "max_value": 3,
                            },
                        ],
                    },
                }
            ],
        },
    ),
    GivenExpect(
        given=[
            dict(
                title="two entries, median and tail (1)",
                low_q=0,
                low_bound=1,
                hi_q=0,
                hi_bound=3,
                **TEMPLATE,
            ),
            dict(
                title="two entries, median and tail (2)",
                low_q=False,
                low_bound=False,
                hi_q=10,
                hi_bound=11,
                **TEMPLATE,
            ),
        ],
        expect={
            "data": [
                {
                    "expect_quantile_constraints": {
                        "row_condition": "",
                        "weight_column": "weight",
                        "constraints": [
                            {
                                "quantile": 0,
                                "min_value": 1,
                                "max_value": 3,
                            },
                            {
                                "quantile": 10,
                                "max_value": 11,
                            },
                        ],
                    },
                }
            ],
        },
    ),
    GivenExpect(
        given=[
            {
                "title": "two entries, differing query (1)",
                "low_q": 0,
                "low_bound": 1,
                "hi_q": 0,
                "hi_bound": 3,
                "query": "a",
                "data_col": "data",
                "weight_col": "",
            },
            {
                "title": "two entries, differing query (2)",
                "low_q": 0,
                "low_bound": 5,
                "hi_q": 0,
                "hi_bound": 7,
                "query": "b",
                "data_col": "data",
                "weight_col": "",
            },
        ],
        expect={
            "data": [
                {
                    "expect_quantile_constraints": {
                        "row_condition": "a",
                        "constraints": [
                            {
                                "quantile": 0,
                                "min_value": 1,
                                "max_value": 3,
                            },
                        ],
                    },
                },
                {
                    "expect_quantile_constraints": {
                        "row_condition": "b",
                        "constraints": [
                            {
                                "quantile": 0,
                                "min_value": 5,
                                "max_value": 7,
                            },
                        ],
                    },
                },
            ],
        },
    ),
]


@pytest.fixture
def blank_schema():
    return DbtSchema(
        sources=[
            DbtSource(
                tables=[
                    DbtTable(
                        name="source-table",
                        columns=[
                            DbtColumn(
                                name="source_column",
                            ),
                        ],
                    ),
                ],
            ),
        ],
        models=[
            DbtTable(
                name="model-table",
                columns=[
                    DbtColumn(
                        name="model_column",
                    ),
                ],
            ),
        ],
    )


def test_dbt_schema__add_source_tests(mocker, blank_schema):
    # check sources
    one_source_test = blank_schema.add_source_tests([mocker.sentinel.first_source_test])
    assert (
        mocker.sentinel.first_source_test
        in one_source_test.sources[0].tables[0].data_tests
    )

    # make sure we don't clobber existing tests on add
    two_source_tests = one_source_test.add_source_tests(
        [mocker.sentinel.second_source_test]
    )
    assert (
        mocker.sentinel.first_source_test
        in two_source_tests.sources[0].tables[0].data_tests
    )
    assert (
        mocker.sentinel.second_source_test
        in two_source_tests.sources[0].tables[0].data_tests
    )

    # check models
    one_model_test = blank_schema.add_source_tests(
        [mocker.sentinel.first_model_test], model_name="model-table"
    )
    assert mocker.sentinel.first_model_test in one_model_test.models[0].data_tests

    # make sure we don't clobber existing tests on add
    two_model_tests = one_model_test.add_source_tests(
        [mocker.sentinel.second_model_test], model_name="model-table"
    )
    assert mocker.sentinel.first_model_test in two_model_tests.models[0].data_tests
    assert mocker.sentinel.second_model_test in two_model_tests.models[0].data_tests


def test_dbt_schema__add_column_tests(mocker, blank_schema):
    # check sources
    one_source_test = blank_schema.add_column_tests(
        {"source_column": [mocker.sentinel.first_source_test]}
    )
    assert (
        mocker.sentinel.first_source_test
        in one_source_test.sources[0].tables[0].columns[0].data_tests
    )

    # make sure we don't clobber existing tests on add
    two_source_tests = one_source_test.add_column_tests(
        {"source_column": [mocker.sentinel.second_source_test]}
    )
    assert (
        mocker.sentinel.first_source_test
        in two_source_tests.sources[0].tables[0].columns[0].data_tests
    )
    assert (
        mocker.sentinel.second_source_test
        in two_source_tests.sources[0].tables[0].columns[0].data_tests
    )

    # check models
    one_model_test = blank_schema.add_column_tests(
        {"model_column": [mocker.sentinel.first_model_test]}, model_name="model-table"
    )
    assert (
        mocker.sentinel.first_model_test
        in one_model_test.models[0].columns[0].data_tests
    )

    # make sure we don't clobber existing tests on add
    two_model_tests = one_model_test.add_column_tests(
        {"model_column": [mocker.sentinel.second_model_test]}, model_name="model-table"
    )
    assert (
        mocker.sentinel.first_model_test
        in two_model_tests.models[0].columns[0].data_tests
    )
    assert (
        mocker.sentinel.second_model_test
        in two_model_tests.models[0].columns[0].data_tests
    )


MERGE_METADATA_TEST_CASES = [
    GivenExpect(
        given={
            "old_schema": {
                "table_name": "test_table",
                "columns": ["col1"],
                "partition_expr": "year",
                "add_row_count_test": True,
            },
            "new_schema": {
                "table_name": "test_table",
                "columns": ["col1"],
                "partition_expr": None,
                "add_row_count_test": False,
            },
        },
        expect={
            "data_tests": [
                {
                    "check_row_counts_per_partition": {
                        "arguments": {
                            "table_name": "test_table",
                            "partition_expr": "year",
                        }
                    }
                }
            ],
            "column_names": ["col1"],
        },
    ),
    GivenExpect(
        given={
            "old_schema": {
                "table_name": "test_table",
                "columns": ["col1"],
                "partition_expr": "old_expr",
                "add_row_count_test": True,
            },
            "new_schema": {
                "table_name": "test_table",
                "columns": ["col1"],
                "partition_expr": "new_expr",
                "add_row_count_test": True,
            },
        },
        expect={
            "data_tests": [
                {
                    "check_row_counts_per_partition": {
                        "arguments": {
                            "table_name": "test_table",
                            "partition_expr": "new_expr",
                        }
                    }
                }
            ],
            "column_names": ["col1"],
        },
    ),
    GivenExpect(
        given={
            "old_schema": {
                "table_name": "test_table",
                "columns": ["col1", "col2"],
                "partition_expr": "year",
                "add_row_count_test": False,
            },
            "new_schema": {
                "table_name": "test_table",
                "columns": ["col1", "col3"],
                "partition_expr": None,
                "add_row_count_test": False,
            },
        },
        expect={
            "data_tests": None,
            "column_names": ["col1", "col3"],
        },
    ),
]


@pytest.mark.parametrize("case", MERGE_METADATA_TEST_CASES)
def test_dbt_schema__merge_metadata_from(case, schema_factory):
    old_schema_args = case.given["old_schema"]
    new_schema_args = case.given["new_schema"]

    old_schema = schema_factory(**old_schema_args)
    new_schema = schema_factory(**new_schema_args)

    merged = new_schema.merge_metadata_from(old_schema)

    merged_table = merged.sources[0].tables[0]

    assert merged_table.data_tests == case.expect["data_tests"]

    expected_column_names = case.expect["column_names"]
    actual_column_names = [col.name for col in merged_table.columns]
    assert actual_column_names == expected_column_names


@pytest.mark.parametrize(
    "old, new, expected",
    [
        pytest.param(
            {"description": "x"},
            {"description": "x", "extra": "added"},
            False,
            id="Add only",
        ),
        pytest.param(
            {"description": "x"},
            {"description": "y"},
            True,
            id="Scalar mod",
        ),
        pytest.param(
            {"columns": {"col_a": {}}},
            {"columns": {}},
            False,
            id="Removed column only",
        ),
        pytest.param(
            {"columns": {"col_a": {}, "col_b": {}, "col_c": {}}},
            {"columns": {}},
            False,
            id="Multiple removed empty columns",
        ),
        pytest.param(
            {"columns": {"col_old": {}}},
            {"columns": {"col_new": {}}},
            False,
            id="Empty column rename (or add and remove different empty columns) ignored",
        ),
        pytest.param(
            {"columns": {"col_a": {"tests": ["not_null"]}}},
            {"columns": {}},
            True,
            id="Removed column with test(s)",
        ),
        pytest.param(
            {"columns": {"col_b": {"tags": ["a"]}}},
            {"columns": {"col_b": {"tags": ["b"]}}},
            True,
            id="Nested mod",
        ),
        pytest.param(
            {},
            {},
            False,
            id="Empty",
        ),
        pytest.param(
            {"meta": {}},
            {"meta": {"notes": "foo"}},
            False,
            id="Add in nested key",
        ),
        pytest.param(
            {"meta": {"notes": "foo"}},
            {"meta": {"notes": "bar"}},
            True,
            id="Nested old",
        ),
    ],
)
def test_schema_has_removals_or_modifications(old, new, expected):
    assert schema_has_removals_or_modifications(old, new) == expected


def test_complex_schema_diff_output():
    old_schema = DbtSchema(
        version=1,
        sources=[
            DbtSource(
                name="source1",
                tables=[
                    DbtTable(
                        name="table1",
                        description="desc",
                        columns=[],
                    )
                ],
            )
        ],
        models=[
            DbtTable(name="model1", description="old", columns=[]),
        ],
    )

    new_schema = DbtSchema(
        version=2,
        sources=[
            DbtSource(
                name="source1",
                tables=[
                    DbtTable(
                        name="table1",
                        description="updated",
                        columns=[],
                    )
                ],
            ),
            DbtSource(
                name="source2",
                tables=[
                    DbtTable(
                        name="new_table",
                        description="new",
                        columns=[],
                    )
                ],
            ),
        ],
        models=[
            DbtTable(name="model1", description="new", columns=[]),
            DbtTable(name="model2", description="added", columns=[]),
        ],
    )

    output = _schema_diff_summary(old_schema, new_schema)
    expected = """--- old_schema
+++ new_schema
@@ -1,12 +1,20 @@
-version: 1
+version: 2
 sources:
   - name: source1
     tables:
       - name: table1
-        description: desc
+        description: updated
+        columns: []
+  - name: source2
+    tables:
+      - name: new_table
+        description: new
         columns: []
 models:
   - name: model1
-    description: old
+    description: new
+    columns: []
+  - name: model2
+    description: added
     columns: []
"""
    assert [line.strip() for line in expected.strip().split("\n")] == [
        line.strip() for line in output
    ]


UPDATE_TABLE_SCHEMA_CASES = [
    # add column and metadata, no clobber -> success
    GivenExpect(
        given={
            "existing_columns": ["col_new"],
            "removed_columns_have_metadata": False,
            "clobber": False,
        },
        expect={"success": True},
    ),
    # add column and metadata, clobber -> success
    GivenExpect(
        given={
            "existing_columns": ["col_new"],
            "removed_columns_have_metadata": False,
            "clobber": True,
        },
        expect={"success": True},
    ),
    # remove column with no metadata, no clobber -> success
    GivenExpect(
        given={
            "existing_columns": [],
            "removed_columns_have_metadata": False,
            "clobber": False,
        },
        expect={"success": True},
    ),
    # remove column with no metadata, clobber -> success
    GivenExpect(
        given={
            "existing_columns": [],
            "removed_columns_have_metadata": False,
            "clobber": True,
        },
        expect={"success": True},
    ),
    # remove column with metadata, no clobber -> fail
    GivenExpect(
        given={
            "existing_columns": [],
            "removed_columns_have_metadata": True,
            "clobber": False,
        },
        expect={"success": False},
    ),
    # remove column with metadata, clobber -> success
    GivenExpect(
        given={
            "existing_columns": [],
            "removed_columns_have_metadata": True,
            "clobber": True,
        },
        expect={"success": True},
    ),
]


@pytest.mark.parametrize("case", UPDATE_TABLE_SCHEMA_CASES)
def test_update_table_schema(case, mocker):
    table_name = "my_table"
    data_source = "pudl"
    clobber = case.given["clobber"]

    # Patch filesystem and DBT methods
    mocker.patch("pathlib.Path.exists", return_value=not clobber)
    mocker.patch("pathlib.Path.mkdir")
    mock_schema = mocker.MagicMock()
    mock_schema.model_dump.return_value = {"columns": case.given["existing_columns"]}
    mock_schema.merge_metadata_from.return_value = mock_schema

    mocker.patch(
        "pudl.scripts.dbt_helper.DbtSchema.from_table_name", return_value=mock_schema
    )
    mocker.patch(
        "pudl.scripts.dbt_helper.DbtSchema.from_yaml", return_value=mock_schema
    )

    # Mock DeepDiff depending on metadata removal
    if case.given["removed_columns_have_metadata"]:
        mocker.patch(
            "pudl.scripts.dbt_helper.DeepDiff",
            return_value={
                "dictionary_item_removed": {
                    "root['columns']['col_a']",
                    "root['columns']['col_a']['description']",
                }
            },
        )
    else:
        mocker.patch("pudl.scripts.dbt_helper.DeepDiff", return_value={})

    mocker.patch("pudl.scripts.dbt_helper._log_schema_diff")  # suppress logging

    result = update_table_schema(table_name, data_source, clobber=clobber)
    assert result.success == case.expect["success"]


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
    parquet_path = PudlPaths().parquet_path("test_source__table_name")

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    row_count_csv_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # write out test data to disk
    test_data.to_parquet(parquet_path)
    old_row_counts.to_csv(row_count_csv_path, index=False)
    with schema_path.open("w") as f:
        f.write(test_schema)

    # patch out DBT_DIR so we use our lovely test schema + rowcounts
    mocker.patch("pudl.scripts.dbt_helper.DBT_DIR", new=dbt_dir)
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
