import unittest
from collections import namedtuple
from dataclasses import dataclass
from io import StringIO

import pytest

from pudl.scripts.dbt_helper import (
    DbtColumn,
    DbtSchema,
    DbtSource,
    DbtTable,
    _clean_row_condition,
    _convert_config_variable_to_quantile_tests,
    _generate_quantile_bounds_test,
    _get_local_table_path,
    _get_model_path,
    _get_row_count_csv_path,
    _infer_partition_column,
    _load_schema_yaml,
    get_data_source,
)

TEMPLATE = {
    "data_col": "data",
    "query": "",
    "weight_col": "weight",
}


def with_name(mock, name):
    """Helper function to set the name attribute on a Mock without it getting sucked into the Mock internals (for repr, etc)."""
    mock.name = name
    return mock


def test_get_data_source(mocker):
    mock_resource = mocker.Mock()
    mocker.patch(
        "pudl.scripts.dbt_helper.PUDL_PACKAGE"
    ).get_resource.return_value = mock_resource
    mock_resource.sources = [""] * 2
    assert get_data_source("multiple sources") == "output"
    mock_resource.sources = [with_name(mocker.Mock(), str(mocker.sentinel.source))]
    assert get_data_source("one source") == str(mocker.sentinel.source)


def test__get_local_table_path(mocker):
    mock_pudlpaths = mocker.Mock()
    mocker.patch("pudl.scripts.dbt_helper.PudlPaths").return_value = mock_pudlpaths
    _get_local_table_path(mocker.sentinel.table)
    mock_pudlpaths.parquet_path.assert_called_once_with(mocker.sentinel.table)


def test__get_model_path():
    assert "models" in str(_get_model_path("", ""))


def test__get_row_count_csv_path():
    assert _get_row_count_csv_path(etl_fast=True) != _get_row_count_csv_path(
        etl_fast=False
    )


@pytest.mark.parametrize("key", ["report_year", "report_date", "datetime_utc", None])
def test__infer_partition_column(mocker, key):
    mock_resource = mocker.Mock()
    mocker.patch(
        "pudl.scripts.dbt_helper.PUDL_PACKAGE"
    ).get_resource.return_value = mock_resource
    mock_resource.schema.fields = [with_name(mocker.Mock(), key)]
    assert _infer_partition_column("") == key


GivenExpect = namedtuple("GivenExpect", ["given", "expect"])

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
    "test_config,expected",
    GENERATE_QUANTILE_BOUNDS,
)
def test__generate_quantile_bounds_test(test_config, expected):
    actual = _generate_quantile_bounds_test(test_config)
    assert actual == expected


@pytest.mark.parametrize(
    "row_condition,expected",
    [
        ("'0000-00-00'", "CAST('0000-00-00' AS DATE)"),
        ("x == 0", "x = 0"),
        ("x != 0", "x <> 0"),
    ],
)
def test__clean_row_condition(row_condition, expected):
    actual = _clean_row_condition(row_condition)
    assert actual == expected


@dataclass
class DbtSchemaMocks:
    field: unittest.mock.Mock
    resource: unittest.mock.Mock
    pudl_package: unittest.mock.Mock
    table_name: str
    partition_column: str
    schema: DbtSchema
    yaml: str


@pytest.fixture
def dbt_schema_mocks(mocker):
    """Set up mocks to check two ways of making the same basic schema: from metadata, and from yaml."""
    mocked_field = mocker.Mock()
    mocked_field.name = str(mocker.sentinel.field_name)
    mocked_resource = mocker.Mock()
    mocked_resource.schema.fields = [
        mocked_field,
    ]
    mocked_ppkg = mocker.patch("pudl.scripts.dbt_helper.PUDL_PACKAGE")
    mocked_ppkg.get_resource.return_value = mocked_resource
    schema = DbtSchema(
        version=2,
        models=None,
        sources=[
            DbtSource(
                name="pudl",
                data_tests=None,
                tables=[
                    DbtTable(
                        name=str(mocker.sentinel.table_name),
                        data_tests=[
                            {
                                "check_row_counts_per_partition": {
                                    "table_name": str(mocker.sentinel.table_name),
                                    "partition_column": str(
                                        mocker.sentinel.partition_column
                                    ),
                                }
                            }
                        ],
                        columns=[
                            DbtColumn(
                                name=str(mocker.sentinel.field_name),
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    yaml = f"""
version: 2
sources:
  - name: pudl
    tables:
      - name: {mocker.sentinel.table_name}
        data_tests:
          - check_row_counts_per_partition:
              table_name: {mocker.sentinel.table_name}
              partition_column: {mocker.sentinel.partition_column}
        columns:
          - name: {mocker.sentinel.field_name}
"""
    return DbtSchemaMocks(
        field=mocked_field,
        resource=mocked_resource,
        pudl_package=mocked_ppkg,
        table_name=str(mocker.sentinel.table_name),
        partition_column=str(mocker.sentinel.partition_column),
        schema=schema,
        yaml=yaml,
    )


def test_dbt_schema__from_table_name(dbt_schema_mocks):
    actual = DbtSchema.from_table_name(
        dbt_schema_mocks.table_name,
        partition_column=dbt_schema_mocks.partition_column,
    )
    assert actual == dbt_schema_mocks.schema


def test__load_schema_yaml(mocker, dbt_schema_mocks):
    with StringIO(dbt_schema_mocks.yaml) as f:
        mock_path = mocker.Mock()
        mock_path.open.return_value = f
        actual = _load_schema_yaml(mock_path)
    assert actual == dbt_schema_mocks.schema


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


@pytest.mark.parametrize(
    "config,expected",
    QUANTILE_TESTS,
)
def test__convert_config_variable_to_quantile_tests(mocker, config, expected):
    mock_get_config = mocker.patch("pudl.scripts.dbt_helper._get_config")
    mock_get_config.return_value = config
    actual = _convert_config_variable_to_quantile_tests("")
    assert actual == expected


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
