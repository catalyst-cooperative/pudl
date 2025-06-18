import unittest
from collections import namedtuple
from dataclasses import dataclass
from io import StringIO

import pytest
from deepdiff import DeepDiff

from pudl.scripts.dbt_helper import (
    DbtColumn,
    DbtSchema,
    DbtSource,
    DbtTable,
    _get_local_table_path,
    _get_model_path,
    _get_row_count_csv_path,
    _infer_partition_column,
    _schema_diff_summary,
    get_data_source,
    schema_has_removals_or_modifications,
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
    assert _get_row_count_csv_path(target="etl-fast") != _get_row_count_csv_path(
        target="etl-full"
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


def test_dbt_schema__from_yaml(mocker, dbt_schema_mocks):
    with StringIO(dbt_schema_mocks.yaml) as f:
        mock_path = mocker.Mock()
        mock_path.open.return_value = f
        actual = DbtSchema.from_yaml(mock_path)
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


@pytest.mark.parametrize(
    "diff, expected",
    [
        pytest.param(
            {"dictionary_item_added": {"root['description']"}}, False, id="Add only"
        ),
        pytest.param(
            {
                "values_changed": {
                    "root['description']": {"old_value": "x", "new_value": "y"}
                }
            },
            True,
            id="Scalar mod",
        ),
        pytest.param(
            {"dictionary_item_removed": {"root['columns']['col_a']"}},
            True,
            id="Removed column",
        ),
        pytest.param(
            {
                "values_changed": {
                    "root['columns']['col_b']['tags']": {
                        "old_value": ["a"],
                        "new_value": ["b"],
                    }
                }
            },
            True,
            id="Nested mod",
        ),
        pytest.param({}, False, id="Empty"),
        pytest.param(
            {"dictionary_item_added": {"root['meta']['notes']"}},
            False,
            id="Add in nested key",
        ),
        pytest.param(
            {
                "values_changed": {
                    "root['meta']['notes']": {"old_value": "foo", "new_value": "bar"}
                }
            },
            True,
            id="Nested old",
        ),
    ],
)
def test_schema_has_removals_or_modifications(diff, expected):
    assert schema_has_removals_or_modifications(diff) == expected


def test_complex_schema_diff_output(capsys):
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

    diff = DeepDiff(
        old_schema.model_dump(exclude_none=True),
        new_schema.model_dump(exclude_none=True),
        ignore_order=True,
    )
    output = _schema_diff_summary(diff)

    # Version change
    assert "version" in output, output
    assert "'old_value': 1" in output, output
    assert "'new_value': 2" in output, output

    # source1 table1 description update
    assert "source1" in output, output
    assert "table1" in output, output
    assert "'description': 'desc'" in output, output
    assert "'description': 'updated'" in output, output

    # Added source2 and new_table
    assert "source2" in output, output
    assert "new_table" in output, output
    assert "'description': 'new'" in output, output

    # model1 description update
    assert "model1" in output, output
    assert "'description': 'old'" in output, output
    assert "'description': 'new'" in output, output

    # model2 addition
    assert "model2" in output, output
    assert "'description': 'added'" in output, output
