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
    _diff_named_items,
    _get_local_table_path,
    _get_model_path,
    _get_row_count_csv_path,
    _has_removals_or_modifications,
    _infer_partition_column,
    diff_dbt_column,
    diff_dbt_schema,
    diff_dbt_source,
    diff_dbt_table,
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
    "case",
    [
        GivenExpect(
            given=(
                "description change",
                lambda: (
                    DbtColumn(name="col", description="old"),
                    DbtColumn(name="col", description="new"),
                ),
            ),
            expect={"description": {"old": "old", "new": "new"}},
        ),
        GivenExpect(
            given=(
                "add data test",
                lambda: (
                    DbtColumn(name="col", data_tests=[]),
                    DbtColumn(name="col", data_tests=[{"expect_column_to_exist": {}}]),
                ),
            ),
            expect={
                "data_tests": {
                    "added": ["{'expect_column_to_exist': {}}"],
                    "removed": [],
                }
            },
        ),
        GivenExpect(
            given=(
                "remove data test",
                lambda: (
                    DbtColumn(name="col", data_tests=[{"expect_column_to_exist": {}}]),
                    DbtColumn(name="col", data_tests=[]),
                ),
            ),
            expect={
                "data_tests": {
                    "added": [],
                    "removed": ["{'expect_column_to_exist': {}}"],
                }
            },
        ),
        GivenExpect(
            given=(
                "add tag",
                lambda: (
                    DbtColumn(name="col", tags=[]),
                    DbtColumn(name="col", tags=["new"]),
                ),
            ),
            expect={
                "tags": {
                    "added": ["new"],
                    "removed": [],
                }
            },
        ),
    ],
)
def test_diff_dbt_column(case):
    label, column_builder = case.given
    old, new = column_builder()
    actual = diff_dbt_column(old, new)
    assert actual == case.expect


@pytest.mark.parametrize(
    "case",
    [
        GivenExpect(
            given=(
                "description changed",
                lambda: (
                    DbtTable(name="t", description="old", columns=[]),
                    DbtTable(name="t", description="new", columns=[]),
                ),
            ),
            expect={"description": {"old": "old", "new": "new"}},
        ),
        GivenExpect(
            given=(
                "columns changed",
                lambda: (
                    DbtTable(name="t", columns=[DbtColumn(name="a")]),
                    DbtTable(name="t", columns=[DbtColumn(name="b")]),
                ),
            ),
            expect={
                "columns": {
                    "a": {"removed": {"name": "a"}},
                    "b": {"added": {"name": "b"}},
                }
            },
        ),
    ],
)
def test_diff_dbt_table(case):
    label, builder = case.given
    old, new = builder()
    actual = diff_dbt_table(old, new)
    assert actual == case.expect


@pytest.mark.parametrize(
    "case",
    [
        GivenExpect(
            given=(
                "description change",
                lambda: (
                    DbtSource(name="source", description="old", tables=[]),
                    DbtSource(name="source", description="new", tables=[]),
                ),
            ),
            expect={"description": {"old": "old", "new": "new"}},
        ),
        GivenExpect(
            given=(
                "add data test",
                lambda: (
                    DbtSource(name="source", data_tests=[], tables=[]),
                    DbtSource(name="source", data_tests=[{"test": "val"}], tables=[]),
                ),
            ),
            expect={
                "data_tests": {
                    "added": ["{'test': 'val'}"],
                    "removed": [],
                }
            },
        ),
        GivenExpect(
            given=(
                "table description change",
                lambda: (
                    DbtSource(
                        name="source",
                        tables=[
                            DbtTable(name="table", description="old", columns=[]),
                        ],
                    ),
                    DbtSource(
                        name="source",
                        tables=[
                            DbtTable(name="table", description="new", columns=[]),
                        ],
                    ),
                ),
            ),
            expect={
                "tables": {
                    "table": {
                        "description": {"old": "old", "new": "new"},
                    }
                }
            },
        ),
        GivenExpect(
            given=(
                "table name changed",
                lambda: (
                    DbtSource(
                        name="source",
                        tables=[
                            DbtTable(name="old_name", description="same", columns=[]),
                        ],
                    ),
                    DbtSource(
                        name="source",
                        tables=[
                            DbtTable(name="new_name", description="same", columns=[]),
                        ],
                    ),
                ),
            ),
            expect={
                "tables": {
                    "new_name": {
                        "added": {
                            "name": "new_name",
                            "description": "same",
                            "columns": [],
                        }
                    },
                    "old_name": {
                        "removed": {
                            "name": "old_name",
                            "description": "same",
                            "columns": [],
                        }
                    },
                }
            },
        ),
    ],
)
def test_diff_dbt_source(case):
    label, source_builder = case.given
    old, new = source_builder()
    actual = diff_dbt_source(old, new)
    assert actual == case.expect


@pytest.mark.parametrize(
    "case",
    [
        GivenExpect(
            given=(
                "complex schema diff",
                lambda: (
                    DbtSchema(
                        version=1,
                        sources=[
                            DbtSource(
                                name="source1",
                                tables=[
                                    DbtTable(
                                        name="table1", description="desc", columns=[]
                                    ),
                                ],
                            )
                        ],
                        models=[
                            DbtTable(name="model1", description="old", columns=[]),
                        ],
                    ),
                    DbtSchema(
                        version=2,
                        sources=[
                            DbtSource(
                                name="source1",
                                tables=[
                                    DbtTable(
                                        name="table1", description="updated", columns=[]
                                    ),
                                ],
                            ),
                            DbtSource(
                                name="source2",
                                tables=[
                                    DbtTable(
                                        name="new_table", description="new", columns=[]
                                    ),
                                ],
                            ),
                        ],
                        models=[
                            DbtTable(name="model1", description="new", columns=[]),
                            DbtTable(name="model2", description="added", columns=[]),
                        ],
                    ),
                ),
            ),
            expect={
                "version": {"old": 1, "new": 2},
                "sources": {
                    "source1": {
                        "tables": {
                            "table1": {
                                "description": {"old": "desc", "new": "updated"},
                            }
                        }
                    },
                    "source2": {
                        "added": {
                            "name": "source2",
                            "tables": [
                                {
                                    "name": "new_table",
                                    "description": "new",
                                    "columns": [],
                                }
                            ],
                        }
                    },
                },
                "models": {
                    "model1": {
                        "description": {"old": "old", "new": "new"},
                    },
                    "model2": {
                        "added": {
                            "name": "model2",
                            "description": "added",
                            "columns": [],
                        }
                    },
                },
            },
        )
    ],
)
def test_diff_dbt_schema(case):
    label, schema_builder = case.given
    old, new = schema_builder()
    actual = diff_dbt_schema(old, new)
    assert actual == case.expect


@pytest.mark.parametrize(
    "case",
    [
        GivenExpect(
            given=(
                "add item",
                lambda: (
                    [DbtTable(name="a", description="desc", columns=[])],
                    [
                        DbtTable(name="a", description="desc", columns=[]),
                        DbtTable(name="b", description="new", columns=[]),
                    ],
                ),
            ),
            expect={"b": {"added": {"name": "b", "description": "new", "columns": []}}},
        ),
        GivenExpect(
            given=(
                "remove item",
                lambda: (
                    [
                        DbtTable(name="a", description="desc", columns=[]),
                        DbtTable(name="b", description="gone", columns=[]),
                    ],
                    [DbtTable(name="a", description="desc", columns=[])],
                ),
            ),
            expect={
                "b": {"removed": {"name": "b", "description": "gone", "columns": []}}
            },
        ),
        GivenExpect(
            given=(
                "change item",
                lambda: (
                    [DbtTable(name="a", description="old", columns=[])],
                    [DbtTable(name="a", description="new", columns=[])],
                ),
            ),
            expect={"a": {"description": {"old": "old", "new": "new"}}},
        ),
    ],
)
def test_diff_named_items(case):
    label, builder = case.given
    old, new = builder()
    actual = _diff_named_items(old, new, diff_dbt_table)
    assert actual == case.expect


@pytest.mark.parametrize(
    "diff,expected",
    [
        ({"description": {"added": "foo"}}, False),  # Add only
        ({"description": {"old": "x", "new": "y"}}, True),  # Scalar mod
        (
            {"columns": {"col_a": {"removed": {"name": "col_a"}}}},
            True,
        ),  # Removed column
        (
            {"columns": {"col_b": {"tags": {"old": ["a"], "new": ["b"]}}}},
            True,
        ),  # Nested mod
        ({}, False),  # Empty
        ({"meta": {"notes": {"added": "info"}}}, False),  # Add in nested key
        ({"meta": {"notes": {"old": "foo", "new": "bar"}}}, True),  # Nested old
    ],
)
def test__has_removals_or_modifications(diff, expected):
    assert _has_removals_or_modifications(diff) == expected
