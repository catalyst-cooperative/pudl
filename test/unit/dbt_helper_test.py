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
    _generate_quantile_bounds_test,
    _load_schema_yaml,
)

TEMPLATE = {
    "data_col": "data",
    "query": "",
    "weight_col": "",
}


GivenExpect = namedtuple("GivenExpect", ["given", "expect"])

GENERATE_QUANTILE_BOUNDS = [
    GivenExpect(
        given=dict(title="Hi only", hi_q=0, hi_bound=1, **TEMPLATE),
        expect=[
            {
                "dbt_expectations.expect_column_quantile_values_to_be_between": {
                    "quantile": 0,
                    "max_value": 1,
                    "row_condition": "",
                    "weight_column": "",
                }
            }
        ],
    ),
    GivenExpect(
        given=dict(title="Low only", low_q=0, low_bound=1, **TEMPLATE),
        expect=[
            {
                "dbt_expectations.expect_column_quantile_values_to_be_between": {
                    "quantile": 0,
                    "min_value": 1,
                    "row_condition": "",
                    "weight_column": "",
                }
            }
        ],
    ),
    GivenExpect(
        given=dict(title="Both", low_q=0, low_bound=1, hi_q=2, hi_bound=3, **TEMPLATE),
        expect=[
            {
                "dbt_expectations.expect_column_quantile_values_to_be_between": {
                    "quantile": 0,
                    "min_value": 1,
                    "row_condition": "",
                    "weight_column": "",
                }
            },
            {
                "dbt_expectations.expect_column_quantile_values_to_be_between": {
                    "quantile": 2,
                    "max_value": 3,
                    "row_condition": "",
                    "weight_column": "",
                }
            },
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
