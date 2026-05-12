"""Focused unit tests for dbt schema merging semantics."""

from textwrap import dedent

import pytest
import yaml

from pudl.dbt_schema import DbtSchema, merge_schema


def _schema_from_yaml(schema_yaml: str) -> DbtSchema:
    """Build DbtSchema from inline YAML for readable object-level test cases."""
    return DbtSchema.model_validate(yaml.safe_load(dedent(schema_yaml).lstrip()))


@pytest.mark.parametrize(
    "machine_yaml,human_yaml,expected_yaml",
    [
        pytest.param(
            # machine_yaml
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: plants
                    data_tests:
                      - machine_table: test
                    columns:
                      - name: plant_id_eia
                        data_tests:
                          - machine_column: test
            """,
            # human_yaml
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: plants
                    data_tests:
                      - human_table: test
                    columns:
                      - name: plant_id_eia
                        data_tests:
                          - human_column: test
            """,
            # expected_yaml
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: plants
                    data_tests:
                      - machine_table: test
                      - human_table: test
                    columns:
                      - name: plant_id_eia
                        data_tests:
                          - machine_column: test
                          - human_column: test
            """,
            id="matching-human-tests-append-after-machine-tests",
        ),
        pytest.param(
            # machine_yaml
            """
            version: 2
            """,
            # human_yaml
            """
            version: 2
            models:
              - name: plants_model
                data_tests:
                  - human_model: test
                columns:
                  - name: plant_id_eia
                    data_tests:
                      - human_column: test
            """,
            # expected_yaml
            """
            version: 2
            models:
              - name: plants_model
                data_tests:
                  - human_model: test
                columns:
                  - name: plant_id_eia
                    data_tests:
                      - human_column: test
            """,
            id="human-models-are-allowed-when-machine-has-no-models",
        ),
    ],
)
def test_merge_schema(
    machine_yaml: str,
    human_yaml: str,
    expected_yaml: str,
) -> None:
    """Check that merged machine and human schemas observe the expected ordering and full/empty constraints."""
    assert merge_schema(
        _schema_from_yaml(machine_yaml),
        _schema_from_yaml(human_yaml),
    ) == _schema_from_yaml(expected_yaml)


@pytest.mark.parametrize(
    "machine_yaml,human_yaml,missing_name",
    [
        pytest.param(
            """
            version: 2
            sources:
              - name: pudl
                tables: []
            """,
            """
            version: 2
            sources:
              - name: ferc
                tables: []
            """,
            "ferc",
            id="unmatched-human-source-raises",
        ),
        pytest.param(
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: plants
            """,
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: utilities
            """,
            "utilities",
            id="unmatched-human-table-raises",
        ),
        pytest.param(
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: plants
                    columns:
                      - name: id
            """,
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: plants
                    columns:
                      - name: utility_id_eia
            """,
            "utility_id_eia",
            id="unmatched-human-column-raises",
        ),
    ],
)
def test_merge_schema_raises_for_unmatched_human_overlay(
    machine_yaml: str,
    human_yaml: str,
    missing_name: str,
) -> None:
    """Check that we raise the expected error when a human schema references structures missing from the machine copy."""
    with pytest.raises(KeyError, match=missing_name):
        merge_schema(
            _schema_from_yaml(machine_yaml),
            _schema_from_yaml(human_yaml),
        )


def test_validate_humanity():
    """Pass humanity check when structures are limited to human-specifiable elements."""
    schema_yaml = """
        version: 2
        sources:
          - name: pudl
            tables:
              - name: plants
                data_tests:
                  - fake_table_test
                columns:
                  - name: utility_id_eia
                    data_tests:
                      - fake_column_test
        models:
          - name: test_model
            description: some test model"""
    _schema_from_yaml(schema_yaml).validate_humanity()


@pytest.mark.parametrize(
    ["schema_yaml", "match"],
    [
        pytest.param(
            """
            version: 2
            sources:
              - name: pudl
                description: misguided override
            """,
            "{'description'} in human source:pudl",
            id="source-description",
        ),
        pytest.param(
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: fake_table
                    tags:
                      - foo
            """,
            "{'tags'} in human source:pudl.fake_table",
            id="table-tags",
        ),
        pytest.param(
            """
            version: 2
            sources:
              - name: pudl
                tables:
                  - name: fake_table
                    columns:
                      - name: fake_column
                        meta:
                          fake: metadata
            """,
            "{'meta'} in human source:pudl.fake_table.fake_column",
            id="table-tags",
        ),
    ],
)
def test_validate_humanity_invalid(schema_yaml, match):
    """Fail humanity check when specifying structures not permitted for humans."""
    with pytest.raises(AssertionError, match=match):
        _schema_from_yaml(schema_yaml).validate_humanity()
