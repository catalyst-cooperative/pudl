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
            """
            version: 2
            """,
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
    """Merge machine and human schema objects into expected reference schema."""
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
                tables:
                  - name: plants
                    columns:
                      - name: id
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
                    columns:
                      - name: id
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
        pytest.param(
            """
            version: 2
            models:
              - name: plants_model
                columns:
                  - name: id
            """,
            """
            version: 2
            models:
              - name: utilities_model
                columns: []
            """,
            "utilities_model",
            id="unmatched-human-model-raises-in-overlay-mode",
        ),
    ],
)
def test_merge_schema_raises_for_unmatched_human_overlay(
    machine_yaml: str,
    human_yaml: str,
    missing_name: str,
) -> None:
    """Raise when human overlay references machine-missing entities."""
    with pytest.raises(KeyError, match=missing_name):
        merge_schema(
            _schema_from_yaml(machine_yaml),
            _schema_from_yaml(human_yaml),
        )
