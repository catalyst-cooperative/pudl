import warnings

import deepdiff
import pytest

from pudl import PUDL_DBT_PATH
from pudl.dbt_schema import DbtSchema, merge_schema
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.scripts.dbt_helper import insert_data_source, maybe_schema_from_path


@pytest.mark.parametrize("resource_name", [r.name for r in PUDL_PACKAGE.resources])
def test_merge_schema_roundtrip(resource_name):
    reference = DbtSchema.from_yaml(
        insert_data_source(PUDL_DBT_PATH / "models", resource_name) / "schema.yml"
    )
    machine_schema = DbtSchema.from_table_name(resource_name)
    human_schema = maybe_schema_from_path(
        insert_data_source(PUDL_DBT_PATH / "schema_inputs", resource_name)
        / "schema.human.yml"
    )

    merged = merge_schema(machine_schema, human_schema)
    try:
        assert merged == reference
    except AssertionError as e:
        # 2026-05 TODO: remove this warn and fail for real once we believe the ordering is stable
        warnings.warn(
            f"{resource_name} strict diff failed, trying order-insensitive",
            stacklevel=1,
        )
        diff = deepdiff.DeepDiff(
            merged, reference, ignore_order=True, report_repetition=True
        )
        assert diff == {}, f"Strict and lax diff failed for {resource_name}"
        raise AssertionError(
            f"Strict diff failed for {resource_name} but lax diff was okay"
        ) from e
