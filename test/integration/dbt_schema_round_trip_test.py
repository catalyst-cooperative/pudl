from pathlib import Path
import warnings

import deepdiff
import pytest

from pudl.dbt_schema import merge_schema
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.scripts.dbt_helper import DbtSchema


def get_schemas(root_dir: Path, pattern: str):
    schema_paths = sorted(root_dir.glob(pattern))
    schemas = {path.parent.name: DbtSchema.from_yaml(path) for path in schema_paths}
    return schemas


@pytest.mark.parametrize("resource_name", [r.name for r in PUDL_PACKAGE.resources])
def test_merge_schema_roundtrip(resource_name):
    dbt_dir = Path(__file__).parent.parent.parent / "dbt"
    reference = DbtSchema.from_yaml(
        next(dbt_dir.glob(f"models/**/{resource_name}/schema.yml"))
    )
    machine_path = next(
        dbt_dir.glob(f"schema_inputs/**/{resource_name}/schema.machine.yml")
    )

    human_path = next(
        dbt_dir.glob(f"schema_inputs/**/{resource_name}/schema.human.yml")
    )
    merged = merge_schema(machine_path, human_path)
    try:
        assert merged == reference
    except AssertionError:
        warnings.warn(f"{resource_name} strict diff failed, trying order-insensitive")
        merged_table = merged.sources[0].tables[0]
        reference_table = reference.sources[0].tables[0]
        diff = deepdiff.DeepDiff(
            merged_table, reference_table, ignore_order=True, report_repetition=True
        )
        assert diff == {}
