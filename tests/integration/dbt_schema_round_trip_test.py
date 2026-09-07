import shutil
import subprocess
from pathlib import Path

import pytest

from pudl import PUDL_DBT_PATH
from pudl.dbt_schema import DbtSchema, merge_schema
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.scripts.dbt_helper import insert_data_source, maybe_schema_from_path


def _merged_schema(resource_name: str) -> DbtSchema:
    machine_schema = DbtSchema.from_table_name(resource_name)
    human_schema = maybe_schema_from_path(
        insert_data_source(PUDL_DBT_PATH / "schema_inputs", resource_name)
        / "schema.human.yml"
    )
    return merge_schema(machine_schema, human_schema)


@pytest.mark.parametrize("resource_name", [r.name for r in PUDL_PACKAGE.resources])
def test_merge_schema_roundtrip(resource_name):
    reference = DbtSchema.from_yaml(
        insert_data_source(PUDL_DBT_PATH / "models", resource_name) / "schema.yml"
    )
    assert _merged_schema(resource_name) == reference


@pytest.fixture(scope="module")
def prettier_bin() -> str:
    path = shutil.which("prettier")
    if path is None:
        pytest.skip("prettier is not installed / not on PATH")
    return path


@pytest.fixture(scope="module")
def unformatted_resources(prettier_bin: str, tmp_path_factory) -> set[str]:
    """Resource names whose freshly generated schema.yml isn't Prettier-formatted.

    Writes every resource's merged schema.yml to its own subdirectory of a temp
    dir, then runs a single batched ``prettier --check`` over all of them --
    much faster than shelling out once per resource. This verifies that
    ``_prettier_yaml_dumps`` (our hand-rolled emulation of Prettier's YAML
    formatting in ``pudl.dbt_schema``) hasn't drifted from what Prettier itself
    would produce, which would otherwise show up as a spurious quoting/line-wrap
    diff the moment someone runs ``prek run prettier`` after regenerating
    schemas with ``dbt_helper update-tables --schema``.
    """
    root = tmp_path_factory.mktemp("dbt_schema_prettier_check")
    for resource in PUDL_PACKAGE.resources:
        out_dir = root / resource.name
        out_dir.mkdir()
        _merged_schema(resource.name).to_yaml(out_dir / "schema.yml")

    result = subprocess.run(  # noqa: S603
        [prettier_bin, "--check", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )

    unformatted = set()
    for line in (result.stdout + result.stderr).splitlines():
        line = line.strip()
        if not line.startswith("[warn]") or not line.endswith(".yml"):
            continue
        path = Path(line.removeprefix("[warn]").strip())
        unformatted.add(path.parent.name)
    return unformatted


@pytest.mark.parametrize("resource_name", [r.name for r in PUDL_PACKAGE.resources])
def test_merged_schema_matches_prettier_formatting(
    resource_name: str, unformatted_resources: set[str]
):
    """Regenerated schema.yml content should already satisfy Prettier's formatting.

    If this fails, ``_prettier_yaml_dumps`` has drifted from Prettier's actual
    output: regenerating schemas would produce a spurious formatting-only diff
    on top of any real content changes, on the next ``prek run prettier``.
    """
    assert resource_name not in unformatted_resources
