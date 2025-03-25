"""Check that all fields in PUDL exist in dbt."""

from collections import defaultdict
from pathlib import Path

from pudl.metadata.classes import PUDL_PACKAGE
from pudl.scripts.dbt_helper import _get_model_path, _load_schema_yaml, get_data_source


def generate_legible_output(
    pudl_tables_not_in_dbt,
    pudl_fields_not_in_dbt,
    dbt_tables_not_in_pudl,
    dbt_fields_not_in_pudl,
) -> str:
    components = []

    def tables(name, dct):
        return f"{name} is missing the following tables:\n\t{'\n\t'.join(sorted(dct))}"

    def fields(name, dct):
        scratch = [f"{name} is missing the following fields:"]
        for table, fields in dct.items():
            scratch.append(f"\t{table}\n\t\t{'\n\t\t'.join(fields)}")
        return "\n".join(scratch)

    if pudl_tables_not_in_dbt:
        components.append(tables("dbt", pudl_tables_not_in_dbt))
    if pudl_fields_not_in_dbt:
        components.append(fields("dbt", pudl_fields_not_in_dbt))
    if dbt_tables_not_in_pudl:
        components.append(tables("pudl", dbt_tables_not_in_pudl))
    if dbt_fields_not_in_pudl:
        components.append(fields("pudl", dbt_fields_not_in_pudl))
    return "\n\n".join(components)


def get_schema_path(table_name) -> Path:
    data_source = get_data_source(table_name)
    model_path = _get_model_path(table_name, data_source)
    return (model_path / "schema.yml").resolve()


def test_dbt_schema_drift():
    """Verify that pudl and dbt catalog identical tables and fields.

    If differences are found, detail which index is missing which items.
    """
    all_pudl_tables = [r.name for r in PUDL_PACKAGE.resources]

    pudl_tables_not_in_dbt = set()
    pudl_fields_not_in_dbt = defaultdict(set)

    all_dbt_schema_paths = set((Path.cwd() / "dbt" / "models").glob("**/schema.yml"))

    dbt_tables_not_in_pudl = set()
    dbt_fields_not_in_pudl = defaultdict(set)

    # check pudl -> dbt direction first, then clean up any dbt items
    # we didn't hit along the way
    for table_name in all_pudl_tables:
        schema_path = get_schema_path(table_name)
        try:
            all_dbt_schema_paths.remove(schema_path)
        except KeyError:
            assert not schema_path.exists(), (
                f"Something is wrong with {schema_path}: the file exists as generated from the model path but was not found by glob"
            )
            pudl_tables_not_in_dbt.add(table_name)
            continue
        schema = _load_schema_yaml(schema_path)
        schema_fields = {
            column.name
            for source in schema.sources
            for table in source.tables
            for column in table.columns
        }
        for field_name in [
            f.name for f in PUDL_PACKAGE.get_resource(table_name).schema.fields
        ]:
            try:
                schema_fields.remove(field_name)
            except KeyError:
                pudl_fields_not_in_dbt[table_name].add(field_name)
        if schema_fields:
            dbt_fields_not_in_pudl[table_name].update(schema_fields)
    for schema_path in all_dbt_schema_paths:
        schema = _load_schema_yaml(schema_path)
        dbt_tables_not_in_pudl.update(
            table.name for source in schema.sources for table in source.tables
        )
    assert (
        len(pudl_tables_not_in_dbt) == 0
        and len(pudl_fields_not_in_dbt) == 0
        and len(dbt_tables_not_in_pudl) == 0
        and len(dbt_fields_not_in_pudl) == 0
    ), f"""
dbt schema drift detected:

{
        generate_legible_output(
            pudl_tables_not_in_dbt,
            pudl_fields_not_in_dbt,
            dbt_tables_not_in_pudl,
            dbt_fields_not_in_pudl,
        )
    }
"""
