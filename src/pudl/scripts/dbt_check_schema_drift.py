"""Check that all fields in PUDL exist in dbt.

Requires that `dbt_helper.py` be in your PYTHONPATH.
"""

from collections import defaultdict

from dbt_helper import _get_model_path, _load_schema_yaml, get_data_source

from pudl.metadata.classes import PUDL_PACKAGE

ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


if __name__ == "__main__":
    if ALL_TABLES:
        print("Checking all tables from PUDL_PACKAGE.resources ...")
    missing_fields = defaultdict(set)
    for table_name in ALL_TABLES:
        data_source = get_data_source(table_name)
        model_path = _get_model_path(table_name, data_source)
        schema_path = model_path / "schema.yml"
        schema = _load_schema_yaml(schema_path)
        schema_fields = set()
        for source in schema.sources:
            for table in source.tables:
                for column in table.columns:
                    schema_fields.add(column.name)
        for f in PUDL_PACKAGE.get_resource(table_name).schema.fields:
            if f.name not in schema_fields:
                missing_fields[table_name].add(f.name)
    assert len(missing_fields) == 0, f"""
dbt schema drift detected. Missing fields:

{"\n".join((table_name + ": " + ", ".join(fields)) for table_name, fields in missing_fields.items())}
"""
    print("dbt schema matches PUDL_PACKAGE.")
