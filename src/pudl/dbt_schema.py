from pathlib import Path

from pudl.scripts.dbt_helper import DbtColumn, DbtSchema, DbtSource, DbtTable


def merge_table(machine_table: DbtTable, human_table: DbtTable) -> DbtTable:
    merged_table = machine_table.model_copy(deep=True)

    if human_table.data_tests is not None:
        merged_data_tests = human_table.data_tests
        if merged_table.data_tests is not None:
            merged_data_tests = [*machine_table.data_tests, *human_table.data_tests]
        merged_table.data_tests = merged_data_tests

    if human_table.columns is not None:
        merged_table.columns = merge_columns(machine_table.columns, human_table.columns)

    return merged_table


def merge_columns(
    machine_columns: list[DbtColumn], human_columns: list[DbtColumn]
) -> list[DbtColumn]:
    human_columns_by_name = {c.name: c for c in human_columns}
    merged_columns = machine_columns[:]

    for col in machine_columns:
        matching_human_col = human_columns_by_name.get(col.name)
        if not matching_human_col:
            continue
        if matching_human_col.data_tests:
            if col.data_tests is None:
                merged_data_tests = matching_human_col.data_tests
            else:
                merged_data_tests = [*col.data_tests, *matching_human_col.data_tests]
            col.data_tests = merged_data_tests

    return merged_columns


def merge_sources(
    machine_sources: list[DbtSource], human_sources: list[DbtSource]
) -> list[DbtSource]:
    human_sources_by_name = {s.name: s for s in human_sources}
    merged_sources = machine_sources[:]

    for source in merged_sources:
        matching_human_source = human_sources_by_name.get(source.name)
        if not matching_human_source:
            continue

        if matching_human_source.tables is not None:
            if source.tables is None:
                source.tables = matching_human_source.tables
            else:
                source.tables = merge_tables_by_name(
                    source.tables, matching_human_source.tables
                )

    return merged_sources


def merge_tables_by_name(
    machine_tables: list[DbtTable], human_tables: list[DbtTable]
) -> list[DbtTable]:
    human_tables_by_name = {t.name: t for t in human_tables}
    merged_tables = machine_tables[:]

    for i, table in enumerate(merged_tables):
        matching_human_table = human_tables_by_name.get(table.name)
        if not matching_human_table:
            continue
        merged_tables[i] = merge_table(table, matching_human_table)

    return merged_tables


def _maybe_get_human_schema(human_path: Path) -> DbtSchema:
    if not human_path.exists():
        return DbtSchema(sources=[])
    with human_path.open("r") as f:
        if f.read().strip() == "":
            return DbtSchema(sources=[])
    return DbtSchema.from_yaml(human_path)


def merge_schema(machine_path: Path, human_path: Path) -> DbtSchema:
    machine_schema = DbtSchema.from_yaml(machine_path)
    human_schema = _maybe_get_human_schema(human_path)
    merged_schema = machine_schema.model_copy(deep=False)

    if human_schema.sources is not None:
        if merged_schema.sources is None:
            merged_schema.sources = human_schema.sources
        else:
            merged_schema.sources = merge_sources(
                machine_schema.sources, human_schema.sources
            )

    if human_schema.models is not None:
        if merged_schema.models is None:
            merged_schema.models = human_schema.models
        else:
            merged_schema.models = merge_tables_by_name(
                machine_schema.models, human_schema.models
            )

    return merged_schema
