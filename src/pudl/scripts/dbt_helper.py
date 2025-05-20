"""A basic CLI to autogenerate dbt data test configurations."""

from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import click
import duckdb
import pandas as pd
import yaml
from pydantic import BaseModel

from pudl.logging_helpers import configure_root_logger, get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

configure_root_logger()
logger = get_logger(__file__)

ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


class DbtColumn(BaseModel):
    """Define yaml structure of a dbt column."""

    name: str
    description: str | None = None
    data_tests: list | None = None
    meta: dict | None = None
    tags: list[str] | None = None

    def add_column_tests(self, column_tests: list) -> "DbtColumn":
        """Add data tests to columns in dbt config."""
        data_tests = self.data_tests if self.data_tests is not None else []
        return self.model_copy(update={"data_tests": data_tests + column_tests})


class DbtTable(BaseModel):
    """Define yaml structure of a dbt table."""

    name: str
    description: str | None = None
    data_tests: list | None = None
    columns: list[DbtColumn]
    meta: dict | None = None
    tags: list[str] | None = None
    config: dict | None = None  # only for models

    def add_source_tests(self, source_tests: list) -> "DbtSource":
        """Add data tests to source in dbt config."""
        data_tests = self.data_tests if self.data_tests is not None else []
        return self.model_copy(update={"data_tests": data_tests + source_tests})

    def add_column_tests(self, column_tests: dict[str, list]) -> "DbtSource":
        """Add data tests to columns in dbt config."""
        columns = {column.name: column for column in self.columns}
        columns.update(
            {
                name: columns[name].add_column_tests(tests)
                for name, tests in column_tests.items()
            }
        )

        return self.model_copy(update={"columns": list(columns.values())})

    @staticmethod
    def get_row_count_test_dict(table_name: str, partition_column: str):
        """Return a dictionary with a dbt row count data test encoded in a dict."""
        return [
            {
                "check_row_counts_per_partition": {
                    "table_name": table_name,
                    "partition_column": partition_column,
                }
            }
        ]

    @classmethod
    def from_table_name(cls, table_name: str, partition_column: str) -> "DbtSchema":
        """Construct configuration defining table from PUDL metadata."""
        return cls(
            data_tests=cls.get_row_count_test_dict(table_name, partition_column),
            name=table_name,
            columns=[
                DbtColumn(name=f.name)
                for f in PUDL_PACKAGE.get_resource(table_name).schema.fields
            ],
        )


class DbtSource(BaseModel):
    """Define basic dbt yml structure to add a pudl table as a dbt source."""

    name: str = "pudl"
    tables: list[DbtTable]
    data_tests: list | None = None
    description: str | None = None
    meta: dict | None = None

    def add_source_tests(self, source_tests: list) -> "DbtSource":
        """Add data tests to source in dbt config."""
        return self.model_copy(
            update={"tables": [self.tables[0].add_source_tests(source_tests)]}
        )

    def add_column_tests(self, column_tests: dict[list]) -> "DbtSource":
        """Add data tests to columns in dbt config."""
        return self.model_copy(
            update={"tables": [self.tables[0].add_column_tests(column_tests)]}
        )


class DbtSchema(BaseModel):
    """Define basic structure of a dbt models yaml file."""

    version: int = 2
    sources: list[DbtSource]
    models: list[DbtTable] | None = None

    def add_source_tests(
        self, source_tests: list, model_name: str | None = None
    ) -> "DbtSchema":
        """Add data tests to source in dbt config."""
        if model_name is None:
            schema = self.model_copy(
                update={"sources": [self.sources[0].add_source_tests(source_tests)]}
            )
        else:
            models = {model.name: model for model in self.models}
            models[model_name] = models[model_name].add_source_tests(source_tests)
            schema = self.model_copy(update={"models": list(models.values())})

        return schema

    def add_column_tests(
        self, column_tests: dict[list], model_name: str | None = None
    ) -> "DbtSchema":
        """Add data tests to columns in dbt config."""
        if model_name is None:
            schema = self.model_copy(
                update={"sources": [self.sources[0].add_column_tests(column_tests)]}
            )
        else:
            models = {model.name: model for model in self.models}
            models[model_name] = models[model_name].add_column_tests(column_tests)
            schema = self.model_copy(update={"models": list(models.values())})

        return schema

    @classmethod
    def from_table_name(cls, table_name: str, partition_column: str) -> "DbtSchema":
        """Construct configuration defining table from PUDL metadata."""
        return cls(
            sources=[
                DbtSource(
                    version=2,
                    tables=[DbtTable.from_table_name(table_name, partition_column)],
                )
            ],
        )

    @classmethod
    def from_yaml(cls, schema_path: Path) -> "DbtSchema":
        """Load a DbtSchema object from a YAML file."""
        with schema_path.open("r") as schema_yaml:
            return cls.model_validate(yaml.safe_load(schema_yaml))


def diff_scalar(field, old, new):
    return {field: {"old": old, "new": new}} if old != new else {}

def diff_list(field, old, new):
    old_set, new_set = set(old or []), set(new or [])
    added, removed = list(new_set - old_set), list(old_set - new_set)
    return {field: {"added": added, "removed": removed}} if added or removed else {}

def diff_dict_keys(field, old, new):
    old_keys, new_keys = set((old or {}).keys()), set((new or {}).keys())
    added, removed = list(new_keys - old_keys), list(old_keys - new_keys)
    return {field: {"added": added, "removed": removed}} if added or removed else {}

def diff_dbt_column(o, n):
    diff = {}
    diff.update(diff_scalar("description", o.description, n.description))
    diff.update(diff_list("data_tests", list(map(str, o.data_tests or [])), list(map(str, n.data_tests or []))))
    diff.update(diff_list("tags", o.tags, n.tags))
    diff.update(diff_dict_keys("meta", o.meta, n.meta))
    return diff

def diff_dbt_table(o, n):
    diff = {}
    diff.update(diff_scalar("description", o.description, n.description))
    diff.update(diff_list("data_tests", list(map(str, o.data_tests or [])), list(map(str, n.data_tests or []))))
    diff.update(diff_dict_keys("meta", o.meta, n.meta))
    diff.update(diff_list("tags", o.tags, n.tags))
    diff.update(diff_dict_keys("config", o.config, n.config))
    # Columns
    cols = {}
    old_cols = {c.name: c for c in o.columns}
    new_cols = {c.name: c for c in n.columns}
    for col in set(old_cols) | set(new_cols):
        if col not in old_cols:
            cols[col] = {"added": new_cols[col].dict(exclude_none=True)}
        elif col not in new_cols:
            cols[col] = {"removed": old_cols[col].dict(exclude_none=True)}
        else:
            d = diff_dbt_column(old_cols[col], new_cols[col])
            if d:
                cols[col] = d
    if cols: diff["columns"] = cols
    return diff

def diff_dbt_source(o, n):
    diff = {}
    diff.update(diff_scalar("description", o.description, n.description))
    diff.update(diff_list("data_tests", list(map(str, o.data_tests or [])), list(map(str, n.data_tests or []))))
    diff.update(diff_dict_keys("meta", o.meta, n.meta))
    # Tables
    tables = {}
    old_tbl = {t.name: t for t in o.tables}
    new_tbl = {t.name: t for t in n.tables}
    for tbl in set(old_tbl) | set(new_tbl):
        if tbl not in old_tbl:
            tables[tbl] = {"added": new_tbl[tbl].dict(exclude_none=True)}
        elif tbl not in new_tbl:
            tables[tbl] = {"removed": old_tbl[tbl].dict(exclude_none=True)}
        else:
            d = diff_dbt_table(old_tbl[tbl], new_tbl[tbl])
            if d:
                tables[tbl] = d
    if tables: diff["tables"] = tables
    return diff

def diff_dbt_schema(o, n):
    diff = {}
    diff.update(diff_scalar("version", o.version, n.version))
    # Sources
    sources = {}
    old_src = {s.name: s for s in o.sources}
    new_src = {s.name: s for s in n.sources}
    for s in set(old_src) | set(new_src):
        if s not in old_src:
            sources[s] = {"added": new_src[s].dict(exclude_none=True)}
        elif s not in new_src:
            sources[s] = {"removed": old_src[s].dict(exclude_none=True)}
        else:
            d = diff_dbt_source(old_src[s], new_src[s])
            if d:
                sources[s] = d
    if sources: diff["sources"] = sources
    # Models (if present)
    if o.models or n.models:
        models = {}
        old_mod = {m.name: m for m in o.models or []}
        new_mod = {m.name: m for m in n.models or []}
        for m in set(old_mod) | set(new_mod):
            if m not in old_mod:
                models[m] = {"added": new_mod[m].dict(exclude_none=True)}
            elif m not in new_mod:
                models[m] = {"removed": old_mod[m].dict(exclude_none=True)}
            else:
                d = diff_dbt_table(old_mod[m], new_mod[m])
                if d:
                    models[m] = d
        if models: diff["models"] = models
    return diff


def _has_removals_or_modifications(diff: dict) -> bool:
    """Recursively checks if any removal or modification exists in a diff dict."""
    if isinstance(diff, dict):
        for key, value in diff.items():
            if key in {"removed", "modified"} and value:
                return True
            if _has_removals_or_modifications(value):
                return True
    elif isinstance(diff, list):
        for item in diff:
            if _has_removals_or_modifications(item):
                return True
    return False


def get_data_source(table_name: str) -> str:
    """Return data source for a table or 'output' if there's more than one source."""
    resource = PUDL_PACKAGE.get_resource(table_name)

    return "output" if len(resource.sources) > 1 else resource.sources[0].name


UpdateResult = namedtuple("UpdateResult", ["success", "message"])


def _get_local_table_path(table_name):
    return str(PudlPaths().parquet_path(table_name))


def _get_model_path(table_name: str, data_source: str) -> Path:
    return Path("./dbt") / "models" / data_source / table_name


def _get_row_count_csv_path(target: str = "etl-full") -> Path:
    if target == "etl-fast":
        return Path("./dbt") / "seeds" / "etl_fast_row_counts.csv"
    return Path("./dbt") / "seeds" / "etl_full_row_counts.csv"


def _get_existing_row_counts(target: str = "etl-full") -> pd.DataFrame:
    return pd.read_csv(_get_row_count_csv_path(target), dtype={"partition": str})


def _calculate_row_counts(
    table_name: str,
    partition_column: str = "report_year",
) -> pd.DataFrame:
    table_path = _get_local_table_path(table_name)

    if partition_column == "report_year":
        row_count_query = (
            f"SELECT {partition_column} as partition, COUNT(*) as row_count "  # noqa: S608
            f"FROM '{table_path}' GROUP BY {partition_column}"  # noqa: S608
        )
    elif partition_column in ["report_date", "datetime_utc"]:
        row_count_query = (
            f"SELECT CAST(YEAR({partition_column}) as VARCHAR) as partition, COUNT(*) as row_count "  # noqa: S608
            f"FROM '{table_path}' GROUP BY YEAR({partition_column})"  # noqa: S608
        )
    else:
        row_count_query = f"SELECT '' as partition, COUNT(*) as row_count FROM '{table_path}'"  # noqa: S608

    new_row_counts = duckdb.sql(row_count_query).df().astype({"partition": str})
    new_row_counts["table_name"] = table_name

    return new_row_counts


def _combine_row_counts(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.concat([existing, new])
        .drop_duplicates(subset=["partition", "table_name"], keep="last")
        .sort_values(["table_name", "partition"])
    )


def _write_row_counts(row_counts: pd.DataFrame, target: str = "etl-full"):
    csv_path = _get_row_count_csv_path(target)
    row_counts.to_csv(csv_path, index=False)


def update_row_counts(
    table_name: str,
    partition_column: str = "report_year",
    target: str = "etl-full",
    clobber: bool = False,
    update: bool = False,
) -> UpdateResult:
    """Generate updated row counts per partition and write to csv file within dbt project."""
    existing = _get_existing_row_counts(target)
    if table_name in existing["table_name"].to_numpy() and not (clobber or update):
        return UpdateResult(
            success=False,
            message=f"Row counts for {table_name} already exist (run with clobber or update to overwrite).",
        )

    new = _calculate_row_counts(table_name, partition_column)
    combined = _combine_row_counts(existing, new)
    _write_row_counts(combined, target)

    return UpdateResult(
        success=True,
        message=f"Successfully updated row count table with counts from {table_name}.",
    )


def _print_schema_diff(diff: dict, old_schema: DbtSchema, new_schema: DbtSchema):
    """Print old and new YAML, and summary of schema changes."""
    print("\n======================")
    print("📜 Old YAML:")
    print(yaml.dump(old_schema.model_dump(exclude_none=True), sort_keys=False))
    print("\n======================")
    print("📜 New YAML:")
    print(yaml.dump(new_schema.model_dump(exclude_none=True), sort_keys=False))
    print("\n======================")

    print("🔍 Schema Diff Summary:\n")
    _print_schema_diff_summary(diff))

    print("======================\n")


def _print_schema_diff_summary(diff: dict, indent: int = 0):
    """
    Recursively print a summary of the schema diff.

    The diff is expected to be a nested dictionary as produced by diff_dbt_schema.
    """
    pad = " " * indent
    for key, value in diff.items():
        if isinstance(value, dict):
            # If the dictionary is a leaf-level diff (has 'added', 'removed', etc.), print it directly.
            if any(sub_key in value for sub_key in ("added", "removed", "old", "new")):
                if "added" in value:
                    print(f"{pad}{key} added:")
                    # For added/removed items, value can be a dict or a direct value.
                    if isinstance(value["added"], dict):
                        _print_schema_diff_summary(value["added"], indent + 2)
                    else:
                        print(f"{pad}  {value['added']}")
                if "removed" in value:
                    print(f"{pad}{key} removed:")
                    if isinstance(value["removed"], dict):
                        _print_schema_diff_summary(value["removed"], indent + 2)
                    else:
                        print(f"{pad}  {value['removed']}")
                if "old" in value and "new" in value:
                    print(f"{pad}{key} modified:")
                    print(f"{pad}  old: {value['old']}")
                    print(f"{pad}  new: {value['new']}")
            else:
                # Otherwise, this key groups further nested diffs.
                print(f"{pad}{key}:")
                _print_schema_diff_summary(value, indent + 2)
        else:
            # In case the value isn't a dict (unlikely in our diff structure)
            print(f"{pad}{key}: {value}")


def _write_dbt_schema(schema_path: Path, schema: DbtSchema):
    with schema_path.open("w") as schema_file:
        yaml.dump(
            schema.model_dump(exclude_none=True),
            schema_file,
            default_flow_style=False,
            sort_keys=False,
            width=float("inf"),
        )


def update_table_schema(
    table_name: str,
    data_source: str,
    partition_column: str = "report_year",
    clobber: bool = False,
    update: bool = False,
) -> UpdateResult:
    """Generate and write out a schema.yaml file defining a new or updated table."""
    model_path = _get_model_path(table_name, data_source)
    schema_path = model_path / "schema.yml"

    if model_path.exists() and not (clobber or update):
        return UpdateResult(
            success=False,
            message=f"DBT configuration already exists for table {table_name} and clobber or update is not set.",
        )

    new_schema = DbtSchema.from_table_name(
        table_name, partition_column=partition_column
    )

    if model_path.exists() and update:
        # Load existing schema
        old_schema = DbtSchema.from_yaml(schema_path)

        # Generate the diff report
        diff = diff_dbt_schema(old_schema, new_schema)
        if _has_removals_or_modifications(diff):
            print("\n⚠️ WARNING: Some elements would be deleted by this update!")
            _print_schema_diff(diff, old_schema, new_schema)
            return UpdateResult(
                success=False,
                message=f"DBT configuration for table {table_name} has information the would be deleted. Update manually or run with clobber.",
            )

    model_path.mkdir(parents=True, exist_ok=True)
    _write_dbt_schema(schema_path, new_schema)

    return UpdateResult(
        success=True,
        message=f"Wrote schema config for table {table_name} at {schema_path}.",
    )


def _log_update_result(result: UpdateResult):
    if result.success:
        logger.info(result.message)
    else:
        logger.error(result.message)


def _infer_partition_column(table_name: str) -> str:
    all_columns = [c.name for c in PUDL_PACKAGE.get_resource(table_name).schema.fields]
    if (
        (partition_column := "report_year") in all_columns
        or (partition_column := "report_date") in all_columns
        or (partition_column := "datetime_utc") in all_columns
    ):
        return partition_column
    return None


@dataclass
class TableUpdateArgs:
    """Define a single class to collect the args for all table update commands."""

    tables: list[str]
    target: Literal["etl-full", "etl-fast"] = "etl-full"
    schema: bool = False
    row_counts: bool = False
    clobber: bool = False
    update: bool = False


@click.command
@click.argument(
    "tables",
    nargs=-1,
)
@click.option(
    "--target",
    default="etl-full",
    type=click.Choice(["etl-full", "etl-fast"]),
    show_default=True,
    help="What dbt target should be used as the source of new row counts.",
)
@click.option(
    "--schema/--no-schema",
    default=False,
    help="Update source table schema.yml configs.",
)
@click.option(
    "--row-counts/--no-row-counts",
    default=False,
    help="Update source table row count expectations.",
)
@click.option(
    "--clobber/--no-clobber",
    default=False,
    help="Overwrite existing table schema config and row counts. Otherwise, the script will fail if the table configuration already exists.",
)
@click.option(
    "--update/--no-update",
    default=False,
    help="Allow the table schema to be updated if the new schema is a superset of the existing schema.",
)
def update_tables(
    tables: list[str],
    target: str,
    clobber: bool,
    update: bool,
    schema: bool,
    row_counts: bool,
):
    """Add or update dbt schema configs and row count expectations for PUDL tables.

    The ``tables`` argument can be a single table name, a list of table names, or
    'all'. If 'all' the script will update configurations for for all PUDL tables.

    If ``--clobber`` is set, existing configurations for tables will be overwritten.
    If ``--update`` is set, existing configurations for tables will be updated only
    if this does not result in deletions.
    """
    args = TableUpdateArgs(
        tables=list(tables),
        target=target,
        schema=schema,
        row_counts=row_counts,
        clobber=clobber,
        update=update,
    )

    tables = args.tables
    if "all" in tables:
        tables = ALL_TABLES
    elif len(bad_tables := [name for name in tables if name not in ALL_TABLES]) > 0:
        raise RuntimeError(
            f"The following table(s) could not be found in PUDL metadata: {bad_tables}"
        )

    for table_name in tables:
        data_source = get_data_source(table_name)
        partition_column = _infer_partition_column(table_name)
        if args.schema:
            _log_update_result(
                update_table_schema(
                    table_name,
                    data_source,
                    partition_column=partition_column,
                    clobber=args.clobber,
                    update=args.update,
                )
            )
        if args.row_counts:
            _log_update_result(
                update_row_counts(
                    table_name=table_name,
                    partition_column=partition_column,
                    target=args.target,
                    clobber=args.clobber,
                    update=args.update,
                )
            )


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def dbt_helper():
    """Script for auto-generating dbt configuration and migrating existing tests.

    This CLI currently provides one sub-command: ``update-tables`` which can update or
    create a dbt table (model) schema.yml file under the ``dbt/models`` repo. These
    configuration files tell dbt about the structure of the table and what data tests
    are specified for it. It also adds a (required) row count test by default. The
    script can also generate or update the expected row counts for existing tables,
    assuming they have been materialized to parquet files and are sitting in your
    $PUDL_OUT directory.

    Run ``dbt_helper {command} --help`` for detailed usage on each command.
    """


dbt_helper.add_command(update_tables)


if __name__ == "__main__":
    dbt_helper()
