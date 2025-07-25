"""A basic CLI to autogenerate dbt data test configurations."""

import json
from collections import namedtuple
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path
from typing import Any, Literal

import click
import duckdb
import pandas as pd
import yaml
from deepdiff import DeepDiff
from pydantic import BaseModel

from pudl.dbt_wrapper import build_with_context, dagster_to_dbt_selection
from pudl.logging_helpers import configure_root_logger, get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

configure_root_logger()
logger = get_logger(__name__)
logger.parent.propagate = False


ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


def _prettier_yaml_dumps(yaml_contents: dict[str, Any]) -> str:
    """Dump YAML to string that Prettier likes."""

    class PrettierCompatibleDumper(yaml.Dumper):
        """Custom Dumper that indents lists like prettier does.

        Default dumper behavior::

            foo:
            - name: a
            - name: b

        prettier behavior::

            foo:
              - name: a
              - name: b

        NOTE (2025-07-10): this code was generated by LLM and Does The
        Right Thing. It appears to do so by forcing the ``indentless`` parameter to
        always be ``False``. If at any point this stops working we should convert
        to using ``ruamel`` which has much more customizable formatting with actual
        documentation.
        """

        def increase_indent(self, flow=False, indentless=False):
            return super().increase_indent(flow, False)

    return yaml.dump(
        yaml_contents,
        default_flow_style=False,
        Dumper=PrettierCompatibleDumper,
        indent=2,
        sort_keys=False,
        width=float("inf"),
    )


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

    def to_yaml(self, schema_path: Path):
        """Write DbtSchema object to YAML file."""
        with schema_path.open("w") as schema_file:
            yaml_output = _prettier_yaml_dumps(self.model_dump(exclude_none=True))
            schema_file.write(yaml_output)


def schema_has_removals_or_modifications(diff: DeepDiff) -> bool:
    """Check if the DeepDiff includes any removals or modifications."""
    change_keys = {
        "values_changed",
        "type_changes",
        "dictionary_item_removed",
        "iterable_item_removed",
        "attribute_deleted",
    }

    return any(key in diff and diff[key] for key in change_keys)


def _log_schema_diff(old_schema: DbtSchema, new_schema: DbtSchema):
    """Print colored summary of schema changes."""
    summary = _schema_diff_summary(old_schema, new_schema)
    click.echo("Proposed schema changes:")
    for line in summary:
        if line.startswith("- "):
            click.secho(line, fg="red")
        elif line.startswith("+ "):
            click.secho(line, fg="green")
        else:
            click.secho(line)


def _schema_diff_summary(old_schema: DbtSchema, new_schema: DbtSchema):
    """Return a summary of schema changes based on YAML output."""
    stripped = (
        line.rstrip()
        for line in unified_diff(
            _prettier_yaml_dumps(old_schema.model_dump(exclude_none=True)).split("\n"),
            _prettier_yaml_dumps(new_schema.model_dump(exclude_none=True)).split("\n"),
            fromfile="old_schema",
            tofile="new_schema",
        )
    )
    return [line for line in stripped if line != ""]


def get_data_source(table_name: str) -> str:
    """Return the data source element of the table's name."""
    return table_name.strip("_").split("_")[1]


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
        row_count_query = (
            f"SELECT '' as partition, COUNT(*) as row_count FROM '{table_path}'"  # noqa: S608
        )

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
        diff = DeepDiff(
            old_schema.model_dump(exclude_none=True),
            new_schema.model_dump(exclude_none=True),
            ignore_order=True,
            verbose_level=2,
            view="tree",
        )

        if schema_has_removals_or_modifications(diff):
            _log_schema_diff(old_schema, new_schema)
            # TODO 2025-07-11: perhaps we can integrate this `git add -p` flow directly
            # into the --clobber command?
            return UpdateResult(
                success=False,
                message=f"DBT configuration for table {table_name} has "
                "information that would be deleted. Update manually, or: "
                "1) re-run with --clobber, 2) run `git add dbt/models -p` "
                "and follow the instructions for recovering the deleted info",
            )

    model_path.mkdir(parents=True, exist_ok=True)
    new_schema.to_yaml(schema_path)

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

    if args.clobber and args.update:
        raise click.UsageError(
            "Cannot use --clobber and --update at the same time. Choose one."
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


@click.command()
@click.option(
    "--select",
    help="DBT selector for the asset(s) you want to validate. Syntax "
    "documentation at https://docs.getdbt.com/reference/node-selection/syntax",
)
@click.option(
    "--asset-select",
    "-a",
    help=(
        "*DAGSTER* selector for the asset(s) you want to validate. "
        "This gets translated into a DBT selection. For example, you can "
        "use '+key:\"out_eia__yearly_generators\"' to validate "
        "out_eia_yearly_generators and its upstream assets. Syntax "
        "documentation at https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference "
    ),
)
@click.option(
    "--exclude",
    help="DBT selector for the asset(s) you want to exclude from validation. Syntax "
    "documentation at https://docs.getdbt.com/reference/node-selection/syntax",
)
@click.option(
    "--target",
    default="etl-full",
    type=click.Choice(["etl-full", "etl-fast"]),
    help="DBT target - etl-full (default) or etl-fast.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="If dry, will print out the parameters we would pass to dbt, but not "
    "actually run the validation tests. Defaults to not-dry.",
)
def validate(
    select: str | None = None,
    asset_select: str | None = None,
    exclude: str | None = None,
    target: str = "etl-full",
    dry_run: bool = False,
) -> None:
    """Validate a selection of DBT nodes.

    Wraps the ``dbt build`` command line so we can annotate the result with the
    actual data that was returned from the test query.

    Understands how to translate Dagster asset selection syntax into dbt node
    selections via the --asset-select flag.

    Default behavior if you do not pass `--asset-select` or `--select` is to
    validate everything.

    Usage examples:

    Run all the checks for one asset:

        $ dbt_helper validate --asset-select "key:out_eia__yearly_generators"

    Run the checks for one specific dbt node:

        $ dbt_helper validate --select "source:pudl_dbt.pudl.out_eia__yearly_generators"

    Run checks for an asset and all its upstream dependencies:

        $ dbt_helper validate --asset-select "+key:out_eia__yearly_generators"

    Exclude the row count tests:

        $ dbt_helper validate --asset-select "+key:out_eia__yearly_generators" --exclude "*check_row_counts*"



    """
    if select is not None:
        if asset_select is not None:
            raise click.UsageError(
                "You can't pass --select and --asset-select at the same time."
            )
        node_selection = select
    else:
        if asset_select is not None:
            node_selection = dagster_to_dbt_selection(asset_select)
        else:
            node_selection = "*"

    build_params = {
        "node_selection": node_selection,
        "dbt_target": target,
        "node_exclusion": exclude,
    }

    if dry_run:
        logger.info(
            f"Dry run - would build with these params: {json.dumps(build_params)}"
        )
        return

    test_result = build_with_context(**build_params)
    if not test_result.success:
        raise AssertionError(
            f"failure contexts:\n{test_result.format_failure_contexts()}"
        )


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def dbt_helper():
    """Script for auto-generating dbt configuration and migrating existing tests.

    This CLI currently provides the following sub-commands:

    * ``update-tables`` which can update or create a dbt table (model)
      schema.yml file under the ``dbt/models`` repo. These configuration files
      tell dbt about the structure of the table and what data tests are specified
      for it. It also adds a (required) row count test by default. The script
      can also generate or update the expected row counts for existing tables,
      assuming they have been materialized to parquet files and are sitting in
      your $PUDL_OUT directory.
    * ``validate``: run validation tests for a selection of DBT nodes.

    Run ``dbt_helper {command} --help`` for detailed usage on each command.
    """


dbt_helper.add_command(update_tables)
dbt_helper.add_command(validate)


if __name__ == "__main__":
    dbt_helper()
