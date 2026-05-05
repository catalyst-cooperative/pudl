"""A basic CLI to autogenerate dbt data test configurations."""

import json
import re
import sys
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path

import click
import duckdb
import pandas as pd

from pudl.dagster.build import build_defs
from pudl.dbt_schema import DbtSchema, DbtTable, merge_schema_paths
from pudl.logging_helpers import configure_root_logger, get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.validate.dbt import DBT_DIR, build_with_context, dagster_to_dbt_selection
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)


ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


def insert_data_source(parent_dir: Path, table_name: str) -> Path:
    """Convert parent_dir and table_name to parent_dir/data_source/table_name.

    Table name must have <layer>_<source>__<...> format.
    """
    match = re.match(r"_?([a-zA-Z0-9]+)_([a-zA-Z0-9]+)__", table_name)
    if not match:
        raise ValueError(f"{table_name} has no data source segment.")
    data_source = match.group(2)
    return parent_dir / data_source / table_name


UpdateResult = namedtuple("UpdateResult", ["success", "message"])


def _get_row_count_csv_path() -> Path:
    return DBT_DIR / "seeds" / "etl_full_row_counts.csv"


def _get_existing_row_counts() -> pd.DataFrame:
    return pd.read_csv(
        _get_row_count_csv_path(),
        dtype={"partition": "string", "table_name": "string"},
    ).fillna(value="")


def _calculate_row_counts(
    table_name: str,
    partition_expr: str | None = None,
) -> pd.DataFrame:
    table_path = str(PudlPaths().parquet_path(table_name))

    if partition_expr is None:
        partition_expr_sql = "''"
        group_by_clause = ""
    else:
        partition_expr_sql = partition_expr
        group_by_clause = f"GROUP BY {partition_expr_sql}"

    row_count_query = f"""
SELECT
    CAST(COALESCE(CAST({partition_expr_sql} AS VARCHAR), '') AS VARCHAR) AS partition,
    COUNT(*) AS row_count
FROM '{table_path}' {group_by_clause}
    """  # noqa: S608

    new_row_counts = (
        duckdb.sql(row_count_query)
        .df()
        .assign(table_name=table_name)
        .astype({"partition": "string", "table_name": "string"})
        .loc[:, ["table_name", "partition", "row_count"]]
    )

    return new_row_counts


def _combine_row_counts(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.concat([existing, new])
        .drop_duplicates(subset=["partition", "table_name"], keep="last")
        .sort_values(["table_name", "partition"])
    )


def _write_row_counts(row_counts: pd.DataFrame):
    csv_path = _get_row_count_csv_path()
    row_counts.to_csv(csv_path, index=False)


def update_row_counts(
    table_name: str,
    dbt_root: Path,
    clobber: bool = False,
) -> UpdateResult:
    """Generate updated row counts per partition and write to csv file within dbt project."""
    schema_path = insert_data_source(dbt_root / "models", table_name) / "schema.yml"
    schema = DbtSchema.from_yaml(schema_path)
    table = schema.sources[0].tables[0]

    partition_expressions = _extract_row_count_partitions(table)
    if len(partition_expressions) > 1:
        raise ValueError(
            f"Only a single row counts test per table is supported, "
            f"but found {len(partition_expressions)} in {table_name}."
        )

    has_test = bool(partition_expressions)
    existing = _get_existing_row_counts()
    has_existing_row_counts = table_name in existing["table_name"].to_numpy()

    if not has_test and not has_existing_row_counts:
        return UpdateResult(
            success=True,
            message=f"No row count test defined for {table_name}, and no row counts found, nothing to update.",
        )

    if not has_test and not clobber:
        return UpdateResult(
            success=False,
            message=f"Row counts exist for {table_name}, but no row count test is defined. Use clobber to remove.",
        )

    if has_existing_row_counts and not clobber:
        return UpdateResult(
            success=False,
            message=f"Row counts for {table_name} already exist. Use clobber to overwrite.",
        )

    # At this point, we know row counts can be written: either because no row counts
    # exist yet, or because clobber is set.

    # In any case, we remove the old row counts for the table we are refreshing
    filtered = existing[existing["table_name"] != table_name]
    old = existing[existing["table_name"] == table_name]

    # At this point, there is no test defined but there are row counts, and overwrite is allowed, so
    # we want to remove the row counts for this table
    if not has_test:
        # Remove outdated entry
        _write_row_counts(filtered)
        return UpdateResult(
            success=True,
            message=f"Removed {len(existing) - len(filtered)} outdated row counts for {table_name} (no test defined).",
        )

    partition_expr = partition_expressions[0]  # TODO: support multiple partitions
    new = _calculate_row_counts(table_name, partition_expr)

    # Make old and new row counts comparable so we can detect changes
    row_count_idx = ["table_name", "partition"]
    if (
        old.sort_values(by=row_count_idx)
        .reset_index(drop=True)
        .equals(new.sort_values(by=row_count_idx).reset_index(drop=True))
    ):
        return UpdateResult(
            success=True,
            message=f"Row counts for {table_name} are unchanged.",
        )

    # Finally, we reach the case where there are actual row counts to update:
    combined = _combine_row_counts(filtered, new)
    _write_row_counts(combined)

    return UpdateResult(
        success=True,
        message=f"Successfully updated row counts for {table_name}, partitioned by {partition_expr}.",
    )


def update_table_schema(
    table_name: str,
    dbt_root: Path,
) -> UpdateResult:
    """Generate and write out a schema.yaml file defining a new or updated table."""
    machine_path = (
        insert_data_source(dbt_root / "schema_inputs", table_name)
        / "schema.machine.yml"
    )

    machine_schema = DbtSchema.from_table_name(table_name)
    # TODO: does it make sense to persist this machine-generated schema?
    # pros: maybe easier to read for a human: "merge these two files" vs. "merge my file on top of *nebulous thing that only exists in memory*"
    # cons: this persisted file is never actually used, when we merge we want to use whatever's *most* up to date i.e. DbtSchema.from_table_name
    machine_schema.to_yaml(machine_path)
    human_path = (
        insert_data_source(dbt_root / "schema_inputs", table_name) / "schema.human.yml"
    )
    merged_schema = merge_schema_paths(machine_path, human_path)

    machine_path = (
        insert_data_source(dbt_root / "schema_inputs", table_name)
        / "schema.machine.yml"
    )
    merged_path = insert_data_source(dbt_root / "models", table_name) / "schema.yml"

    merged_schema.to_yaml(merged_path)

    return UpdateResult(
        success=True,
        message=f"Successfully generated schema {table_name}.",
    )


def _log_update_result(result: UpdateResult):
    if result.success:
        logger.info(result.message)
    else:
        logger.error(result.message)


def _extract_row_count_partitions(table: DbtTable) -> list[str | None]:
    """Extract partition columns from check_row_counts_per_partition tests in a DbtTable."""
    partitions: list[str | None] = []

    if table.data_tests:
        for test in table.data_tests:
            if "check_row_counts_per_partition" not in test:
                continue
            if not isinstance(test, dict):
                raise ValueError(f"Row counts test expected to be a dictionary: {test}")
            test_def = test.get("check_row_counts_per_partition")
            if isinstance(test_def, dict):
                partitions.append(test_def.get("arguments", {}).get("partition_expr"))

    return partitions


@dataclass
class TableUpdateArgs:
    """Define a single class to collect the args for all table update commands."""

    tables: list[str]
    schema: bool = False
    row_counts: bool = False
    clobber: bool = False


@click.command
@click.argument(
    "tables",
    nargs=-1,
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
    help="Overwrite existing table schema config and row counts. Otherwise, the script will fail if destructive changes are made.",
)
def update_tables(
    tables: list[str],
    clobber: bool,
    schema: bool,
    row_counts: bool,
):
    """Add or update dbt schema configs and row count expectations for PUDL tables.

    The ``tables`` argument can be a single table name, a list of table names, or
    'all'. If 'all' the script will update configurations for for all PUDL tables.

    If ``--clobber`` is set, existing configurations for tables will be overwritten.
    if this does not result in deletions.
    """
    args = TableUpdateArgs(
        tables=list(tables),
        schema=schema,
        row_counts=row_counts,
        clobber=clobber,
    )

    tables = args.tables
    if "all" in tables:
        tables = ALL_TABLES
    elif len(bad_tables := [name for name in tables if name not in ALL_TABLES]) > 0:
        raise RuntimeError(
            f"The following table(s) could not be found in PUDL metadata: {bad_tables}"
        )

    for table_name in tables:
        if args.schema:
            _log_update_result(
                update_table_schema(
                    table_name=table_name,
                    dbt_root=DBT_DIR,
                )
            )
        if args.row_counts:
            _log_update_result(
                update_row_counts(
                    table_name=table_name,
                    dbt_root=DBT_DIR,
                    clobber=args.clobber,
                )
            )


@click.command()
@click.option(
    "--select",
    help="dbt selector for the asset(s) you want to validate. Syntax "
    "documentation at https://docs.getdbt.com/reference/node-selection/syntax",
)
@click.option(
    "--asset-select",
    "-a",
    help=(
        "*DAGSTER* selector for the asset(s) you want to validate. "
        "This gets translated into a dbt selection. For example, you can "
        "use '+key:\"out_eia__yearly_generators\"' to validate "
        "out_eia_yearly_generators and its upstream assets. Syntax "
        "documentation at https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference "
    ),
)
@click.option(
    "--exclude",
    help="dbt selector for the asset(s) you want to exclude from validation. Syntax "
    "documentation at https://docs.getdbt.com/reference/node-selection/syntax",
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
    dry_run: bool = False,
) -> None:
    """Validate a selection of dbt nodes.

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
            node_selection = dagster_to_dbt_selection(asset_select, build_defs())
        else:
            node_selection = "*"

    build_params = {
        "node_selection": node_selection,
        "node_exclusion": exclude,
        "dbt_target": "etl-full",
    }

    if dry_run:
        click.echo(
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
def main() -> int:
    """Script for auto-generating dbt configuration and migrating existing tests.

    This CLI currently provides the following sub-commands:

    update-tables: which can update or create a dbt table (model) schema.yml file under
    the ``dbt/models`` repo. These configuration files tell dbt about the structure of
    the table and what data tests are specified for it. The script can also generate or
    update the expected row counts for existing tables, assuming they have been
    materialized to parquet files and are sitting in your $PUDL_OUTPUT directory.

    validate: run validation tests for a selection of dbt nodes.

    Run ``dbt_helper {command} --help`` for detailed usage on each command.
    """
    return 0


main.add_command(update_tables)
main.add_command(validate)

# Alias for callers that import dbt_helper by name (e.g. integration tests).
dbt_helper = main


if __name__ == "__main__":
    configure_root_logger()
    sys.exit(main())
