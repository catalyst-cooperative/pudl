"""A basic CLI to autogenerate dbt data test configurations."""

import json
import re
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
) -> UpdateResult:
    """Generate updated row counts per partition and write to csv file within dbt project."""
    existing = _get_existing_row_counts(target)
    if table_name in existing["table_name"].to_numpy() and not clobber:
        return UpdateResult(
            success=False,
            message=f"Row counts for {table_name} already exist (run with clobber to overwrite).",
        )

    new = _calculate_row_counts(table_name, partition_column)
    combined = _combine_row_counts(existing, new)
    _write_row_counts(combined, target)

    return UpdateResult(
        success=True,
        message=f"Successfully updated row count table with counts from {table_name}.",
    )


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
) -> UpdateResult:
    """Generate and write out a schema.yaml file defining a new or updated table."""
    model_path = _get_model_path(table_name, data_source)
    if model_path.exists() and not clobber:
        return UpdateResult(
            success=False,
            message=f"DBT configuration already exists for table {table_name} and clobber is not set.",
        )

    table_config = DbtSchema.from_table_name(
        table_name, partition_column=partition_column
    )
    model_path.mkdir(parents=True, exist_ok=True)
    _write_dbt_schema(model_path / "schema.yml", table_config)

    return UpdateResult(
        success=True,
        message=f"Wrote schema config for table {table_name} at {model_path / 'schema.yml'}.",
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


def _get_tables(tables: list[str]) -> list[str]:
    if "all" in tables:
        tables = ALL_TABLES
    elif len(bad_tables := [name for name in tables if name not in ALL_TABLES]) > 0:
        raise RuntimeError(
            f"The following table(s) could not be found in PUDL metadata: {bad_tables}"
        )

    return tables


@dataclass
class TableUpdateArgs:
    """Define a single class to collect the args for all table update commands."""

    tables: list[str]
    target: Literal["etl-full", "etl-fast"] = "etl-full"
    schema: bool = False
    row_counts: bool = False
    clobber: bool = False


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
def update_tables(
    tables: list[str],
    target: str,
    clobber: bool,
    schema: bool,
    row_counts: bool,
):
    """Add or update dbt schema configs and row count expectations for PUDL tables.

    The ``tables`` argument can be a single table name, a list of table names, or
    'all'. If 'all' the script will update configurations for for all PUDL tables.

    If ``--clobber`` is set, existing configurations for tables will be overwritten.
    """
    args = TableUpdateArgs(
        tables=list(tables),
        target=target,
        schema=schema,
        row_counts=row_counts,
        clobber=clobber,
    )

    tables = _get_tables(args.tables)

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
                )
            )
        if args.row_counts:
            _log_update_result(
                update_row_counts(
                    table_name=table_name,
                    partition_column=partition_column,
                    target=args.target,
                    clobber=args.clobber,
                )
            )


@click.command
@click.option(
    "--verbose",
    type=bool,
    default=False,
    is_flag=True,
    help="Report exact year by year row count changes, otherwise report total percent change.",
)
@click.option(
    "--table",
    type=str,
    default="all",
    help="Specify a single table to get row count diffs for. If not set this command will display row count diffs for all tables.",
)
def summarize_row_count_diffs(table: str, verbose: bool = False):
    """Load all row count failures from duckdb file and summarize.

    After running dbt tests, this command can be used to summarize row count changes.
    It uses a json file dbt saves at 'dbt/target/run_results.json'. The verbose option
    will output the exact changes per partition, while the default will output the total
    percent change in row counts.

    Example usage:
        dbt_helper summarize-row-count-diffs
    """
    tables = _get_tables([table])
    # Load failures
    with Path("./dbt/target/run_results.json").open() as f:
        failures = [
            result
            for result in json.load(f)["results"]
            if (result["status"] == "fail")
            and ("check_row_counts" in result["unique_id"])
        ]

    # Connect to duckdb database
    db = duckdb.connect(PudlPaths().output_dir / "pudl_dbt_tests.duckdb")

    # Load expected row counts and sum all partitions so we can calculate percent change
    etl_full_row_counts = (
        _get_existing_row_counts().groupby("table_name").sum()["row_count"]
    )
    etl_fast_row_counts = (
        _get_existing_row_counts("etl-fast").groupby("table_name").sum()["row_count"]
    )

    # Loop through failures and output results
    extract_table_name_pattern = r"table_name = '(\w+)'"
    for failure in failures:
        # Extract table name from json
        table_name = re.search(
            extract_table_name_pattern, failure["compiled_code"]
        ).group(1)

        # Filter to requested table
        if table_name not in tables:
            continue

        # Get row count diffs from duckdb
        row_counts_df = db.sql(f"SELECT * FROM {failure['relation_name']}").df()  # noqa: S608

        # Log per partition diffs
        if verbose:
            logger.warning(
                f"Row count failures found for table: {table_name}\n{row_counts_df}"
            )
        # Otherwise log percent change across all partitions
        else:
            total_diff = (
                row_counts_df["observed_count"] - row_counts_df["expected_count"]
            ).sum()
            if "etl_fast" in failure["compiled_code"]:
                total_count = etl_fast_row_counts[table_name]
            else:
                total_count = etl_full_row_counts[table_name]

            percent_diff = (total_diff / total_count) * 100
            logger.warning(
                f"Row counts for table {table_name} changed by {percent_diff}%"
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


dbt_helper.add_command(summarize_row_count_diffs)
dbt_helper.add_command(update_tables)


if __name__ == "__main__":
    dbt_helper()
