"""A basic CLI to autogenerate dbt yml."""

from collections import namedtuple
from pathlib import Path

import click
import pandas as pd
import yaml
from pydantic import BaseModel

from pudl.etl import defs
from pudl.logging_helpers import configure_root_logger, get_logger
from pudl.metadata.classes import PUDL_PACKAGE

configure_root_logger()
logger = get_logger(__file__)

ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


class DbtColumn(BaseModel):
    """Define yaml structure of a dbt column."""

    name: str


class DbtTable(BaseModel):
    """Define yaml structure of a dbt table."""

    name: str
    data_tests: list
    columns: list[DbtColumn]

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


class DbtSchema(BaseModel):
    """Define basic structure of a dbt models yaml file."""

    version: int = 2
    sources: list[DbtSource]

    @classmethod
    def from_table_name(cls, table_name: str, partition_column: str) -> "DbtSchema":
        """Construct configuration defining table from PUDL metadata."""
        return cls(
            sources=[
                DbtSource(
                    tables=[DbtTable.from_table_name(table_name, partition_column)]
                )
            ]
        )


def get_data_source(table_name: str) -> str:
    """Return data source for a table or 'output' if there's more than one source."""
    resource = PUDL_PACKAGE.get_resource(table_name)
    if len(resource.sources) > 1:
        return "output"

    return resource.sources[0].name


AddTableResult = namedtuple("AddTableResult", ["success", "message"])


def _get_nightly_url(table_name: str) -> str:
    return f"https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/{table_name}.parquet"


def _get_model_path(table_name: str, data_source: str) -> Path:
    return Path("./dbt") / "models" / data_source / table_name


def _get_row_count_csv_path() -> Path:
    return Path("./dbt") / "seeds" / "row_counts.csv"


def generate_row_counts(
    table_name: str,
    partition_column: str = "report_year",
    use_local_tables: bool = False,
    clobber: bool = False,
) -> AddTableResult:
    """Generate row counts per partition and write to csv file within dbt project."""
    # Get existing row counts table
    row_counts_df = pd.read_csv(_get_row_count_csv_path())

    if table_name in row_counts_df["table_name"].to_numpy() and not clobber:
        return AddTableResult(
            success=False,
            message=f"There are already row counts for table {table_name} in row counts table and clobber is not set.",
        )

    # Load table of interest
    if not use_local_tables:
        df = pd.read_parquet(_get_nightly_url(table_name))
    else:
        df = defs.load_asset_value(table_name)

    new_row_counts = (
        df.groupby([partition_column])
        .size()
        .reset_index(name="row_count")
        .rename(columns={partition_column: "partition"})
        .astype({"partition": "str"})
    )
    new_row_counts["table_name"] = table_name

    all_row_counts = pd.concat([row_counts_df, new_row_counts]).drop_duplicates(
        subset=["partition", "table_name"], keep="last"
    )

    all_row_counts.to_csv(_get_row_count_csv_path(), index=False)

    return AddTableResult(
        success=True,
        message=f"Successfully updated row count table with counts from {table_name}.",
    )


def _write_dbt_yaml_config(schema_path: Path, schema: DbtSchema):
    with schema_path.open("w") as schema_file:
        yaml.dump(
            schema.model_dump(exclude_none=True),
            schema_file,
            default_flow_style=False,
            sort_keys=False,
        )


def generate_table_yaml(
    table_name: str,
    data_source: str,
    partition_column: str = "report_year",
    clobber: bool = False,
) -> AddTableResult:
    """Generate yaml defining a new table."""
    model_path = _get_model_path(table_name, data_source)
    if model_path.exists() and not clobber:
        return AddTableResult(
            success=False,
            message=f"DBT configuration already exists for table {table_name} and clobber is not set.",
        )

    table_config = DbtSchema.from_table_name(
        table_name, partition_column=partition_column
    )
    model_path.mkdir(parents=True, exist_ok=True)
    _write_dbt_yaml_config(model_path / "schema.yml", table_config)

    model_path.mkdir(parents=True, exist_ok=True)

    return AddTableResult(
        success=True,
        message=f"Wrote yaml configuration for table {table_name} at {model_path / 'schema.yml'}.",
    )


def _log_add_table_result(result: AddTableResult):
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
    raise RuntimeError(
        f"Could not determine partition column for table {table_name}. "
        "You can pass this in manually with the '--partition-column' option."
    )


def add_table(
    table_name: str,
    partition_column: str = "report_year",
    use_local_tables: bool = False,
    clobber: bool = False,
) -> AddTableResult:
    """Scaffold dbt yaml for a single table."""
    data_source = get_data_source(table_name)

    if partition_column == "inferred":
        partition_column = _infer_partition_column(table_name)

    _log_add_table_result(
        generate_table_yaml(
            table_name, data_source, partition_column=partition_column, clobber=clobber
        )
    )
    _log_add_table_result(
        generate_row_counts(
            table_name=table_name,
            partition_column=partition_column,
            use_local_tables=use_local_tables,
            clobber=clobber,
        )
    )


@click.command(help="Generate scaffolding to add a new table to the dbt project.")
@click.argument(
    "tables",
    nargs=-1,
)
@click.option(
    "--partition-column",
    default="inferred",
    type=str,
    help="Column used to generate row count per partition test. If 'inferred' the script will attempt to infer a reasonable partitioning column.",
)
@click.option(
    "--use-local-tables",
    default=False,
    type=bool,
    is_flag=True,
    help="If set look for tables locally when generating row counts, otherwise get tables from nightly builds.",
)
@click.option(
    "--clobber",
    default=False,
    is_flag=True,
    type=bool,
    help="Overwrite existing yaml and row counts. If false command will fail if yaml or row counts already exist.",
)
def add_tables(
    tables: list[str],
    partition_column: str = "report_year",
    use_local_tables: bool = False,
    clobber: bool = False,
):
    """Generate dbt yaml to add PUDL table(s) as dbt source(s)."""
    if "all" in tables:
        tables = ALL_TABLES
    elif len(bad_tables := [name for name in tables if name not in ALL_TABLES]) > 0:
        raise RuntimeError(
            f"The following table(s) could not be found in PUDL metadata: {bad_tables}"
        )

    [
        add_table(
            table_name=table_name,
            use_local_tables=use_local_tables,
            partition_column=partition_column,
            clobber=clobber,
        )
        for table_name in tables
    ]


@click.group()
def dbt_helper():
    """Top level cli."""


dbt_helper.add_command(add_tables)


if __name__ == "__main__":
    dbt_helper()
