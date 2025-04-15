"""A basic CLI to autogenerate dbt data test configurations."""

import re
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from pathlib import Path

import click
import duckdb
import pandas as pd
import yaml
from pydantic import BaseModel

from pudl import validate
from pudl.logging_helpers import configure_root_logger, get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

configure_root_logger()
logger = get_logger(__file__)

ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


class DbtColumn(BaseModel):
    """Define yaml structure of a dbt column."""

    name: str
    data_tests: list | None = None

    def add_column_tests(self, column_tests: list) -> "DbtColumn":
        """Add data tests to columns in dbt config."""
        data_tests = self.data_tests if self.data_tests is not None else []
        return self.model_copy(update={"data_tests": data_tests + column_tests})


class DbtTable(BaseModel):
    """Define yaml structure of a dbt table."""

    name: str
    data_tests: list | None = None
    columns: list[DbtColumn]

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


def get_data_source(table_name: str) -> str:
    """Return data source for a table or 'output' if there's more than one source."""
    resource = PUDL_PACKAGE.get_resource(table_name)
    if len(resource.sources) > 1:
        return "output"

    return resource.sources[0].name


UpdateResult = namedtuple("UpdateResult", ["success", "message"])


def _get_nightly_url(table_name: str) -> str:
    return f"https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/{table_name}.parquet"


def _get_local_table_path(table_name):
    return str(PudlPaths().parquet_path(table_name))


def _get_model_path(table_name: str, data_source: str) -> Path:
    return Path("./dbt") / "models" / data_source / table_name


def _get_row_count_csv_path(etl_fast: bool = False) -> Path:
    if etl_fast:
        return Path("./dbt") / "seeds" / "etl_fast_row_counts.csv"
    return Path("./dbt") / "seeds" / "etl_full_row_counts.csv"


def _get_existing_row_counts(etl_fast: bool = False) -> pd.DataFrame:
    return pd.read_csv(_get_row_count_csv_path(etl_fast), dtype={"partition": str})


def _calculate_row_counts(
    table_name: str,
    partition_column: str = "report_year",
    use_local_tables: bool = False,
) -> pd.DataFrame:
    table_path = (
        _get_local_table_path(table_name)
        if use_local_tables
        else _get_nightly_url(table_name)
    )

    if partition_column == "report_year":
        query = f"""
            SELECT {partition_column} as partition, COUNT(*) as row_count
            FROM '{table_path}' GROUP BY {partition_column}
        """
    elif partition_column in ["report_date", "datetime_utc"]:
        query = f"""
            SELECT CAST(YEAR({partition_column}) as VARCHAR) as partition, COUNT(*) as row_count
            FROM '{table_path}' GROUP BY YEAR({partition_column})
        """
    else:
        query = f"SELECT '' as partition, COUNT(*) as row_count FROM '{table_path}'"

    df = duckdb.sql(query).df().astype({"partition": str})
    df["table_name"] = table_name
    return df


def _combine_row_counts(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.concat([existing, new])
        .drop_duplicates(subset=["partition", "table_name"], keep="last")
        .sort_values(["table_name", "partition"])
    )


def _write_row_counts(row_counts: pd.DataFrame, etl_fast: bool = False):
    csv_path = _get_row_count_csv_path(etl_fast)
    row_counts.to_csv(csv_path, index=False)


def update_row_counts(
    table_name: str,
    partition_column: str = "report_year",
    use_local_tables: bool = False,
    etl_fast: bool = False,
    clobber: bool = False,
) -> UpdateResult:
    """Generate updated row counts per partition and write to csv file within dbt project."""

    existing = _get_existing_row_counts(etl_fast)
    if table_name in existing["table_name"].values and not clobber:
        return UpdateResult(
            success=False,
            message=f"Row counts for {table_name} already exist (run with clobber to overwrite).",
        )

    new = _calculate_row_counts(table_name, partition_column, use_local_tables)
    combined = _combine_row_counts(existing, new)
    _write_row_counts(combined, etl_fast)

    return UpdateResult(
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
            width=float("inf"),
        )


def generate_table_yaml(
    table_name: str,
    data_source: str,
    partition_column: str = "report_year",
    clobber: bool = False,
) -> UpdateResult:
    """Generate yaml defining a new table."""
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
    _write_dbt_yaml_config(model_path / "schema.yml", table_config)

    model_path.mkdir(parents=True, exist_ok=True)

    return UpdateResult(
        success=True,
        message=f"Wrote yaml configuration for table {table_name} at {model_path / 'schema.yml'}.",
    )


def _log_add_table_result(result: UpdateResult):
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
class AddTablesArgs:
    """Define a single class to collect all args for add-tables command."""

    tables: list[str]
    use_local_tables: bool = False
    clobber: bool = False
    etl_fast: bool = False
    yaml_only: bool = False
    row_counts_only: bool = False


@click.command
@click.argument(
    "tables",
    nargs=-1,
)
@click.option(
    "--use-local-tables",
    default=False,
    type=bool,
    is_flag=True,
    help="If set read tables from parquet files in $PUDL_OUTPUT locally when generating row counts, otherwise get tables from nightly builds.",
)
@click.option(
    "--clobber",
    default=False,
    is_flag=True,
    type=bool,
    help="Overwrite existing yaml and row counts. If false command will fail if yaml or row counts already exist.",
)
@click.option(
    "--etl-fast",
    default=False,
    is_flag=True,
    type=bool,
    help="Update row counts for fast ETL counts.",
)
@click.option(
    "--yaml-only",
    default=False,
    is_flag=True,
    type=bool,
    help="Only generate new source table schema.yml config and ignore row counts.",
)
@click.option(
    "--row-counts-only",
    default=False,
    is_flag=True,
    type=bool,
    help="Only generate row counts and ignore yaml.",
)
def add_tables(**kwargs):
    """Generate dbt yaml to add PUDL table(s) as dbt source(s).

    The ``tables`` argument can either be a list of table names, a single table name,
    or 'all'. If 'all' the script will generate configuration for all PUDL tables.

    Note: if ``--clobber`` is set, any manually added configuration for tables
    will be overwritten.
    """
    args = AddTablesArgs(**kwargs)

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

        if not args.row_counts_only:
            _log_add_table_result(
                generate_table_yaml(
                    table_name,
                    data_source,
                    partition_column=partition_column,
                    clobber=args.clobber,
                )
            )
        if not args.yaml_only:
            _log_add_table_result(
                update_row_counts(
                    table_name=table_name,
                    partition_column=partition_column,
                    use_local_tables=args.use_local_tables,
                    etl_fast=args.etl_fast,
                    clobber=args.clobber,
                )
            )


def _get_config(test_config_name: str) -> list[dict]:
    return validate.__getattribute__(test_config_name)


def _load_schema_yaml(schema_path: Path) -> DbtSchema:
    with schema_path.open("r") as schema_yaml:
        return DbtSchema(**yaml.safe_load(schema_yaml))


def _get_test_name(test_config: dict) -> str:
    if not test_config.get("weight_col"):
        test_name = "dbt_expectations.expect_column_quantile_values_to_be_between"
    else:
        test_name = "expect_column_weighted_quantile_values_to_be_between"
    return test_name


def _clean_row_condition(row_condition: str) -> str:
    row_condition = (
        re.sub(
            r"('\d{4}-\d{2}-\d{2}')",
            r"CAST(\1 AS DATE)",
            row_condition,
        )
        .replace("==", "=")
        .replace("!=", "<>")
    )
    return row_condition


def _generate_quantile_bounds_test(test_config: dict) -> list[dict]:
    """Convert dict of config from `validate.py` to construct config for dbt test."""
    return [
        {
            _get_test_name(test_config): {
                "quantile": test_config[quantile_key],
                min_max_value: test_config[bound_key],
                "row_condition": _clean_row_condition(test_config.get("query")),
                "weight_column": test_config.get("weight_col"),
            }
        }
        for quantile_key, bound_key, min_max_value in [
            ("low_q", "low_bound", "min_value"),
            ("hi_q", "hi_bound", "max_value"),
        ]
        if quantile_key in test_config
    ]


@click.command
@click.option("--table-name", type=str, help="Name of table test will be applied to.")
@click.option(
    "--test-config-name",
    type=str,
    help="Name of variable containing test configuration in `pudl.validate`.",
)
@click.option(
    "--model-name",
    default=None,
    help="Name of model if test should be applied to an ephemeral dbt model and not the (source) table directly.",
)
def migrate_tests(table_name: str, test_config_name: str, model_name: str | None):
    """Generate dbt tests that mirror existing vs_bounds tests.

    This command expects a table name, and the name of a config variable in
    ``validate.py``. It will then use this configuration to add a new dbt test which
    mimics the existing quantile tests. This command will add to existing yaml for
    the specified table, but it attempts to add the new test without modifying any
    existing configuration. That being said, it's encouraged to look carefully at
    any changes made when running the command.

    The tests generated by this command may have slight differences in behavior
    from the orginal tests, because the method for computing quantiles is not
    quite identical. After generating the tests, it may take some slight
    modifications to bounds to get the tests passing.

    Example usage:

    dbt_helper migrate-tests \
        --table-name out_eia__yearly_generators \
        --test-config-name mcoe_gas_capacity_factor
    """
    schema_path = (
        _get_model_path(table_name, get_data_source(table_name)) / "schema.yml"
    )
    if not schema_path.exists():
        raise RuntimeError(
            f"Can not migrate tests for table {table_name}, "
            "because no dbt configuration exists for the table."
        )

    schema = _load_schema_yaml(schema_path)
    test_config = _get_config(test_config_name)

    dbt_tests = defaultdict(list)
    for config in test_config:
        logger.info(f"Adding test {config['title']}")
        dbt_tests[config["data_col"]] += _generate_quantile_bounds_test(config)

    schema = schema.add_column_tests(dbt_tests)

    _write_dbt_yaml_config(schema_path, schema)


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
)
def dbt_helper():
    """Script for auto-generating dbt configuration and migrating existing tests.

    This CLI currently provides two commands: ``add-tables`` and ``migrate-tests``.
    The `add-tables` command will generate a yaml file in the ``dbt/models`` repo,
    which tells dbt about the table and adds a row count test. ``migrate-tests`` is
    used to migrate ``vs_bounds`` tests. This command uses configuration defined in
    ``validate.py`` to generate dbt tests.

    Run ``dbt_helper {command} --help`` for detailed usage on each command.
    """


dbt_helper.add_command(add_tables)
dbt_helper.add_command(migrate_tests)


if __name__ == "__main__":
    dbt_helper()
