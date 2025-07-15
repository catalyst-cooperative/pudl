"""A CLI tool for generating expect_column_not_all_null dbt test conditions."""

import re

import click
import duckdb
import pyarrow.parquet as pq
import yaml

from pudl.helpers import get_parquet_table
from pudl.logging_helpers import get_logger
from pudl.metadata.classes import PUDL_PACKAGE
from pudl.workspace.setup import PudlPaths

logger = get_logger(__name__)

ALL_TABLES = [r.name for r in PUDL_PACKAGE.resources]


def max_eia860_year() -> int:
    """Get the maximum year available in the EIA-860 dataset."""
    return get_parquet_table(
        "core_eia860__scd_ownership",
        columns=["report_date"],
    )["report_date"].dt.year.max()


def date_column_to_year_expr(date_column: str):
    """Convert a date column to a year extraction expression."""
    if re.match(".*year.*", date_column):
        return date_column
    return f"EXTRACT(year FROM {date_column})"


def get_null_years(
    table_name: str,
    column: str,
    date_column: str,
    max_year: int | None = None,
) -> list[int]:
    """Find years where a specific column is entirely null."""
    pq_path = PudlPaths().parquet_path(table_name)
    year_expr = date_column_to_year_expr(date_column)
    where_clause = f"WHERE {year_expr} <= {max_year}" if max_year else ""
    sql = f"""
    SELECT CAST({year_expr} AS INTEGER) AS year
    FROM '{pq_path}'
    {where_clause}
    GROUP BY {year_expr}
    HAVING COUNT({column}) = 0
    ORDER BY year
    """  # noqa: S608
    with duckdb.connect() as conn:
        null_years = conn.execute(sql).df()["year"].tolist()
    return null_years


def get_available_years(
    table_name: str,
    date_column: str,
    max_year: int | None = None,
) -> list[int]:
    """Generate a list of all years present in the named table."""
    pq_path = PudlPaths().parquet_path(table_name)
    year_expr = date_column_to_year_expr(date_column)
    where_clause = f"WHERE {year_expr} <= {max_year}" if max_year else ""
    sql = f"""
    SELECT DISTINCT CAST({year_expr} AS INTEGER) AS year
    FROM '{pq_path}'
    {where_clause}
    ORDER BY year
    """  # noqa: S608
    with duckdb.connect() as conn:
        avail_years = conn.execute(sql).df()["year"].tolist()
    return avail_years


def infer_row_conditions(
    table_name: str,
    date_column: str,
    max_year: int | None = None,
) -> dict[str, str]:
    """Analyze a single table for null columns and generate conditions."""
    pq_path = PudlPaths().parquet_path(table_name)
    column_names = [col for col in pq.read_schema(pq_path).names if col != date_column]
    available_years = get_available_years(
        table_name,
        date_column=date_column,
        max_year=max_year,
    )

    row_conditions = {}
    for column in column_names:
        null_years = get_null_years(
            table_name,
            column,
            date_column=date_column,
            max_year=max_year,
        )
        if null_years:
            row_conditions[column] = compact_row_condition(
                null_years, available_years, date_column=date_column
            )

    return row_conditions


def compact_row_condition(
    null_years: list[int],
    available_years: list[int],
    date_column: str,
) -> str:
    """Generate a compact SQL condition that excludes entirely null years.

    This function generates conditions that are compatible with the
    expect_columns_not_all_null test, which automatically excludes recent years when
    ignore_eia860m_nulls=true. Any column that's entirely null across all available
    years will result in an error -- such columns need to be debugged, or explicitly
    excluded from the data test. They may also be a sign that you've run the script
    against incomplete output (e.g. the Fast ETL, not the Full ETL).

    Args:
        null_years: List of years where the column is entirely null
        available_years: List of all years present in the dataset
        date_column: The date column to use for year extraction.

    Returns:
        A compact SQL condition string that works with the dbt test's automatic year
        exclusion

    """
    # Get the years where data exists (inverse of null_years)
    data_years = sorted([year for year in available_years if year not in null_years])
    year_expr = date_column_to_year_expr(date_column)

    if not data_years:
        logger.error("No data found! Column should be excluded or debugged.")
        return "ERROR -- Column has no data. Debug the issue or exclude the column."

    # Check if data years form a contiguous range, allowing for compact row_conditions
    if len(data_years) == (data_years[-1] - data_years[0] + 1):
        if len(data_years) == 1:
            return f"{year_expr} = {data_years[0]}"
        if data_years[0] == min(available_years):
            # Data starts at beginning - prefer <= for compatibility
            return f"{year_expr} <= {data_years[-1]}"
        if data_years[-1] == max(available_years):
            # Data goes to the end of our filtered range - use >=
            return f"{year_expr} >= {data_years[0]}"
        # Data is in a finite middle range - use BETWEEN
        return f"{year_expr} BETWEEN {data_years[0]} AND {data_years[-1]}"

    # If we don't have a contiguous range, use the more compact of IN or NOT IN:
    if len(data_years) <= len(null_years):
        # Fewer data years than null years - use IN
        return f"{year_expr} IN ({', '.join(map(str, data_years))})"
    # More data years than null years - use NOT IN
    return f"{year_expr} NOT IN ({', '.join(map(str, null_years))})"


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument(
    "table_name",
    nargs=1,
    type=str,
)
@click.option(
    "--date-column",
    type=str,
    default="report_date",
    show_default=True,
    help="The date column to use for year extraction. Defaults to 'report_date'.",
)
@click.option(
    "--max-year",
    type=int,
    default=None,
    help="Maximum year to consider for analysis. If not provided, all years in the "
    "table will be used. Cannot be used with --ignore-eia860m.",
)
@click.option(
    "--ignore-eia860m",
    is_flag=True,
    default=False,
    help="Generate conditions assuming ignore_eia860m_nulls=true will be used in the "
    "dbt test. This dynamically sets max_year to be whatever the most recent EIA-860 "
    "data is from. Cannot be used with --max-year.",
)
def main(
    table_name: str,
    ignore_eia860m: bool,
    date_column: str,
    max_year: int | None,
):
    """Generate row_conditions for use with the expect_columns_not_all_null dbt test.

    While these row conditions will work out of the box in many cases, they need to be
    reviewed and potentially adjusted to ensure they are appropriate for the
    specific table and its data.
    """
    if table_name not in ALL_TABLES:
        raise click.BadParameter(f"Invalid table name: {table_name}.")
    if ignore_eia860m and max_year is not None:
        raise click.BadParameter(
            "--ignore-eia860m and --max-year are mutually exclusive"
        )

    if ignore_eia860m:
        max_year = max_eia860_year()
        logger.info(f"Using max EIA-860 year: {max_year}")

    row_conditions = infer_row_conditions(
        table_name=table_name,
        date_column=date_column,
        max_year=max_year,
    )

    output_data = yaml.dump(
        row_conditions,
        width=200,
        default_flow_style=False,
        sort_keys=True,
    )

    click.echo(output_data)


if __name__ == "__main__":
    main()
