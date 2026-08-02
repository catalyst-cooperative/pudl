"""Audit DBF SQLite datapackage types against observed SQLite values."""

import sys
from pathlib import Path

import click


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument(
    "sqlite_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.argument(
    "datapackage_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["csv", "json"]),
    default="csv",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--row-limit",
    type=int,
    default=10_000,
    show_default=True,
    help="Maximum rows to inspect per column. Use 0 for all rows.",
)
def main(
    sqlite_path: Path,
    datapackage_path: Path,
    output_format: str,
    row_limit: int,
) -> None:
    """Compare a DBF SQLite database to its datapackage field types."""
    from pudl.extract.dbf import audit_dbf_datapackage_types  # noqa: PLC0415

    audit = audit_dbf_datapackage_types(
        sqlite_path=sqlite_path,
        datapackage_path=datapackage_path,
        row_limit=row_limit or None,
    )
    if output_format == "json":
        click.echo(audit.to_json(orient="records"))
    else:
        click.echo(audit.to_csv(index=False), nl=False)


if __name__ == "__main__":
    sys.exit(main())
