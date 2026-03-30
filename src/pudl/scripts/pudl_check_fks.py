"""Check that foreign key constraints in the PUDL database are respected."""

import pathlib
import sys

import click


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--logfile",
    help="If specified, write logs to this file.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
)
@click.option(
    "--loglevel",
    default="INFO",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
)
@click.option(
    "--db_path",
    help="Path to PUDL SQLite database where foreign keys should be checked.",
    type=click.Path(
        exists=False,
        resolve_path=True,
        path_type=pathlib.Path,
    ),
    default=None,
)
def main(logfile: pathlib.Path, loglevel: str, db_path: pathlib.Path):
    """Check that foreign key constraints in the PUDL database are respected.

    Dagster manages the dependencies between various assets in our ETL pipeline,
    attempting to materialize tables only after their upstream dependencies have been
    satisfied. However, this order is non deterministic because they are executed in
    parallel, and doesn't necessarily correspond to the foreign-key constraints within
    the database, so durint the ETL we disable foreign key constraints within
    ``pudl.sqlite``.

    However, we still expect foreign key constraints to be satisfied once all of the
    tables have been loaded, so we check that they are valid after the ETL has
    completed. This script runs the same check.
    """
    # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
    import sqlalchemy as sa  # noqa: PLC0415
    from dotenv import load_dotenv  # noqa: PLC0415

    import pudl  # noqa: PLC0415
    from pudl.validate.integrity import check_foreign_keys  # noqa: PLC0415
    from pudl.workspace.setup import PudlPaths  # noqa: PLC0415

    load_dotenv()

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=str(logfile) if logfile else None,
        loglevel=loglevel,
    )

    # Using PudlPaths to get default value for CLI causes validation issues
    if not db_path:
        db_uri = PudlPaths().sqlite_db_uri("pudl")
    else:
        db_uri = f"sqlite:///{db_path}"

    check_foreign_keys(sa.create_engine(db_uri))
    return 0


if __name__ == "__main__":
    sys.exit(main())
