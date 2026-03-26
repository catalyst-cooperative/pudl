"""Export PUDL table and field metadata to RST for use in documentation."""

import pathlib
import sys

import click

import pudl.logging_helpers
from pudl.metadata.classes import Package
from pudl.metadata.resources import RESOURCE_METADATA

logger = pudl.logging_helpers.get_logger(__name__)


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--skip",
    "-s",
    help=(
        "Name of a table that should be skipped and excluded from RST output. "
        "Use this option multiple times to skip multiple tables."
    ),
    type=str,
    default=[],
    multiple=True,
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Path to which the RST output should be written. Defaults to STDOUT.",
)
@click.option(
    "--docs-dir",
    "-d",
    type=click.Path(
        exists=True,
        dir_okay=True,
        file_okay=False,
        resolve_path=True,
        path_type=pathlib.Path,
        writable=True,
    ),
    default=pathlib.Path().cwd() / "docs",
    help=(
        "Path to the PUDL repository docs directory. "
        "Must exist and be writable. Defaults to ./docs/"
    ),
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
def metadata_to_rst(
    skip: list[str],
    output: pathlib.Path,
    docs_dir: pathlib.Path,
    logfile: pathlib.Path,
    loglevel: str,
):
    """Export PUDL table and field metadata to RST for use in documentation.

    metadata_to_rst -s bad_table1 -s bad_table2 -d ./pudl/docs -o ./datadict.rst
    """
    pudl.logging_helpers.configure_root_logger(logfile=logfile, loglevel=loglevel)

    logger.info(f"Exporting PUDL metadata to: {output}")
    resource_ids = [rid for rid in sorted(RESOURCE_METADATA) if rid not in skip]
    package = Package.from_resource_ids(resource_ids=tuple(sorted(resource_ids)))
    # Sort fields within each resource by name:
    for resource in package.resources:
        resource.schema.fields = sorted(resource.schema.fields, key=lambda x: x.name)
    package.to_rst(docs_dir=docs_dir, path=output)


if __name__ == "__main__":
    sys.exit(metadata_to_rst())
