"""Tiny CLI for showing table descriptions without building the full docs."""

import sys

import click

from pudl.metadata.classes import PudlResourceDescriptor, Resource
from pudl.metadata.descriptions import ResourceDescriptionBuilder
from pudl.metadata.resources import RESOURCE_METADATA


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--name",
    "-n",
    prompt="Table or resource name",
    help="The name of the resource whose description information to display.",
)
def main(name: str):
    """Compute and display the description components for a resource.

    These components are used to build the full resource description which goes into the
    data dictionary, datapackage, and other downstream applications.

    Useful when adding a new table, if you have the top-level structure installed in
    :mod:`pudl.metadata.resources` but don't yet have public documentation written.

    """
    if name not in RESOURCE_METADATA:
        click.echo(f"No table {name}")
        return
    resolved = ResourceDescriptionBuilder(
        name,
        Resource._resolve_references_from_resource_descriptor(
            name, PudlResourceDescriptor.model_validate(RESOURCE_METADATA[name])
        ),
    ).build()
    click.echo("Table found:")
    click.echo(resolved.summarize())


if __name__ == "__main__":
    sys.exit(main())
