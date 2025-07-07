"""Tiny CLI for showing table descriptions without building the full docs."""

import sys

import click

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
def show_description_components(name: str):
    """Compute and display the description components for a resource.

    These components are used to build the full resource description which goes into the data dictionary, datapackage, and other downstream applications.

    Useful when adding a new table, if you have the toplevel structure installed in pudl.metadata.resources but don't yet have public documentation written.
    """
    builder = ResourceDescriptionBuilder(name, RESOURCE_METADATA[name])
    click.echo("Table found:")
    click.echo(builder.summarize())


if __name__ == "__main__":
    sys.exit(show_description_components())
