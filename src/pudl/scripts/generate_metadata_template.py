"""Tiny CLI for building default table description templates."""

import sys
from pathlib import Path

import click

from pudl.metadata.classes import MetaFromResourceName, _get_jinja_environment


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--name",
    "-n",
    prompt="Table or resource name",
    help="The name of the resource that needs a description template.",
)
def generate_resource_description_template(name: str):
    """Generate a template for the resource description for a resource.

    Useful when adding a new table, if you have the structure installed in pudl.metadata.resources but don't yet have public documentation written.
    """
    env = _get_jinja_environment(
        (Path(__file__).parent.parent.parent.parent / "docs").resolve()
    )
    meta_resource = MetaFromResourceName(name=name)
    click.echo("Table found:")
    click.echo(meta_resource.summarize())
    click.echo("\n" + "=" * 40 + " TEMPLATE " + "=" * 40 + "\n")
    click.echo(
        env.get_template("resource_description.rst.jinja").render(
            resource=meta_resource,
        )
    )


if __name__ == "__main__":
    sys.exit(generate_resource_description_template())
