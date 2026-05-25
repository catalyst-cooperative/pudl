"""Tiny CLI for telling you the table names defined in a particular resource metadata file."""

import importlib
import re
import sys

import click

IS_RESOURCE_FILE = re.compile(r"[a-z0-9_]+\.py")


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.argument("names", nargs=-1)
def main(names: tuple[str]) -> int:
    """Compute and display the tables whose metadata is defined in each file provided.

    Each NAME should be a file in src/pudl/metadata/resources that defines metadata for a set of tables, e.g., eia.py.

    Useful when figuring out what dbt schema files need to be rebuilt.
    """
    modules = []
    # validate args so we don't try to import something hinky
    for name in names:
        _, _, name = name.rpartition("/")
        if not IS_RESOURCE_FILE.match(name):
            click.echo(
                f"{name} not a resource file. Looking for a single filename, or a full path, ending in .py"
            )
            return 0
        modules.append(name[:-3])

    for mod in modules:
        module = importlib.import_module(f"pudl.metadata.resources.{mod}")
        click.echo("\n".join(module.RESOURCE_METADATA.keys()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
