"""Jinja environment shared by the documentation generators."""

import jinja2
from pydantic import DirectoryPath


def get_environment(
    templates_dir: DirectoryPath, autoescape: bool = True
) -> jinja2.Environment:
    """Return a Jinja environment that loads templates from ``templates_dir``.

    Args:
        templates_dir: Directory containing the Jinja templates.
        autoescape: Whether to HTML-escape rendered values. Templates that
            produce RST containing hand-written markup need this to be False.
    """
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(templates_dir),
        autoescape=autoescape,  # noqa: S701
    )
