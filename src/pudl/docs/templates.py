"""Jinja environment shared by the documentation generators."""

import jinja2
from pydantic import DirectoryPath


def get_environment(
    templates_dir: DirectoryPath, autoescape: bool = True
) -> jinja2.Environment:
    """Return a Jinja environment that loads templates from ``templates_dir``.

    The environment uses a plain :class:`jinja2.FileSystemLoader`, so template
    names are paths relative to ``templates_dir``. The directory is not checked
    here; a missing directory or template only shows up as a
    :class:`jinja2.TemplateNotFound` error when a template is requested.

    Args:
        templates_dir: Directory containing the Jinja templates.
        autoescape: Whether to HTML-escape rendered values. The default of True
            suits templates that render values pulled from metadata. Templates
            that produce RST containing hand-written markup (hyperlinks, or
            apostrophes in headings) need this to be False, since escaping
            would corrupt that markup. Only disable it for trusted input.

    Returns:
        A Jinja environment rooted at ``templates_dir``.
    """
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(templates_dir),
        autoescape=autoescape,  # noqa: S701
    )
