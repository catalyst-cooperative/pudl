"""Check if there are build outputs associated with a git tag."""

import sys

import click

from pudl.deploy.pudl import get_build_from_tag
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument(
    "git-tag",
    type=str,
)
def main(git_tag: str) -> int:
    """Check if there are build outputs on GCS associated with a git-tag."""
    try:
        get_build_from_tag(git_tag)
    except RuntimeError as e:
        logger.error(f"Failed to find a build associated with tag, {git_tag}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
