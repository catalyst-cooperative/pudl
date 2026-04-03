"""Unit tests for workspace setup configuration."""

import pytest
from pydantic import ValidationError

from pudl.workspace.setup import PudlPaths


def test_pudlpaths_requires_env_vars_without_env_file() -> None:
    """Make sure bare PudlPaths() fails by default in unit tests.

    You can still opt in by passing in params."""
    with pytest.raises(ValidationError):
        PudlPaths()
