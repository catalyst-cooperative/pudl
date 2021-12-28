"""Metadata constants and methods."""

from functools import lru_cache
from typing import Tuple

from .classes import Package, Resource
from .resources import RESOURCE_METADATA

__all__ = ["Package", "Resource", "RESOURCE_METADATA"]


@lru_cache
def build_package(
    resource_ids: Tuple[str] = tuple(sorted(RESOURCE_METADATA))
) -> Package:
    """Build and cache the full PUDL Metadata structure."""
    return Package.from_resource_ids(resource_ids)
