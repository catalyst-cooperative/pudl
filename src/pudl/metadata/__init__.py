"""Metadata constants and methods."""

from .classes import Package, Resource
from .resources import RESOURCE_METADATA

PACKAGE = Package(
    name="pudl",
    resources=[Resource.dict_from_id(name) for name in RESOURCE_METADATA]
)

RESOURCES = PACKAGE.resources
