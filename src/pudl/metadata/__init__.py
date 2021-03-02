"""Metadata constants and methods."""

from .classes import Resource
from .resources import RESOURCES

RESOURCES = {name: Resource.from_id(name) for name in RESOURCES}
