"""Canonical Dagster orchestration package for PUDL.

This package is the main import surface for the Dagster objects that make up the PUDL
code location. It should expose the assembled ``Definitions`` object along with the
default assets, asset checks, jobs, resources, and sensors that other modules or tools
need to load. Import concrete submodules directly instead of relying on package-level
re-exports so this package can stay free of import-time wiring and side effects.

For the underlying Dagster concept, see
https://docs.dagster.io/getting-started/concepts#definitions
"""
