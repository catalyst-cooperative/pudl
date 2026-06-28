"""Shared fixtures for Dagster unit tests."""

import logging

import pytest


@pytest.fixture(autouse=True)
def suppress_sqlalchemy_pool_noise():
    """Suppress SQLAlchemy NullPool teardown errors from Dagster's ephemeral instance.

    build_asset_context() and build_output_context() create Dagster ephemeral
    instances backed by their own internal SQLite databases. When these contexts
    are GC'd (e.g. after a failed asset execution or after a test exits), Dagster's
    teardown code accesses pathlib internals (_str, _drv) that were removed in
    Python 3.13, causing a cascade: GeneratorExit propagates through build_resources,
    the finally block tries to roll back an already-closed SQLite connection, and
    sqlalchemy.pool logs the ProgrammingError at ERROR level.
    The tests are correct; this is a Dagster + Python 3.13 compatibility issue.
    Setting the level here (not in a with-block) ensures it's active during GC
    teardown, which occurs after the test function returns.
    """
    sa_pool_logger = logging.getLogger("sqlalchemy.pool")
    original_level = sa_pool_logger.level
    sa_pool_logger.setLevel(logging.CRITICAL)
    yield
    sa_pool_logger.setLevel(original_level)
