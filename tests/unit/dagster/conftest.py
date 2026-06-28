"""Shared fixtures for Dagster unit tests."""

import logging

import pytest


@pytest.fixture(autouse=True)
def suppress_sqlalchemy_pool_noise():
    """Suppress SQLAlchemy NullPool teardown errors from Dagster's ephemeral instance.

    build_asset_context() and build_output_context() create Dagster ephemeral instances
    backed by their own internal SQLite databases. After the tests complete there's a
    conflict between Dagster's teardown code and the SQLAlchemy teardown code that
    results in something trying to access a closed SQLite connection, which is logged at
    ERROR level. This doesn't actually cause a problem. It seems to be some kind of race
    condition.

    This autouse fixture (which applies to all the Dagster unit tests by virtue of its
    location in the pytest directory structure) preemptively suppressive non-CRITICAL
    logging output from sqlalchemy.pool, which is where the error is logged, to avoid
    cluttering the test output with scary SQLite errors. Logging suppression with a
    context manager in the tests themselves doesn't work because the error is logged
    after the test has completed.
    """
    sa_pool_logger = logging.getLogger("sqlalchemy.pool")
    original_level = sa_pool_logger.level
    sa_pool_logger.setLevel(logging.CRITICAL)
    yield
    sa_pool_logger.setLevel(original_level)
