import logging
import os

import pytest

from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


@pytest.fixture(name="pudl_test_paths", scope="session", autouse=True)
def unit_pudl_test_paths(
    tmp_path_factory,
    request,
    pudl_test_paths: PudlPaths,
) -> PudlPaths:
    """Apply unit-test-specific path safety on top of ``pudl_test_paths``.

    ``pudl_test_paths`` in ``test/conftest.py`` is the canonical path setup fixture.
    This unit-test fixture only enforces one additional rule: always use a temporary
    ``PUDL_OUTPUT`` even when ``--live-pudl-output`` is passed.
    """
    if not request.config.getoption("--live-pudl-output"):
        return pudl_test_paths

    pudl_tmpdir = tmp_path_factory.mktemp("pudl")
    out_tmp = (pudl_tmpdir / "output").resolve()
    out_tmp.mkdir()

    PudlPaths.set_path_overrides(output_dir=str(out_tmp))
    os.environ["PUDL_OUTPUT"] = str(out_tmp)
    logger.info(
        f"Unit tests ignore --live-pudl-output and use temporary PUDL_OUTPUT: {out_tmp}"
    )

    return PudlPaths(
        pudl_input=pudl_test_paths.pudl_input,
        pudl_output=out_tmp,
    )


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
