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
        pudl_input=pudl_test_paths.input_dir,
        pudl_output=out_tmp,
    )
