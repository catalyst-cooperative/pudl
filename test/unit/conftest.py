import logging
from pathlib import Path

import pydantic
import pytest

from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def configure_paths_for_tests(tmp_path_factory, request):
    """Configures PudlPaths for tests.

    Default behavior:

    PUDL_INPUT is read from the environment.
    PUDL_OUTPUT is set to a tmp path, to avoid clobbering existing databases.

    Set ``--tmp-data`` to force PUDL_INPUT to a temporary directory, causing
    re-downloads of all raw inputs.

    Ignores the ``--live-dbs`` flag; always forces PUDL_OUTPUT to a temp dir so
    unit test can never mess with the outputs.

    See pudl/test/conftest.py for the non-unit test counterpart.
    """
    pudl_tmpdir = tmp_path_factory.mktemp("pudl")

    # We only use a temporary input directory when explicitly requested.
    # This will force a re-download of raw inputs from Zenodo or the GCS cache.
    if request.config.getoption("--tmp-data"):
        in_tmp = pudl_tmpdir / "input"
        in_tmp.mkdir()
        PudlPaths.set_path_overrides(
            input_dir=str(Path(in_tmp).resolve()),
        )
        logger.info(f"Using temporary PUDL_INPUT: {in_tmp}")

    out_tmp = pudl_tmpdir / "output"
    out_tmp.mkdir()
    PudlPaths.set_path_overrides(
        output_dir=str(Path(out_tmp).resolve()),
    )
    logger.info(f"Using temporary PUDL_OUTPUT: {out_tmp}")

    try:
        return PudlPaths()
    except pydantic.ValidationError as err:
        pytest.exit(
            f"Set PUDL_INPUT, PUDL_OUTPUT env variables, or use --tmp-path, --live-dbs flags. Error: {err}."
        )
