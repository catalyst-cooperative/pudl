from pathlib import Path

import nbformat
import pytest
from nbconvert.preprocessors import ExecutePreprocessor
from sqlalchemy.engine import Engine


@pytest.mark.skip(
    "Notebooks can't access in-memory assets from dagster's execute_in_process() method. "
    "If we switch to writing those assets out to disk in a test-oriented $DAGSTER_HOME "
    "then we can re-activate these tests."
)
@pytest.mark.parametrize(
    "notebook",
    [
        "devtools/inspect-assets.ipynb",
        "devtools/debug-eia-etl.ipynb",
        "devtools/debug-ferc1-etl.ipynb",
        "devtools/debug-harvesting.ipynb",
    ],
)
def test_notebook_exec(notebook: str, pudl_engine: Engine, test_dir: Path):
    nb_path = test_dir.parent / notebook
    with nb_path.open() as f:
        nb = nbformat.read(f, as_version=4)
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
        _ = ep.preprocess(nb, resources={"Application": {"log_level": 5}})
