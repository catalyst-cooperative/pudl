"""Test the PUDL console scripts from within PyTest."""

import pkg_resources
import pytest

# Obtain a list of all deployed entry point scripts to test:
PUDL_SCRIPTS = [
    ep.name
    for ep in pkg_resources.iter_entry_points("console_scripts")
    if ep.module_name.startswith("pudl")
]


@pytest.mark.parametrize("script_name", PUDL_SCRIPTS)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_scripts(script_runner, script_name):
    """Run each console script in --help mode for testing."""
    ret = script_runner.run(script_name, "--help", print_result=False)
    assert ret.success
