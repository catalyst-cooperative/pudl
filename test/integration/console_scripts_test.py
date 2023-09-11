"""Test the PUDL console scripts from within PyTest."""

import importlib.metadata

import pytest

# Obtain a list of all deployed entry point scripts to test:
PUDL_SCRIPTS = [
    ep.name
    for ep in importlib.metadata.entry_points(group="console_scripts")
    if ep.value.startswith("pudl")
]


@pytest.mark.parametrize("script_name", PUDL_SCRIPTS)
@pytest.mark.script_launch_mode("inprocess")
def test_pudl_scripts(script_runner, script_name):
    """Run each console script in --help mode for testing."""
    ret = script_runner.run(script_name, "--help", print_result=False)
    assert ret.success
