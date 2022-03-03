"""Test the metadata.yml file outputted for Datasette."""

import pytest


@pytest.mark.script_launch_mode('inprocess')
def test_metadata_script(script_runner):
    """Run metadata_to_yml for testing."""
    ret = script_runner.run(
        'metadata_to_yml', '-o', 'metadata.yml', print_result=False)
    assert ret.success
