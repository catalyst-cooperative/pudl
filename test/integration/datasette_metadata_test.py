"""Test the metadata.yml file outputted for Datasette."""

import logging
import os

import pytest

# import subprocess


logger = logging.getLogger(__name__)


@pytest.mark.script_launch_mode('inprocess')
def test_metadata_script(script_runner, pudl_settings_fixture):
    """Run metadata_to_yml for testing."""
    ret = script_runner.run(
        'metadata_to_yml', '-o', 'metadata.yml', print_result=False)
    assert ret.success
    datasette = os.environ['ENVBINDIR'] + "datasette"
    logger.info(f"Datasette path: {datasette}")
    '''
    subprocess.run([
        datasette,
        'serve',
        '-m', 'metadata.yml',
        pudl_settings_fixture["pudl_db"],
        pudl_settings_fixture["ferc1_db"]],
        check=True)
    '''
