"""Test the metadata.yml file outputted for Datasette."""

import json
import logging
import shutil
from pathlib import Path

import datasette.utils
import pytest
import yaml

# import subprocess


logger = logging.getLogger(__name__)


@pytest.mark.script_launch_mode('inprocess')
def test_metadata_script(script_runner, pudl_settings_fixture, pudl_engine):
    """Run metadata_to_yml for testing."""
    metadata_yml = Path(pudl_settings_fixture["pudl_out"], "metadata.yml")
    logger.info(f"Writing Datasette Metadata to {metadata_yml}")

    ret = script_runner.run(
        'metadata_to_yml',
        '-o',
        str(metadata_yml),
        print_result=False,
    )
    assert ret.success

    logger.info("Parsing generated metadata using datasette utils.")
    metadata_json = json.dumps(yaml.safe_load(metadata_yml.open()))
    # The parsed_metadata dictionary would be the thing to spot check, to see
    # if values / elements are as expected:
    parsed_metadata = datasette.utils.parse_metadata(metadata_json)
    # Some examples:
    assert set(parsed_metadata['databases']) == set(["pudl", "ferc1"])
    assert parsed_metadata["license"] == "CC-BY-4.0"
    assert parsed_metadata['databases'][
        'pudl']['source_url'] == 'https://github.com/catalyst-cooperative/pudl'
    assert parsed_metadata['databases'][
        'pudl']['tables']['plants_entity_eia']['label_column'] == 'plant_name_eia'
    for tbl_name in parsed_metadata['databases']['pudl']['tables']:
        assert parsed_metadata['databases'][
            'pudl']['tables'][tbl_name]['columns'] is not None
    # it sounded like Simon was going to add real validation for the Metadata, so
    # hopefully this test will automatically get upgraded to doing that when the change
    # happens, since I imagine it'll be embedded within parse_metadata()

    # Running Datasette
    # Just to share the issues that came up in getting to the point where
    # we could run datasette from in here:

    # The $PATH will be inherited from the parent process. This means that within Tox,
    # we'll get the datasette installed in the tox environment since tox prepends its
    # bin directory to the $PATH, but running pytest directly we'll get the version
    # installed by conda, which is what we want.  This shutils.which() acts like which
    # in the shell, giving the path to the program (just to verify it's the right one!)
    datasette_path = shutil.which("datasette")
    logger.info(f"Launching Datasette from: {datasette_path}")

    # The [10:] on these strings (URLs) is to strip off the leading sqlite:/// so that
    # they are valid paths. However if we're not going to actually run datasette we
    # don't need these paths. Note that I needed to add pudl_engine to the list of
    # fixtures the test depends on, since otherwise there may or may not be any database
    # at these paths to serve with Datasette.
    pudl_sqlite = Path(pudl_settings_fixture["pudl_db"][10:])
    logger.info(f"Using PUDL DB {pudl_sqlite}")
    ferc1_sqlite = Path(pudl_settings_fixture["ferc1_db"][10:])
    logger.info(f"Using FERC1 DB {ferc1_sqlite}")

    # It ended up that there were two real problems here, which suggest maybe just doing
    # some spot checks of the metadata.yml output file is the better option for now:
    #  1.) The Datasette subprocess never terminates, so the test never ends.
    #  2.) Datasette doesn't complain even if the metadata.yml file is
    #      totally irrelevant (I tested with some other YAML file).
    # subprocess.run([
    #    "datasette",
    #    "serve",
    #    "--metadata",
    #    str(metadata_yml),
    #    str(pudl_sqlite),
    #    str(ferc1_sqlite),
    # ], check=True)
