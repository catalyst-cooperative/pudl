"""Test the metadata.yml file outputted for Datasette."""

import json
import logging
from pathlib import Path

import datasette.utils
import pytest
import yaml

logger = logging.getLogger(__name__)


@pytest.mark.script_launch_mode("inprocess")
def test_datasette_metadata_script(script_runner, pudl_settings_fixture):
    """Run datasette_metadata_to_yml for testing."""
    metadata_yml = Path(pudl_settings_fixture["pudl_out"], "metadata.yml")
    logger.info(f"Writing Datasette Metadata to {metadata_yml}")

    ret = script_runner.run(
        "datasette_metadata_to_yml",
        "-o",
        str(metadata_yml),
        print_result=False,
    )
    assert ret.success

    logger.info("Parsing generated metadata using datasette utils.")
    metadata_json = json.dumps(yaml.safe_load(metadata_yml.open()))
    parsed_metadata = datasette.utils.parse_metadata(metadata_json)
    assert set(parsed_metadata["databases"]) == {"pudl", "ferc1"}
    assert parsed_metadata["license"] == "CC-BY-4.0"
    assert (
        parsed_metadata["databases"]["pudl"]["source_url"]
        == "https://github.com/catalyst-cooperative/pudl"
    )
    assert (
        parsed_metadata["databases"]["pudl"]["tables"]["plants_entity_eia"][
            "label_column"
        ]
        == "plant_name_eia"
    )
    for tbl_name in parsed_metadata["databases"]["pudl"]["tables"]:
        assert (
            parsed_metadata["databases"]["pudl"]["tables"][tbl_name]["columns"]
            is not None
        )
