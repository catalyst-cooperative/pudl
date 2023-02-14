"""Test the metadata.yml file that is output generated for Datasette."""

import json
import logging
import os
from pathlib import Path

import datasette.utils
import yaml

from pudl.metadata.classes import DatasetteMetadata

logger = logging.getLogger(__name__)


def test_datasette_metadata_to_yml(pudl_env, ferc1_engine_xbrl):
    """Test the ability to export metadata as YML for use with Datasette."""
    pudl_output = Path(os.getenv("PUDL_OUTPUT"))
    metadata_yml = pudl_output / "metadata.yml"
    logger.info(f"Writing Datasette Metadata to {metadata_yml}")

    dm = DatasetteMetadata.from_data_source_ids(pudl_output)
    dm.to_yaml(path=metadata_yml)

    logger.info("Parsing generated metadata using datasette utils.")
    metadata_json = json.dumps(yaml.safe_load(metadata_yml.open()))
    parsed_metadata = datasette.utils.parse_metadata(metadata_json)
    assert set(parsed_metadata["databases"]) == {
        "pudl",
        "ferc1",
        "ferc1_xbrl",
        "ferc2_xbrl",
        "ferc6_xbrl",
        "ferc60_xbrl",
        "ferc714_xbrl",
    }
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
