"""Test the metadata.yml file that is output generated for Datasette."""

import json
import logging

import datasette.utils
import yaml

from pudl.metadata.classes import DatasetteMetadata
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


def test_datasette_metadata_to_yml(ferc1_engine_xbrl, tmp_path):
    """Test the ability to export metadata as YML for use with Datasette.

    Requires the ferc1_engine_xbrl because we construct Datasette metadata from the
    datapackage.json files which annotate the XBRL derived FERC SQLite DBs.
    """
    metadata_yml = tmp_path / "metadata.yml"
    logger.info(f"Writing Datasette Metadata to {metadata_yml}")

    dm = DatasetteMetadata.from_data_source_ids(
        output_path=PudlPaths().output_dir,
        data_source_ids=[
            "pudl",
            "eia860",
            "eia860m",
            "eia861",
            "eia923",
            "ferc1",
        ],
        xbrl_ids=["ferc1_xbrl"],
    )
    with metadata_yml.open("w") as f:
        f.write(dm.to_yaml())

    logger.info("Parsing generated metadata using datasette utils.")
    metadata_json = json.dumps(yaml.safe_load(metadata_yml.open()))
    parsed_metadata = datasette.utils.parse_metadata(metadata_json)
    assert sorted(set(parsed_metadata["databases"])) == sorted(
        {
            "ferc1_dbf",
            "ferc1_xbrl",
            "ferc2_dbf",
            "ferc2_xbrl",
            "ferc60_dbf",
            "ferc60_xbrl",
            "ferc6_dbf",
            "ferc6_xbrl",
            "ferc714_xbrl",
            "pudl",
        }
    )
    assert parsed_metadata["license"] == "CC-BY-4.0"
    assert (
        parsed_metadata["databases"]["pudl"]["source_url"]
        == "https://github.com/catalyst-cooperative/pudl"
    )
    assert (
        parsed_metadata["databases"]["pudl"]["tables"]["core_eia__entity_plants"][
            "label_column"
        ]
        == "plant_name_eia"
    )
    for tbl_name in parsed_metadata["databases"]["pudl"]["tables"]:
        if parsed_metadata["databases"]["pudl"]["tables"][tbl_name]["columns"] is None:
            raise AssertionError(f"pudl.{tbl_name}.columns is None")
