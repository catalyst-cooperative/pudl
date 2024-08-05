"""Test the metadata.yml file that is output generated for Datasette."""

import logging

import sqlalchemy as sa

from pudl.helpers import (
    check_tables_have_metadata,
    parse_datasette_metadata_yml,
)
from pudl.metadata.classes import DatasetteMetadata
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


def test_datasette_metadata_to_yml(ferc1_engine_xbrl, tmp_path):
    """Test the ability to export metadata as YML for use with Datasette.

    Requires the ferc1_engine_xbrl because we construct Datasette metadata from the
    datapackage.json files which annotate the XBRL derived FERC SQLite DBs.
    """
    metadata_yml_path = tmp_path / "metadata.yml"
    logger.info(f"Writing Datasette Metadata to {metadata_yml_path}")

    with metadata_yml_path.open("w") as f:
        # If --live-dbs is not true, we only feed in the ferc1 xbrl outputs
        # and this method would fail otherwise. Even though with --live-dbs
        # we could process all the datasets, we are opting for restricting
        # the domain here for simplicity.

        # TODO(rousik): Remove this restriction when --live-dbs is removed
        # entirely.
        ds_metadata = DatasetteMetadata.from_data_source_ids(
            PudlPaths().pudl_output,
            xbrl_ids=["ferc1_xbrl"],
        )
        f.write(ds_metadata.to_yaml())

    logger.info("Parsing generated metadata using datasette utils.")
    parsed_metadata = parse_datasette_metadata_yml(metadata_yml_path.open())
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


def test_database_metadata(pudl_engine: sa.Engine):
    """Test to make sure all of the tables in the databases have metadata."""
    xbrl_ids = [
        db_file.name.replace(".sqlite", "")
        for db_file in PudlPaths().output_dir.glob("ferc*_xbrl.sqlite")
    ]
    metadata_yml = DatasetteMetadata.from_data_source_ids(
        PudlPaths().pudl_output,
        xbrl_ids=xbrl_ids,
    ).to_yaml()

    databases = (
        ["pudl.sqlite"]
        + sorted(str(p.name) for p in PudlPaths().pudl_output.glob("ferc*.sqlite"))
        + ["censusdp1tract.sqlite"]
    )
    check_tables_have_metadata(metadata_yml, databases)
