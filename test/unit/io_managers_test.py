"""Test Dagster IO Managers."""
import shutil
import tempfile

import pandas as pd
from dagster import (
    AssetKey,
    build_init_resource_context,
    build_input_context,
    build_output_context,
)

from pudl.io_managers import pudl_sqlite_io_manager


def test_sqlite_io_manager_delete_stmt():
    """Test we are replacing the data without dropping the table schema."""
    asset_key = "utilities_pudl"
    utilities_pudl = pd.DataFrame(
        {"utility_id_pudl": [1], "utility_name_pudl": "Utility LLC"}
    )

    # TODO (bendnorman): Make the manager a fixture?
    tmp_output_path = tempfile.mkdtemp()
    init_context = build_init_resource_context(
        config={"pudl_output_path": tmp_output_path}
    )
    manager = pudl_sqlite_io_manager(init_context)

    # Load the dataframe to a sqlite table
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, utilities_pudl)

    # Read the table back into pandas
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    returned_df = manager.load_input(input_context)
    assert len(returned_df) == 1

    # Rerun the asset
    # Load the dataframe to a sqlite table
    output_context = build_output_context(asset_key=AssetKey(asset_key))
    manager.handle_output(output_context, utilities_pudl)

    # Read the table back into pandas
    input_context = build_input_context(asset_key=AssetKey(asset_key))
    returned_df = manager.load_input(input_context)
    assert len(returned_df) == 1

    # Remove temporary directory
    shutil.rmtree(tmp_output_path)
