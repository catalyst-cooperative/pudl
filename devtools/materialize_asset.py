#! /usr/bin/env python
"""Materialize one asset & its upstream deps in-process so you can debug.

If you are using the VSCode Debugger, you'll need to specify the asset_id
in the launch.json file:

{
    ...,
    "args": ["{YOUR_ASSET_ID}}"]
    ...,
}
"""

import argparse

from dagster import materialize

from pudl.etl import default_assets, default_resources, load_dataset_settings_from_file


def _parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("asset_id")
    return parser.parse_args()


def main(asset_id):
    """Entry point.

    Materialize one asset & its upstream deps in-process so you can debug.

    Args:
        asset_id: Name of asset you want to materialize.
    """
    materialize(
        default_assets,
        selection=f"*{asset_id}",
        resources=default_resources,
        run_config={
            "resources": {
                "dataset_settings": {
                    "config": load_dataset_settings_from_file("etl_fast")
                }
            }
        },
    )


if __name__ == "__main__":
    main(**vars(_parse()))
