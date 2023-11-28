#! /usr/bin/env python
"""Materialize one asset & its upstream deps in-process so you can debug."""

import argparse
import importlib.resources

from dagster import AssetSelection, Definitions, define_asset_job

from pudl import etl
from pudl.settings import EtlSettings


def _parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("asset_id")
    return parser.parse_args()


def main(asset_id):
    """Entry point.

    Defines dagster context like in etl/__init__.py - needs to be kept in sync.

    Then creates a job with asset selection.
    """
    etl_fast_settings = EtlSettings.from_yaml(
        importlib.resources.files("pudl.package_data.settings") / "etl_fast.yml"
    ).datasets

    # TODO (daz/zach): maybe there's a way to do this directly with dagster cli?
    defs = Definitions(
        assets=etl.default_assets,
        resources=etl.default_resources,
        jobs=[
            define_asset_job(
                name="materialize_one",
                selection=AssetSelection.keys(asset_id).upstream(),
                config={
                    "resources": {
                        "dataset_settings": {
                            "config": etl_fast_settings.model_dump(),
                        },
                    },
                },
            ),
        ],
    )
    defs.get_job_def("materialize_one").execute_in_process()


if __name__ == "__main__":
    main(**vars(_parse()))
