#! /usr/bin/env python3

"""Materialize an asset while profiling its memory usage.

Automatically generates a flamegraph from the tracked memory usage.

Example usage:

    ./memory_profile.py -a <asset selection>

    ./memory_profile.py -a <asset selection> -d <output_dir>
"""

from datetime import UTC, datetime
from pathlib import Path
from subprocess import run
from typing import Any

import click
import dagster as dg
from memray import FileFormat, Tracker


def materialize_assets(asset_selection: str) -> Any:
    """Materialize an asset (and its dependencies).

    Args:
        asset_selection: a string that we can turn into an asset key.
    """
    # NOTE (2025-10-21): Putting the PUDL import here means we don't have to wait for the lengthy import.
    from pudl.etl import defs

    asset_selection_with_multi_asset_siblings = dg.AssetSelection.from_string(
        asset_selection
    ).required_multi_asset_neighbors()

    full_etl_job = defs.get_job_def("etl_full")
    execution_result = dg.materialize(
        assets=defs.assets,
        resources=full_etl_job.resource_defs,
        selection=asset_selection_with_multi_asset_siblings,
    )

    asset_value = execution_result.asset_value(asset_key=asset_selection)
    return asset_value


@click.command(help=__doc__)
@click.option(
    "--asset-selection", "-a", required=True, help="Name of asset to materialize."
)
@click.option(
    "--aggregate",
    is_flag=True,
    help="Whether to use the aggregated memory tracker. Defaults to False. If True, the report generates faster but we lose some other stats & reporting on crashed runs.",
)
@click.option("--directory", "-d", default=".", help="Directory to put profiles in.")
def cli(asset_selection, aggregate, directory):
    """Materialize an asset with memory tracking and create a flamegraph."""
    click.echo(f"Materializing {asset_selection} via in-process executor.")
    profile_location = (
        Path(directory)
        / f"memray-{asset_selection}-{datetime.now(tz=UTC):%Y-%m-%dT%H:%M:%S}.bin"
    )
    file_format = (
        FileFormat.AGGREGATED_ALLOCATIONS if aggregate else FileFormat.ALL_ALLOCATIONS
    )
    with Tracker(file_name=profile_location, follow_fork=True, file_format=file_format):
        materialize_assets(asset_selection)
    run(["/usr/bin/env", "memray", "flamegraph", str(profile_location)])  # noqa: S603


if __name__ == "__main__":
    cli()
