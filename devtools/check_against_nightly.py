#! /usr/bin/env python3

"""Check a local parquet file or parquet files against the nightly build.

Usage:

    ./check_against_nightly.py <local_targets>

Example usage:

Check against S3 nightly:

    ./check_against_nightly.py core_eia930__*.parquet

Check against local copy:

    ./check_against_nightly.py --reference-dir $PUDL_OUTPUT/../nightly/parquet core_eia930__*.parquet

Check against GCS build outputs (requires gcloud application default credentials):

    ./check_against_nightly.py --reference-dir gs://builds.catalyst.coop/2025-10-11-0605-a531a9a7f-main/parquet core_eia930__*.parquet
"""

from pathlib import Path

import click
import polars as pl
from polars.testing import assert_frame_equal


@click.command(help=__doc__)
@click.argument("local-targets", nargs=-1, type=click.Path())
@click.option(
    "--pdb", is_flag=True, help="Drop into a breakpoint if any dataframes don't match."
)
@click.option(
    "--reference-dir",
    type=str,
    default="https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly",
    help="The directory containing the parquet files to check against. Defaults to S3 nightly.",
)
def cli(local_targets: tuple[Path], pdb: bool, reference_dir):
    """Compare local Parquet files against nightly."""
    local_paths = [Path(target) for target in local_targets]
    click.echo(
        f"Comparing {', '.join(p.stem for p in local_paths)} against those in {reference_dir}..."
    )
    for local_path in local_paths:
        click.echo(f"Comparing {local_path.stem}...", nl=False)
        local = pl.scan_parquet(local_path)
        reference = pl.scan_parquet(f"{reference_dir}/{local_path.name}")
        try:
            assert_frame_equal(local, reference, check_column_order=False)
        except AssertionError as e:
            if pdb:
                breakpoint()
            click.echo(
                f"MISMATCH\n{e}".replace("left", "local").replace("right", "reference")
            )
        else:
            click.echo("OK")


if __name__ == "__main__":
    cli()
