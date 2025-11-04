#! /usr/bin/env python3

"""Check a local parquet file or parquet files against the nightly build.

Usage:

    ./check_against_nightly.py <local_targets>

Example usage:

    ./check_against_nightly.py core_eia930__*.parquet
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
def cli(local_targets: tuple[Path], pdb: bool = False):
    """Compare local Parquet files against nightly."""
    local_paths = [Path(target) for target in local_targets]
    click.echo(f"Comparing {', '.join(p.stem for p in local_paths)} against nightly...")
    for local_path in local_paths:
        click.echo(f"Comparing {local_path.stem}...", nl=False)
        local_lf = pl.scan_parquet(local_path)
        nightly_base = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly"
        remote_lf = pl.scan_parquet(f"{nightly_base}/{local_path.name}")
        try:
            assert_frame_equal(local_lf, remote_lf)
        except AssertionError as e:
            if pdb:
                breakpoint()
            click.echo(f"MISMATCH\n{e}")
        else:
            click.echo("OK")


if __name__ == "__main__":
    cli()
