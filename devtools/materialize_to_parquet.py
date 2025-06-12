#! /usr/bin/env python3

"""Materialize an asset and export it to Parquet for sharing.

Particularly useful for assets that don't get persisted normally.

Example usage:

    ./materialize_to_parquet.py --asset raw_eia860__generator --out generators.parquet
"""

from pathlib import Path

import click
import dagster as dg
import pandas as pd


def materialize_asset(asset_name: str, output_file: Path) -> None:
    """Materialize an asset (and its dependencies) and write to Parquet.

    Note (2025-06-12): we only handle assets that are pandas dataframes at this point.

    Args:
        asset_name: a string that we can turn into an asset key.
        output_file: the path to write Parquet file out to.
    """
    from pudl.etl import defs

    asset = dg.AssetSelection.assets(asset_name)
    asset_and_deps = asset | asset.upstream() | asset.required_multi_asset_neighbors()

    full_etl_job = defs.get_job_def("etl_full")
    execution_result = dg.materialize(
        assets=defs.assets,
        resources=full_etl_job.resource_defs,
        selection=asset_and_deps,
    )

    asset_value = execution_result.asset_value(asset_key=asset_name)

    if isinstance(asset_value, pd.DataFrame):
        asset_value.to_parquet(output_file, engine="pyarrow")
    else:
        raise RuntimeError(f"Value for {asset_name} is not a pandas DataFrame!")


@click.command(help=__doc__)
@click.option("--asset", "-a", required=True, help="Name of asset to materialize.")
@click.option("--out", "-o", required=True, type=click.Path(), help="Output filepath.")
def cli(asset, out):
    """CLI interface wrapper."""
    output_path = Path(out)
    materialize_asset(asset_name=asset, output_file=output_path)


if __name__ == "__main__":
    cli()
