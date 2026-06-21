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

import re
from pathlib import Path
from urllib.parse import urlparse

import click
import geopandas
import pandas as pd
import polars as pl
import pyarrow.fs as pafs
from polars.testing import assert_frame_equal

# Tables stored as GeoParquet (written by geoparquet_io_manager). Polars cannot
# read GeoParquet's union-typed geometry column, so these are compared using
# geopandas + pyarrow instead of polars.
GEO_TABLES: frozenset[str] = frozenset(
    {
        "out_ferc714__georeferenced_respondents",
        "out_censusdp1tract__states",
        "out_censusdp1tract__counties",
        "out_censusdp1tract__tracts",
    }
)


def _build_pyarrow_filesystem(
    reference_dir: str,
) -> tuple[pafs.FileSystem | None, str]:
    """Return a pyarrow filesystem and bucket-relative path prefix for reference_dir.

    Returns (None, reference_dir) for local paths so callers can pass the result
    directly to geopandas.read_parquet without a filesystem argument.
    """
    parsed = urlparse(reference_dir)
    if parsed.scheme == "gs":
        return pafs.GcsFileSystem(), reference_dir.removeprefix("gs://")
    if parsed.scheme in ("http", "https") and "amazonaws.com" in (
        parsed.hostname or ""
    ):
        m = re.search(r"s3\.([^.]+)\.amazonaws\.com", parsed.hostname)
        region = m.group(1) if m else "us-east-1"
        return pafs.S3FileSystem(anonymous=True, region=region), parsed.path.lstrip("/")
    return None, reference_dir


def _compare_geo(local_path: Path, reference_dir: str, pdb: bool) -> bool:
    """Compare a GeoParquet file against a reference. Returns True on match."""
    fs, path_prefix = _build_pyarrow_filesystem(reference_dir)
    ref_path = f"{path_prefix}/{local_path.name}"

    local = geopandas.read_parquet(local_path)
    reference = (
        geopandas.read_parquet(ref_path, filesystem=fs)
        if fs is not None
        else geopandas.read_parquet(ref_path)
    )

    geo_col = local.geometry.name
    non_geo = [c for c in local.columns if c != geo_col]
    sort_cols = [c for c in non_geo if not pd.api.types.is_float_dtype(local[c])]

    local = local.sort_values(sort_cols).reset_index(drop=True)
    reference = reference.sort_values(sort_cols).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(
            local[non_geo], reference[non_geo], check_like=True
        )
        if not local.geometry.geom_equals_exact(reference.geometry, tolerance=0).all():
            n = (
                ~local.geometry.geom_equals_exact(reference.geometry, tolerance=0)
            ).sum()
            raise AssertionError(f"{n} of {len(local)} geometries differ")
    except AssertionError as e:
        if pdb:
            breakpoint()
        click.echo(
            f"MISMATCH\n{e}".replace("left", "local").replace("right", "reference")
        )
        return False
    return True


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
        if local_path.stem in GEO_TABLES:
            ok = _compare_geo(local_path, reference_dir, pdb)
            click.echo("OK" if ok else "")
        else:
            local = pl.scan_parquet(local_path)
            reference = pl.scan_parquet(f"{reference_dir}/{local_path.name}")
            try:
                assert_frame_equal(local, reference, check_column_order=False)
            except AssertionError as e:
                if pdb:
                    breakpoint()
                click.echo(
                    f"MISMATCH\n{e}".replace("left", "local").replace(
                        "right", "reference"
                    )
                )
            else:
                click.echo("OK")


if __name__ == "__main__":
    cli()
