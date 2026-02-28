"""Helper script for running quarterly data updates."""

import argparse
import zipfile
from io import BytesIO

import pandas as pd

from pudl.workspace.setup import PudlPaths


def column_diff(
    form,
    filename,
    old_doi,
    new_doi,
    paths=PudlPaths(),
    verbose=(__name__ == "__main__"),
):
    """Check whether columns differ between previous and new archives."""
    old_path = paths.input_dir / form / old_doi / filename
    new_path = paths.input_dir / form / new_doi / filename
    result = {}
    with (
        zipfile.ZipFile(old_path) as old_resource,
        zipfile.ZipFile(new_path) as new_resource,
    ):
        new_files = set(new_resource.namelist())
        for oldf in old_resource.namelist():
            verbose and print("====")
            verbose and print(oldf)
            if oldf not in new_files:
                result[oldf] = "dropped"
                verbose and print("- Only in old")
                continue
            new_files.remove(oldf)
            if not oldf.endswith(".csv"):
                verbose and print("? Don't know how to check columns")
                continue
            old_df = pd.read_csv(BytesIO(old_resource.read(oldf)), low_memory=False)
            new_df = pd.read_csv(BytesIO(new_resource.read(oldf)), low_memory=False)
            if old_df.shape[1] == new_df.shape[1] and all(
                old_df.columns == new_df.columns
            ):
                continue
            result[oldf] = {}
            verbose and print("Column mismatch")
            new_not_old = set(new_df.columns) - set(old_df.columns)
            old_not_new = set(old_df.columns) - set(new_df.columns)
            if old_not_new:
                result[oldf]["dropped"] = sorted(old_not_new)
                verbose and print("  Removed columns:")
                for c in result[oldf]["dropped"]:
                    verbose and print(f"    {c}")
            if new_not_old:
                result[oldf]["added"] = sorted(new_not_old)
                verbose and print("  Added columns:")
                for c in result[oldf]["added"]:
                    verbose and print(f"    {c}")
        for newf in new_files:
            result[newf] = "added"
            verbose and print("====")
            verbose and print(newf)
            verbose and print("- Only in new")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Check for column differences between raw archives.

Prints output for each file within the specified resource. If columns differ, prints removed and added columns. No additional output means the resource columns match."""
    )
    parser.add_argument("form", help="Name of the archive, e.g., eia930")
    parser.add_argument(
        "filename",
        help="Name of the resource, e.g., eia930-2024half2.zip. Must exist in both DOIs.",
    )
    parser.add_argument(
        "old_doi", help="Old/previous DOI, e.g., 10.5281-zenodo.14026427"
    )
    parser.add_argument("new_doi", help="New DOI, e.g., 10.5281-zenodo.14842901")
    args = parser.parse_args()
    column_diff(**vars(args))
