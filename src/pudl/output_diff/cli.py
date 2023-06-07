"""Script for comparing two PUDL output directories.

This script scans the baseline and experiment directories, finds files that are missing
in one or the other side and for files that are present in both it will analyze the
contents, based on their type.

For SQLite databases, it will compare the tables, schemas and individual rows.

For other files, file checksums are calculated and compared instead.
"""
import argparse
import os
import re
import sys
from typing import Any

import fsspec
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text

import pudl

# Perhaps it would make sense to construct hierarchical-diff structure
# that will encode differences vs equivalence at various abstraction
# levels. E.g. File, Table, Schema, RowCount, Rows.

# Create a logger to output any messages we might have...
logger = pudl.logging_helpers.get_logger(__name__)


def parse_command_line(argv) -> argparse.Namespace:
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=str, help="path containing baseline outputs.")
    parser.add_argument(
        "experiment", type=str, help="path containing experiment outputs."
    )
    parser.add_argument(
        "--logfile",
        default=None,
        type=str,
        help="If specified, write logs to this file.",
    )
    parser.add_argument(
        "--gcs_project_name",
        default="catalyst-cooperative-pudl",
        type=str,
        help="GCS project to use when accessing storage buckets.",
    )
    parser.add_argument(
        "--filetypes",
        nargs="*",
        default=["sqlite"],  # TODO(rousik): add support for json and others
    )
    parser.add_argument(
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    # TODO(janrous): perhaps rework the following comparison depth arguments.
    parser.add_argument(
        "--compare-sqlite",
        type=bool,
        default=False,
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


class CategoryDiff(BaseModel):
    """This is ancestor for all typed diffs."""

    category: str

    def sides_equal(self):
        """Returns True if both sides are equal."""
        return True

    def print_diff(self):
        """Prints human readable representation of the diff."""
        pass


class ListDiff(CategoryDiff):
    """Encodes the differences between two sides.

    The two sides are expected to be lists of objects that can be turned into sets.
    """

    left: list[Any]
    right: list[Any]
    both: list[Any]

    @staticmethod
    def from_sets(category: str, left: list[Any], right: list[Any]):
        """Converts two sets into ListDiff.

        This is done by separating the two lists into disjoint sets of left_only, both,
        right_only.
        """
        ls = set(left)
        rs = set(right)
        return ListDiff(
            category=category,
            left=sorted(ls - rs),
            right=sorted(rs - ls),
            both=sorted(ls & rs),
        )

    def sides_equal(self):
        """Returns True if left_only and right_only lists are empty."""
        return len(self.left) == 0 and len(self.right) == 0

    def print_diff(self):
        """Prints the human-friendly representation of the diff."""
        if not self.sides_equal():
            print(f"{self.category}:")
            for l_item in self.left:
                print(f"- {l_item}")
            for r_item in self.right:
                print(f"+ {r_item}")


class RowCountDiff(CategoryDiff):
    """Represents diff that considers number of rows only."""

    left: int
    right: int

    def sides_equal(self):
        """Returns true if both sides are considered equal."""
        return self.left == self.right

    def print_diff(self):
        """Prints the human-friendly representation of the diff."""
        if not self.sides_equal():
            print(f"{self.category}: {self.right - self.left}")


class RowSampleDiff(ListDiff):
    """Diff for row-by-row comparison."""

    left_unique: int
    right_unique: int
    shared_rows: int
    left: list[Any] = []
    right: list[Any] = []
    both: list[Any] = []

    def sides_equal(self):
        """Returns true if sides are equal."""
        return self.left_unique == 0 and self.right_unique == 0

    def print_diff(self):
        """Prints human friendly representation of this diff."""
        if not self.sides_equal():
            print(
                f"{self.category}: -{self.left_unique} +{self.right_unique} ={self.shared_rows}"
            )
            for l_item in self.left:
                print(f"- {l_item}")
            for r_item in self.right:
                print(f"+ {r_item}")


class OutputBundle:
    """Represents single pudl output directory.

    This could be either local or remote and faciliates access to the files and type-
    aware accessors (e.g. sqlite_engine).
    """

    FILE_TYPES = ["json", "sqlite"]

    def __init__(self, root_path: str, gcs_project=None):
        """Create new instance pointed at root_path."""
        self.root_path = root_path
        if root_path.startswith("gs://"):
            self.fs = fsspec.filesystem("gcs", project=gcs_project)
        else:
            self.fs = fsspec.filesystem("file")

    def list_files(self) -> dict[str, str]:
        """Returns dict mapping from basenames to full paths."""
        # TODO(rousik): check if root_path is an actual directory.
        if self.root_path.endswith(".sqlite"):
            return {"sqlite": self.root_path}
        else:
            return {
                os.path.basename(fpath): fpath
                for fpath in self.fs.glob(self.root_path + "/*")
                if self.match_filetype(fpath)
            }

    def match_filetype(self, fpath: str) -> bool:
        """Returns true if file should be considered for comparison."""
        return any([fpath.endswith(ft) for ft in self.FILE_TYPES])

    def get_engine(self, fname: str):
        """Returns sqlalchemy engine for reading contents of fname."""
        return create_engine_remote(self.fs, f"{self.root_path}/{fname}")

    def get_inspector(self, fname: str):
        """Returns sqlalchemy inspector for analyzing contents of fname."""
        return inspect(self.get_engine(fname))

    def get_row_count(self, fname: str, table_name: str) -> int:
        """Returns number of rows contained within the table."""
        if not re.compile(r"^[a-zA-Z0-9_]+$").match(table_name):
            raise ValueError("table_name is not SQL safe")
        # Note that because of the above check, CWE-89 SQL injection issue
        # is no longer possible below.
        # Because we get the table names from sqlite metadata, these table
        # names should be safe by definition also.
        return (
            self.get_engine(fname)
            .execute(text(f"SELECT COUNT(*) FROM {table_name}"))  # nosec
            .scalar()
        )

    def get_rows_as_df(self, fname: str, table_name: str) -> pd.DataFrame:
        """Returns sqlite table contents as pandas DataFrame."""
        con = self.get_engine(fname)
        return pd.concat(
            [df for df in pd.read_sql_table(table_name, con, chunksize=100_000)]
        )


class OutputPair:
    """Represents pair of output.

    Baseline and an experiment.
    """

    def __init__(
        self, baseline: OutputBundle, experiment: OutputBundle, args: argparse.Namespace
    ):
        """Create new instance with given baseline and experiment."""
        self.baseline = baseline
        self.experiment = experiment
        self.args = args

    def compare_files(self) -> ListDiff:
        """Produces GenericDiff that contains file basenames."""
        baseline_files = set(self.baseline.list_files())
        experiment_files = set(self.experiment.list_files())
        return ListDiff.from_sets("File", baseline_files, experiment_files)

    def compare_sqlite_databases(self, file_diff: ListDiff) -> list[CategoryDiff]:
        """Generates series of structural diffs over sqlite databases.

        Returns dictionary where keys are diff categories and values are GenericDiff
        representing the differences at the given category.

        The following categories of diffs are generated:
        - filename/table_name, values are table names
        - filename/table_name/schema, values are (col_name, col_type) tuples
        - filename/table_name/rowcounts, values are [row_count]
        - filename/table_name/rows, values are [distinct_row_count]
        """
        # TODO(rousik): using GenericDiff for rowcounts and rows are not ideal,
        # perhaps better approach would be to have type-specific diffs that allow
        # either numbers for left/right (for rowcount) or panda dataframes for rows.
        # But let's work with the hacky thing for now.
        output_diffs = []

        def maybe_append_diff(diff: CategoryDiff):
            """Appends diff to output_diffs if there are actual differences."""
            if not diff.sides_equal():
                output_diffs.append(diff)
                diff.print_diff()

        for fname in file_diff.both:
            logger.info(f"Analyzing file {fname}")
            if not fname.endswith(".sqlite"):
                # This file is not supported by this comparison function
                continue

            table_diff = self.compare_tables(fname)
            maybe_append_diff(table_diff)
            for table_name in table_diff.both:
                schema_diff = self.compare_table_schemas(fname, table_name)
                maybe_append_diff(schema_diff)
                # We could consider comparing tables that have divergent schemas,
                # but that would be trickier.
                if schema_diff.sides_equal():
                    rc_diff = self.compare_table_rowcounts(fname, table_name)
                    maybe_append_diff(rc_diff)
                    # if rc_diff.sides_equal():
                    #    maybe_append_diff(self.compare_table_rows(fname, table_name))
        return output_diffs

    def compare_tables(self, fname: str) -> ListDiff:
        """Generates diff comparing the presence/absence of sqlite tables."""
        baseline_inspector = self.baseline.get_inspector(fname)
        experiment_inspector = self.experiment.get_inspector(fname)

        baseline_tables = set(baseline_inspector.get_table_names())
        experiment_tables = set(experiment_inspector.get_table_names())
        return ListDiff.from_sets(f"{fname}/tables", baseline_tables, experiment_tables)

    def compare_table_schemas(self, fname: str, table_name: str) -> ListDiff:
        """Generate diff comparing the schema for a given table."""
        baseline_inspector = self.baseline.get_inspector(fname)
        experiment_inspector = self.experiment.get_inspector(fname)
        baseline_cols = [
            (col["name"], str(col["type"]))
            for col in baseline_inspector.get_columns(table_name)
        ]
        experiment_cols = [
            (col["name"], str(col["type"]))
            for col in experiment_inspector.get_columns(table_name)
        ]
        return ListDiff.from_sets(
            f"Schema({fname}/{table_name})",
            baseline_cols,
            experiment_cols,
        )

    def compare_table_rowcounts(self, fname: str, table_name: str) -> RowCountDiff:
        """Compares the number of rows within given file and table."""
        logger.info(f"Analyzing rows of {fname}/{table_name}.")
        brc = self.baseline.get_row_count(fname, table_name)
        erc = self.experiment.get_row_count(fname, table_name)
        return RowCountDiff(
            category=f"RowCount({fname}/{table_name})", left=brc, right=erc
        )

    def compare_table_rows(self, fname: str, table_name: str) -> RowSampleDiff:
        """Compares individual rows within given file and table."""
        # TODO(rousik): this may be very memory/resource expensive for tables
        # with many rows. We might want to switch this off for very large tables
        # or switch to some cheaper approach, e.g. statistical sampling or
        # comparing row hashes.
        bdf = self.baseline.get_rows_as_df(fname, table_name)
        edf = self.experiment.get_rows_as_df(fname, table_name)

        merged = bdf.merge(edf, how="outer", indicator=True)
        b_only = merged[merged["_merge"] == "left_only"]
        e_only = merged[merged["_merge"] == "right_only"]
        shared = merged[merged["_merge"] == "both"]

        # For the sake of compact result, simply count distinct records.
        # Later on, samples of differing rows, or even all differing rows
        # could be stored in left/right.
        return RowSampleDiff(
            category=f"Rows({fname}/{table_name})",
            left_unique=len(b_only),
            right_unique=len(e_only),
            shared_rows=len(shared),
            left=[],
            right=[],
        )


def main():  # noqa: C901
    """Clone the FERC Form 1 FoxPro database into SQLite."""
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )
    outputs = OutputPair(
        baseline=OutputBundle(args.baseline, gcs_project=args.gcs_project_name),
        experiment=OutputBundle(args.experiment, gcs_project=args.gcs_project_name),
        args=args,
    )

    file_diff = outputs.compare_files()
    file_diff.print_diff()
    sql_diffs = outputs.compare_sqlite_databases(file_diff)

    # Put together all diffs that actually represent some differences
    all_diffs = [d for d in [file_diff] + sql_diffs if not d.sides_equal()]
    return len(all_diffs)


def create_engine_remote(fs, url: str):
    """Make instance of sqlalchemy engine that can be local or remote."""
    # Perhaps we need to modify the sqlite:///{url} but lets start this way.
    if url.startswith("gs://"):
        # FIXME(rousik): this doesn't seem to work
        logger.info(f"Connecting to sqlite db at sqlite:///{url}")
        return create_engine(
            f"sqlite:///{url}", creator=lambda: fs.open(url, mode="rb")
        )
    else:
        # assuming local files
        return create_engine(f"sqlite:///{url}")


if __name__ == "__main__":
    sys.exit(main())
