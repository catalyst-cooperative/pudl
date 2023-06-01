"""Script for comparing two PUDL output directories.

This script scans the baseline and experiment directories, finds files that are missing
in one or the other side and for files that are present in both it will analyze the
contents, based on their type.

For SQLite databases, it will compare the tables, schemas and individual rows.

For other files, file checksums are calculated and compared instead.
"""
import argparse
import difflib
import os
import sys

import fsspec
import pandas as pd
from sqlalchemy import create_engine, inspect

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
        return (
            self.get_engine(fname)
            .execute("SELECT COUNT(*) FROM :table_name", table_name=table_name)
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

    def get_file_groups(self) -> tuple[set[str], set[str], set[str]]:
        """Returns tuple of baseline_only, common_files, experiment_only."""
        baseline_files = self.baseline.list_files()
        experiment_files = self.experiment.list_files()

        baseline_only = set(baseline_files.keys()) - set(experiment_files.keys())
        experiment_only = set(experiment_files.keys()) - set(baseline_files.keys())
        common_files = set(baseline_files.keys()) & set(experiment_files.keys())
        return baseline_only, common_files, experiment_only

    def compare_sqlite_databases(self, fname: str) -> bool:
        """Compare two sqlite databases, returns true if they're equal."""
        are_equal = True
        baseline_inspector = self.baseline.get_inspector(fname)
        experiment_inspector = self.experiment.get_inspector(fname)

        # Compare which tables are present vs missing.
        baseline_tables = set(baseline_inspector.get_table_names())
        experiment_tables = set(experiment_inspector.get_table_names())

        baseline_only_tables = baseline_tables - experiment_tables
        if baseline_only_tables:
            are_equal = False
            print(f"{fname}: following tables are only present in baseline:")
            for t in baseline_only_tables:
                print(f"  {t}")
        experiment_only_tables = experiment_tables - baseline_tables
        if experiment_only_tables:
            are_equal = False
            print(f"{fname}: following tables are only present in experiment:")
            for t in experiment_only_tables:
                print(f"  {t}")

        common_tables = baseline_tables & experiment_tables
        for t in common_tables:
            if not self.compare_sqlite_tables(fname, t):
                are_equal = False
        return are_equal

    def compare_sqlite_tables(self, fname: str, table_name: str) -> bool:
        """Compares table within a sqlite database.

        Returns True if equal.
        """
        are_equal = True
        baseline_inspector = self.baseline.get_inspector(fname)
        experiment_inspector = self.experiment.get_inspector(fname)
        baseline_columns = {
            column["name"]: str(column["type"])
            for column in baseline_inspector.get_columns(table_name)
        }
        experiment_columns = {
            column["name"]: str(column["type"])
            for column in experiment_inspector.get_columns(table_name)
        }
        if baseline_columns != experiment_columns:
            differ = difflib.Differ()
            are_equal = False
            logger.info(f"baseline_columns: {baseline_columns}")
            logger.info(f"experiment_columns: {experiment_columns}")
            diff = differ.compare(baseline_columns.items(), experiment_columns.items())
            print(f"{fname}: following columns are different in table {table_name}:")
            for line in diff:
                print(line)
        else:
            # Time how long these operations take compare to the get_rows_as_df.
            # Perhaps we can rely on loading data frames only once.
            brc = self.baseline.get_row_count(fname, table_name)
            erc = self.experiment.get_row_count(fname, table_name)
            if brc != erc:
                are_equal = False
                print(
                    f"{fname}: Row count mismatch for table {table_name}: {brc} vs {erc}"
                )
            else:
                logger.info(f"{fname}: Comparing individual rows in {table_name}.")
                # Compare all records in the table using pandas dataframe
                bdf = self.baseline.get_rows_as_df(fname, table_name)
                edf = self.experiment.get_rows_as_df(fname, table_name)

                merged = bdf.merge(edf, how="outer", indicator=True)
                b_only = merged[merged["_merge"] == "left_only"]
                e_only = merged[merged["_merge"] == "right_only"]
                shared = merged[merged["_merge"] == "both"]
                if not b_only.empty or not e_only.empty:
                    are_equal = False
                    print(
                        f"{fname}: Table {table_name} has {len(b_only)} baseline, {len(e_only)} experiment and {len(shared)} shared rows."
                    )
                    # TODO(rousik): print diff recods if args.print_row_diff is set
        return are_equal


def main():  # noqa: C901
    """Clone the FERC Form 1 FoxPro database into SQLite."""
    args = parse_command_line(sys.argv)
    are_equal = True

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )
    outputs = OutputPair(
        baseline=OutputBundle(args.baseline, gcs_project=args.gcs_project_name),
        experiment=OutputBundle(args.experiment, gcs_project=args.gcs_project_name),
        args=args,
    )

    baseline_only, common_files, experiment_only = outputs.get_file_groups()

    if baseline_only:
        are_equal = False
        print("The following files are only present in the baseline directory:")
        for f in baseline_only:
            print(f"  {f}")
    if experiment_only:
        are_equal = False
        print("The following files are only present in the experiment directory:")
        for f in experiment_only:
            print(f"  {f}")
    # TODO: quick md5 checksum comparison could be added here for fast evaluation
    # for remote files. If md5 is the same, we should not need to access the sqlite.
    if common_files:
        for f in common_files:
            if f.endswith(".sqlite") and args.compare_sqlite:
                if not outputs.compare_sqlite_databases(f):
                    are_equal = False
    # Return appropriate return code, fails if differs.
    return 0 if are_equal else 1


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
