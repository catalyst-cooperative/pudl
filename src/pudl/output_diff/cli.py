"""Script for comparing two PUDL output directories.

This script scans the baseline and experiment directories, finds files that
are missing in one or the other side and for files that are present in both
it will analyze the contents, based on their type.

For SQLite databases, it will compare the tables, schemas and individual
rows.

For other files, file checksums are calculated and compared instead.
"""
import argparse
import os
import difflib
import sys

import pudl
from sqlalchemy import create_engine, inspect

# Create a logger to output any messages we might have...
logger = pudl.logging_helpers.get_logger(__name__)


def parse_command_line(argv):
    """Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "baseline", type=str, help="path containing baseline outputs."
    )
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
        "--loglevel",
        help="Set logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL).",
        default="INFO",
    )
    arguments = parser.parse_args(argv[1:])
    return arguments



def main():  # noqa: C901
    """Clone the FERC Form 1 FoxPro database into SQLite."""
    args = parse_command_line(sys.argv)

    # Display logged output from the PUDL package:
    pudl.logging_helpers.configure_root_logger(
        logfile=args.logfile, loglevel=args.loglevel
    )

    # Open directory specified by args.baseline
    baseline_files = set(os.listdir(args.baseline))
    experiment_files = set(os.listdir(args.experiment))

    baseline_only = baseline_files - experiment_files
    experiment_only = experiment_files - baseline_files
    common_files = baseline_files & experiment_files

    if baseline_only:
        print("The following files are only present in the baseline directory:")
        for f in baseline_only:
            print(f"  {f}")
    if experiment_only:
        print("The following files are only present in the experiment directory:")
        for f in experiment_only:
            print(f"  {f}") 
    if common_files:
        for f in common_files:
            compare_sqlite_databases(args.baseline, args.experiment, f)
                

def compare_sqlite_databases(baseline: str, experiment: str, fname: str):
    baseline_inspector = inspect(
        create_engine(f"sqlite:///{os.path.join(baseline, fname)}")
    )
    experiment_inspector = inspect(
        create_engine(f"sqlite:///{os.path.join(experiment, fname)}")
    )
    # Compare which tables are present vs missing.
    baseline_tables = set(baseline_inspector.get_table_names())
    experiment_tables = set(experiment_inspector.get_table_names())

    baseline_only_tables = baseline_tables - experiment_tables
    if baseline_only_tables:
        print(f"{fname}: following tables are only present in baseline:")
        for t in baseline_only_tables:
            print(f"  {t}")
    experiment_only_tables = experiment_tables - baseline_tables
    if experiment_only_tables:
        print(f"{fname}: following tables are only present in experiment:")
        for t in experiment_only_tables:
            print(f"  {t}")

    common_tables = baseline_tables & experiment_tables
    for t in common_tables:
        compare_sqlite_tables(
            baseline_inspector, experiment_inspector, t, fname
        )
    
differ = difflib.Differ()

def compare_sqlite_tables(baseline_inspector, experiment_inspector, table_name: str, fname: str):
    baseline_columns = {
        column["name"]: column["type"] for column in baseline_inspector.get_columns(table_name)
    }
    experiment_columns = {
        column["name"]: column["type"] for column in experiment_inspector.get_columns(table_name)
    }
    if baseline_columns != experiment_columns:
        diff = differ.compare(baseline_columns.items(), experiment_columns.items())
        print(f"{fname}: following columns are different in table {table_name}:")
        for line in diff:
            print(line)

if __name__ == "__main__":
    sys.exit(main())
