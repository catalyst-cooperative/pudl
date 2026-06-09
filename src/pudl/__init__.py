"""The Public Utility Data Liberation (PUDL) Project."""

import warnings
from pathlib import Path

from dagster import PreviewWarning

from pudl.logging_helpers import configure_root_logger

warnings.filterwarnings(
    action="once",
    message=r"Specifying a partitions_def on an AssetCheckSpec is currently in preview.*",
    category=PreviewWarning,
)

configure_root_logger()

# Paths to resources stored within the PUDL repository. Unlike PUDL_INPUT and
# PUDL_OUTPUT these are not intended to be overridden by users or reset at runtime for
# different environments.
PUDL_ROOT_PATH: Path = Path(__file__).resolve().parents[2]
"""Resolved absolute path to the repository root."""
PUDL_SETTINGS_PATH: Path = PUDL_ROOT_PATH / "src/pudl/package_data/settings"
"""Resolved absolute path to the package_data directory."""
PUDL_DBT_PATH: Path = PUDL_ROOT_PATH / "dbt"
"""Resolved absolute path to the dbt directory."""
PUDL_DOCS_PATH: Path = PUDL_ROOT_PATH / "docs"
"""Resolved absolute path to the docs directory."""

__author__ = "Catalyst Cooperative"
__contact__ = "pudl@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__maintainer_email__ = "zane.selvans@catalyst.coop"

try:
    from pudl._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"

__docformat__ = "restructuredtext en"
__description__ = "Tools for liberating public US electric utility data."
__long_description__ = """
This Public Utility Data Liberation (PUDL) project is a collection of tools
that allow programmatic access to and manipulation of many public data sets
related to electric utilities in the United States. These data sets are
often collected by state and federal agencies, but are publicized in ways
that are not well standardized, or intended for interoperability. PUDL
seeks to allow more transparent and useful access to this important public
data, with the goal of enabling climate advocates, academic researchers, and
data journalists to better understand the electricity system and its impacts
on climate.
"""
__projecturl__ = "https://catalyst.coop/pudl/"
__downloadurl__ = "https://github.com/catalyst-cooperative/pudl/"
