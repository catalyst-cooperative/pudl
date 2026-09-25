"""The Public Utility Data Liberation (PUDL) Project."""

import warnings
from pathlib import Path
from typing import Literal

import pandas as pd
from dagster import PreviewWarning
from upath import UPath

from pudl.logging_helpers import configure_root_logger

warnings.filterwarnings(
    action="ignore",
    message=r"Specifying a partitions_def on an AssetCheckSpec is currently in preview.*",
    category=PreviewWarning,
)
warnings.filterwarnings(
    action="ignore",
    message=r"grpcio < 1\.83\.0 does not support Post-Quantum Cryptography.*",
    category=FutureWarning,
)

# Every persisted output table has its dtypes forced back to the metadata-declared
# schema by Resource.enforce_schema() before it's written, regardless of what
# fillna()/where()/mask()/clip()/replace() may have done upstream. So opting into this
# future behavior now shouldn't change *persisted* results. It stops pandas from
# silently downcasting (and warning about it). However, it doesn't guarantee
# identical behavior in intermediate operations between a call site and that final
# coercion. This option is a pandas 2.x transitional shim. pandas 3.0 removes silent
# downcasting entirely
pd.set_option("future.no_silent_downcasting", True)

configure_root_logger()

# Paths to resources stored within the PUDL repository. Unlike PUDL_INPUT and
# PUDL_OUTPUT these are not intended to be overridden by users or reset at runtime for
# different environments.
PUDL_ROOT_PATH: Path = Path(__file__).resolve().parents[2]
"""Resolved absolute path to the repository root."""
PUDL_PACKAGE_DATA_PATH: Path = PUDL_ROOT_PATH / "src/pudl/package_data"
"""Resolved absolute path to the package_data directory."""
PUDL_SETTINGS_PATH: Path = PUDL_PACKAGE_DATA_PATH / "settings"
"""Resolved absolute path to the package_data/settings directory."""
PUDL_DBT_PATH: Path = PUDL_ROOT_PATH / "dbt"
"""Resolved absolute path to the dbt directory."""
PUDL_DOCS_PATH: Path = PUDL_ROOT_PATH / "docs"
"""Resolved absolute path to the docs directory."""
PUDL_NIGHTLY_BUILDS_BASE_PATH: UPath = UPath(
    "s3://pudl.catalyst.coop/nightly/", anon=True
)
"""Base path to PUDL nightly builds outputs."""
PUDL_EEL_HOLE_BASE_PATH: UPath = UPath("s3://pudl.catalyst.coop/eel-hole/", anon=True)
"""Base path to eel-hole s3 outputs."""

# How PUDL writes Parquet files. Every writer reads these when it writes, so this is the
# one place to change the codec or level. Refer to them as ``pudl.PARQUET_COMPRESSION``
# and so on, not with ``from pudl import ...``, so that overriding them takes effect
# everywhere.
PARQUET_COMPRESSION: Literal[
    "brotli",
    "gzip",
    "snappy",
    "zstd",
] = "zstd"
"""The compression codec for all of PUDL's Parquet outputs.

The compression levels below depend on this codec, so revisit them if it changes.

PUDL uses ZSTD because it compresses our data better than the default SNAPPY, is fast,
and read time is independent of compression level. This list only includes codes that
are supported by all of Pandas, Polars, DuckDB, and PyArrow.
"""
PARQUET_COMPRESSION_LEVEL: int = 3
"""The compression level for :data:`PARQUET_COMPRESSION`, for most Parquet outputs.

Higher levels will typically write more slowly, and use more memory. Depending on the
algorithm, higher compression levels may also impact the time and resources required to
read the file. Note that different compression algorithms have different allowable
compression level values. See the Parquet documentation for more details:

https://parquet.apache.org/docs/file-format/data-pages/compression/

PUDL uses ZSTD level 3 because it is the Polars default and higher compression levels
have little impact on our data but significantly increase write times. On both PUDL's
float-dominated numeric time series and our wider string-dominated tables ZSTD level 9
only saves about 3% in overall combined file size compared to level 3, and took about
2.5 times as long to write.
"""
PARQUET_GEOMETRY_COMPRESSION_LEVEL: int = 9
"""The compression level for Parquet files with geometry columns.

Geometries (WKB) compress far better than PUDL's other data, and dominate the size of
the files that contain them: on the Census tract table, zstd level 9 was about 25%
smaller than level 3, so it is worth the slower write.
"""

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
