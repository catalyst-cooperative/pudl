"""The Public Utility Data Liberation (PUDL) Project."""

import pkg_resources

from . import (  # noqa: F401
    analysis,
    cli,
    convert,
    etl,
    extract,
    glue,
    helpers,
    io_managers,
    logging_helpers,
    metadata,
    output,
    transform,
    validate,
    workspace,
)

logging_helpers.configure_root_logger()

__author__ = "Catalyst Cooperative"
__contact__ = "pudl@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__maintainer_email__ = "zane.selvans@catalyst.coop"
__version__ = pkg_resources.get_distribution("catalystcoop.pudl").version
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
