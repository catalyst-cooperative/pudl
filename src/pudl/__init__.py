"""The Public Utility Data Liberation (PUDL) Project."""

# Create a parent logger for all PUDL loggers to inherit from
import logging

import pkg_resources

import pudl.analysis.demand_mapping
import pudl.analysis.mcoe
import pudl.analysis.service_territory
import pudl.cli
import pudl.constants
import pudl.convert.datapkg_to_sqlite
import pudl.convert.epacems_to_parquet
import pudl.convert.ferc1_to_sqlite
import pudl.convert.merge_datapkgs
import pudl.etl
import pudl.extract.eia860
import pudl.extract.eia861
import pudl.extract.eia923
import pudl.extract.epacems
import pudl.extract.epaipm
import pudl.extract.excel
import pudl.extract.ferc1
import pudl.extract.ferc714
import pudl.glue.ferc1_eia
import pudl.helpers
import pudl.load.csv
import pudl.load.metadata
# Output modules by data source:
import pudl.output.eia860
import pudl.output.eia923
import pudl.output.ferc1
import pudl.output.ferc714
import pudl.output.glue
import pudl.output.pudltabl
# Transformation functions, organized by data source:
import pudl.transform.eia
import pudl.transform.eia860
import pudl.transform.eia861
import pudl.transform.eia923
import pudl.transform.epacems
import pudl.transform.epaipm
import pudl.transform.ferc1
import pudl.transform.ferc714
# Data validation tools and test cases:
import pudl.validate
# Deployed data & workspace management
import pudl.workspace.datastore
import pudl.workspace.setup  # noqa: F401 WTF is this showing up as unused?

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
__pythonrequiredversion__ = "3.8"
__projecturl__ = "https://catalyst.coop/pudl/"
__downloadurl__ = "https://github.com/catalyst-cooperative/pudl/"

logging.getLogger(__name__).addHandler(logging.NullHandler())
