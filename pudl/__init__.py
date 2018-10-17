"""The Public Utility Data Liberation (PUDL) Project."""

__author__ = "Catalyst Cooperative"
__contact__ = "zane.selvans@catalyst.coop"
__maintainer__ = "Catalyst Cooperative"
__license__ = "MIT License"
__maintainer_email__ = "zane.selvans@catalyst.coop"
__version__ = '0.1.0'
__docformat__ = ""
__description__ = "Tools for liberating public US electric utility data."
__long_description__ = """
This Public Utility Data Liberation (PUDL) project is a collection of tools
that allow programmatic access to and manipulation of many public data sets
related to electric utilities in the United States. These data sets are
often collected by state and federal agencies, but are publicized in ways
that are not well standardized, or intended for interoperability. PUDL
seeks to allow more transparent and useful access to this important public
data, with the goal of enabling small companies, non-profit organizations,
and public policy advocates to better understand the electricity system
and its impacts on climate.
"""
__pythonrequiredversion__ = "3.6"
__projecturl__ = "https://github.com/catalyst-cooperative/pudl/"
__downloadurl__ = "https://github.com/catalyst-cooperative/pudl/"

# Top level modules
import pudl.settings
import pudl.constants
import pudl.helpers
import pudl.init
import pudl.load

# Extraction functions, by data source:
import pudl.extract.ferc1
import pudl.extract.eia860
import pudl.extract.eia923
import pudl.extract.epacems

# Transformation function, by data source:
import pudl.transform.pudl
import pudl.transform.eia
import pudl.transform.ferc1
import pudl.transform.eia860
import pudl.transform.eia923
import pudl.transform.epacems

# Ready to use analysis modules:
import pudl.analysis.mcoe

# Output modules by data source:
import pudl.output.ferc1
import pudl.output.eia860
import pudl.output.eia923
import pudl.output.pudltabl
