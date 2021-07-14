"""
Module to convert json metadata into rst files.

All of the information about the transformed pudl tables, namely their fields
types and descriptions, resides in the datapackage metadata. This module makes
that information available to users, without duplicating any data, by converting
json metadata files into documentation-compatible rst files. The functions
serve to extract the field names, field data types, and field descriptions of
each pudl table and outputs them in a manner that automatically updates the
read-the-docs.
"""

import argparse
import json
import logging
import sys

import coloredlogs
from jinja2 import BaseLoader, Environment

logger = logging.getLogger(__name__)


###############################################################################
# T E M P L A T E S
###############################################################################

"""
The following templates map json data into one long rst file seperated by table
titles and document links (RST_TEMPLATE)

It's important for the templates that the json data do not contain excess
white space either at the beginning or the end of each value.
"""


# Template for all tables in one rst file
RST_TEMPLATE = """
===============================================================================
PUDL Data Dictionary
===============================================================================

The following data tables have been cleaned and transformed by our ETL process.

{% for resource in resources %}
.. _{{ resource.name }}:

-------------------------------------------------------------------------------
{{ resource.name }}
-------------------------------------------------------------------------------

{{ resource.description | wordwrap(78)}}
`Browse or query this table in Datasette. <https://data.catalyst.coop/pudl/{{ resource.name }}>`__

.. list-table::
  :widths: auto
  :header-rows: 1

  * - **Field Name**
    - **Type**
    - **Description**{% for field in resource.schema.fields %}
  * - {{ field.name }}
    - {{ field.type }}{% if field.description %}
    - {{ field.description }}{% else %}
    - N/A{% endif %}{% endfor %}
{% endfor %}
"""
""" A template to map data from a json dictionary into one rst file. Contains
    multiple tables seperated by headers.
"""

###############################################################################
# F U N C T I O N S
###############################################################################


def datapkg2rst(meta_json, meta_rst, ignore=None):
    """Convert json metadata to a single rst file."""
    logger.info("Accessing json metadata as dictionary")
    with open(meta_json) as f:
        metadata_dict = json.load(f)

    metadata_dict["resources"] = [
        x for x in metadata_dict["resources"]
        if x["name"] not in ignore
    ]

    metadata_dict["resources"] = sorted(
        metadata_dict["resources"],
        key=lambda x: x["name"]
    )

    for resource in metadata_dict["resources"]:
        resource["schema"]["fields"] = sorted(
            resource["schema"]["fields"],
            key=lambda x: x["name"]
        )

    logger.info("Converting json metadata into an rst file")

    template = (
        Environment(loader=BaseLoader(), autoescape=True)
        .from_string(RST_TEMPLATE)
    )
    rendered = template.render(metadata_dict)
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open(meta_rst, 'w') as f:
        f.seek(0)  # Used to overwrite existing content
        f.write(rendered)
        f.truncate()  # Used to overwrite existing content


def parse_command_line(argv):
    """
    Parse command line arguments. See the -h option.

    Args:
        argv (str): Command line arguments, including caller filename.

    Returns:
        dict: Dictionary of command line arguments and their parsed values.

    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '-i',
        '--input',
        help="Path to the tabular datapackage descriptor (datapackage.json) "
             "to convert into RST.",
        default=False
    )
    parser.add_argument(
        '--ignore',
        help="List of datapackage resource names to skip in generation of RST.",
        nargs="*",
        default=[],
    )
    parser.add_argument(
        '-o',
        '--output',
        help="Path to the file where the RST output should be written.",
        default=False
    )
    arguments = parser.parse_args(argv[1:])
    return arguments


def main():
    """Run conversion from json to rst."""
    pudl_logger = logging.getLogger("pudl")
    log_format = '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)s %(message)s'
    coloredlogs.install(fmt=log_format, level='INFO', logger=pudl_logger)

    args = parse_command_line(sys.argv)
    datapkg2rst(
        meta_json=args.input,
        meta_rst=args.output,
        ignore=args.ignore,
    )


if __name__ == '__main__':
    sys.exit(main())
