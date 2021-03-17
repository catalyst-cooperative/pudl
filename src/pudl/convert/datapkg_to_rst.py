"""
Module to convert json metadata into rst files.

All of the information about the transformed pudl tables, namely their fields
types and descriptions, resides in the datapackage metadata. This module makes
that information available to users, without duplicating any data, by converting
json metadata files into documentation-compatible rst files. The functions
serve to extract the field names, field data types, and field descriptions of
each pudl table and outputs them in a manner that automatically updates the
read-the-docs.

datapky2rst_many() reads the metadata json file, extracts the afformentioned
data, and puts it into an rst file with tags and titles for each of the unique
tables. This way, these tags can be referenced and appear in other rst files.

"""

import argparse
# import importlib.resources
import json
import logging
import sys

from jinja2 import Template

logger = logging.getLogger(__name__)


# pkg = 'pudl.package_data.meta.datapkg'
# with importlib.resources.open_text(pkg, 'datapackage.json') as f:
# metadata_dict = json.load(f)

###############################################################################
# T E M P L A T E S
###############################################################################

"""
The following templates map json data into one long rst file seperated by table
titles and document links (rst_template)

It's important for the templates that the json data do not contain execess
white space either at the beginning or the end of each value.
"""


# Template for all tables in one rst file
rst_template = '''
{% for resource in resources %}
.. _{{ resource.name }}:

Contents of {{ resource.name }} table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 10 5 15
  :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**
{% for field in resource.schema.fields %}
  * - {{ field.name }}
    - {{ field.type }}{% if field.description %}
    - {{ field.description }}{% else %}
    - N/A{% endif %}
{% endfor %}
{% endfor %}
'''
""" A template to map data from a json dictionary into one rst file. Contains
    multiple tables seperated by headers.
"""

###############################################################################
# F U N C T I O N S
###############################################################################


def datapkg2rst_one(meta_json, meta_rst):
    """Convert json metadata to a single rst file."""
    metadata_dict = json.load(meta_json)

    logger.info("Converting json metadata into an rst file")
    template = Template(rst_template)
    rendered = template.render(metadata_dict)
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open(meta_rst, 'w') as f:
        f.seek(0)  # Used to overwrite exisiting content
        f.write(rendered)
        f.truncate()  # Used to overwrite exisiting content


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
        type=str,
        help="""Path to the datapackage.json metadata file""",
        default=False  # defaults["pudl_in"],
    )
    parser.add_argument(
        '-o',
        type=str,
        help="""Path to the metadata.rst output file""",
        default=False  # str(defaults["pudl_out"])
    )
    arguments = parser.parse_args(argv)
    return arguments


def main():
    """Run conversion from json to rst."""
    # parser = argparse.ArgumentParser(description=__doc__)
    # parser.parse_args()
    args = parse_command_line(sys.argv)
    datapkg2rst_one(args.i, args.o)


if __name__ == '__main__':
    sys.exit(main())
