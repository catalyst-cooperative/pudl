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
import importlib.resources
import json
import logging
import sys

from jinja2 import Template

logger = logging.getLogger(__name__)


pkg = 'pudl.package_data.meta.datapkg'
with importlib.resources.open_text(pkg, 'datapackage.json') as f:
    metadata_dict = json.load(f)


def datapkg2rst_one():
    """Convert json metadata to a single rst file."""
    logger.info("Converting json metadata into an rst file")
    template = Template(rst_template_one)
    rendered = template.render(metadata_dict)
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open('docs/data_sources/metadata.rst', 'w') as f:
        f.seek(0)  # Used to overwrite exisiting content
        f.write(rendered)
        f.truncate()  # Used to overwrite exisiting content


def datapkg2rst_many(table):
    """Convert json metadata to multiple rst files."""
    logger.info("Converting json metadata into rst files")
    template = Template(rst_template_many)
    rendered = template.render(metadata_dict, table=table).strip('\n') + ('\n')
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open(f'docs/data_sources/{table}.rst', 'w') as f:
        f.seek(0)  # Used to overwrite exisiting content
        f.write(rendered)
        f.truncate()  # Used to overwrite exisiting content


def main():
    """Run conversion from json to rst."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    datapkg2rst_one()


if __name__ == '__main__':
    sys.exit(main())

###############################################################################
# T E M P L A T E S
###############################################################################

"""
The following templates map json data one long rst file seperated by table
titles and document links (rst_template_one) or a single rst file
per table (rst_template_many)

It's important for the templates that the json data do not contain execess
white space either at the beginning or the end of each value.
"""


# Template for all tables in one rst file
rst_template_one = '''
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

# Template for one table per rst file
rst_template_many = '''
{% for resource in resources %}
{% if resource.name == table %}
Contents of {{ resource.name }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. list-table::
   :widths: 15 10 30
   :header-rows: 1

  * - **Field**
    - **Data Type**
    - **Description**
{% for field in resource.schema.fields %}
  * - {{ field.name }}
    - {{ field.type }}
    - {{ field.description }}
{% endfor %}
{% endif %}
{% endfor %}
'''
""" A template to map data from a json dictionary in multiple rst files--one
    file per table.
"""
