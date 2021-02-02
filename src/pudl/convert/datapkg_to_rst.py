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

import json
from pathlib import Path

from jinja2 import Template

path = Path.cwd() / 'src/pudl/package_data/meta/datapkg/datapackage.json'
metadata = open(path)
metadata_json = json.load(metadata)


def datapkg2rst_many():
    """Convert json metadata to a single rst file."""
    template = Template(rst_template_many)
    rendered = template.render(metadata_json)
    print(rendered)
    rendered
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open('docs/data_sources/metadata.rst', 'w') as f:
        f.seek(0)  # Used to overwrite exisiting content
        f.write(rendered)
        f.truncate()  # Used to overwrite exisiting content


def datapkg2rst_one(table):
    """Convert json metadata to multiple rst files."""
    template = Template(rst_template_one)
    rendered = template.render(metadata_json, table=table).strip('\n') + ('\n')
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open(f'docs/data_sources/{table}.rst', 'w') as f:
        f.seek(0)  # Used to overwrite exisiting content
        f.write(rendered)
        f.truncate()  # Used to overwrite exisiting content

# ----------------------------------------------------------------------------
# T E M P L A T E S
# ----------------------------------------------------------------------------


# Template for all tables in one rst file
rst_template_many = '''
{% for resource in resources %}
.. {{ resource.name }}:

Contents of {{ resource.name }} table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::

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

# Template for one table per rst file
rst_template_one = '''
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
