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

import json
from pathlib import Path

from jinja2 import Template

path = Path.cwd() / 'src/pudl/package_data/meta/datapkg/datapackage.json'
metadata = open(path)
metadata_json = json.load(metadata)


def datapkg2rst(table):
    """Convert json metadata to rst files."""
    template = Template(rst_template)
    rendered = template.render(
        metadata_json, table=table).strip('\n') + ('\n')
    # Create or overwrite an rst file containing the field descriptions of the input table
    with open('docs/data_sources/file.rst', 'w') as f:
        f.write(rendered)
    f.close()


rst_template = '''
{% for resource in resources %}
{% if resource.name == table %}
Contents of {{ resource.name }}
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
