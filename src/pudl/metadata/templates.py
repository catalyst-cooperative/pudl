"""Jinja templates for use in exporting PUDL metadata to various formats."""

PACKAGE_TO_RST = """
===============================================================================
PUDL Data Dictionary
===============================================================================

The following data tables have been cleaned and transformed by our ETL process.

{% for resource in resources %}
.. _{{ resource.name }}:

-------------------------------------------------------------------------------
{{ resource.name }}
-------------------------------------------------------------------------------

{{ resource.description | wordwrap(78) if resource.description is not none else
'No table description available.' }}
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
"""A template to map data from the PUDL metadata dictionary into ReStructuredText."""
