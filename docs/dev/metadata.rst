.. _metadata:

======================
Metadata editing guide
======================

Dataset-level metadata
----------------------

In PUDL, a dataset is called a source. For each source we archive, we record
information about the title, a description, who contributed to archiving the source,
the segments into which the data files are partitioned, its license and keywords.
This information is used downstream in several ways:

* Generating PUDL documentation for each source.
* Annotating long-term archives of the raw input data on Zenodo.
* Defining what data partitions can be processed using PUDL.

Defining metadata for a new dataset
...................................

Metadata for each data source is stored in :py:const:`pudl.metadata.sources.SOURCES`.
For each new source, add the following fields to the dictionary:

* **A short code**: Throughout the code, the source you choose will be referred to by
  a shorthand code - e.g., ``eia860`` or ``nrelatb``. The standard format we use for
  naming sources is agency name + source name. E.g., Form 860 from EIA becomes
  ``eia860``. When the source is not a single agency form with a distinct name
  (e.g., MSHA's mine sources), we aim to choose a name that is as indicative as
  possible - in this case, ``mshamines``. If you're unsure which name to choose, ask
  early in the development process as this will get encoded in many locations. This
  short code will be the key to the entry containing the relevant metadata.
* ``title``: The title of your source should clearly contain the agency publishing
  the data and a non-abbreviated title (e.g., EIA Form 860 -- Annual Electric
  Generator Report, not EIA 860).
* ``path``: The link to the source's "homepage", where information about the
  source and the path to download it can be found.
* ``description``: A short 1-3 sentence description of the source.
* ``working_partitions``: A dictionary where the key is the name of the partition
  (e.g., month, year, form), and the values are the actual available partitions
  that we currently process in PUDL (e.g., 2002-2020). This should correspond to
  the partitions in the datapackage of the source's raw archive.
* ``keywords``: Words that someone might use to search for this source. There are
  collections of common keywords by theme (e.g., electricity, finance) in the
  :py:const:`pudl.metadata.sources.KEYWORDS` dictionary.
* ``license_raw``: We only archive data with an open source license
  (e.g., US Government Works or a Creative Commons License), so make sure any data
  you're archiving is licensed for re-distribution. See the
  :py:const:`pudl.metadata.sources.LICENSES` dictionary for licenses that should
  cover most use cases.
* ``license_pudl``: What license we're releasing the data under. This should always
  be ``LICENSES["cc-by-4.0"]``.
* ``contributors``: Who archived and processed this source? Typically this is
  ``CONTRIBUTORS["catalyst-cooperative"]``, but you can add additional contributors
  to the :py:const:`pudl.metadata.sources.CONTRIBUTORS` dictionary.

Additional fields relating to the source data are tracked under a nested
``source_file_dict`` key:

* ``respondents``: If the source is a form with required respondents, who fills out
  this form?
* ``source_format``: What is the file format of the original data? (e.g., JSON, CSV)?

Updating metadata for an existing dataset
.........................................

Most updates to metadata at the source level occur when we update the
``working_partitions`` field to capture a new partition (e.g., year, month) of data.
This process is described in the :doc:`existing_data_updates` documentation.
Other fields are rarely updated.

Table-level metadata
--------------------

In PUDL, a table is called a resource. For each resource we publish, we record
information about the content, the schema, the resource's data sources, and usage
warnings. This information feeds several processes:

* Generating PUDL documentation for each resource (including descriptions, caveats
  about the input data and our processing methods, and tips for users about which
  table best suits their use case)
* Defining primary and foreign key relationships
* Defining the resource schema to be enforced by Pandera

Defining metadata for a new table
...................................

Metadata for each resource is stored in separate files for each source, with a few
additional files for association tables and imputed assets. If the table doesn't already
have a source file, make a new file that mirrors the format of an existing source.
Otherwise, find the file representing the primary source of the resource
(e.g., :mod:`pudl.metadata.resources.eia860`) and
add a new entry into the ``RESOURCE_METADATA`` dictionary.

Each resource entry should contain the following elements:

* ``description``: A dictionary containing elements that will be compiled into a
  resource description. See :ref:`resource_description` for more detail.
* ``schema``: A dictionary defining the structure of this dataset. This should
  contain the following:

  * ``fields``: A list of all field names in the order you want them to appear in
    the file on disk. These fields should each have their own metadata defined - see
    :ref:`field_description`
  * ``primary_key``: The fields that together define a unique primary key for the
    resource. These fields are required to be non-null, and will be tested for
    uniqueness on writing to disk.
  * ``foreign_key_rules`` (optional): Any fields for which a foreign key rule should
    be created for all other resources containing these fields. For every field(s)
    defined, an error will be raised if other resources contain values not
    included in this table. See :func:`pudl.metadata.helpers.build_foreign_keys`.

* ``field_namespace``: Used to override the field-level definitions for a given group.
  This is typically ``eia`` for EIA-860, 860M, 861 and 923 data, and the same as the
  source short-code for all other resources.
* ``sources``: A list containing the short codes of all sources that feed into this
  table (e.g., for a harvested table combining EIA 860 and 923 data, this would be
  ``["eia860", "eia923"]``.)
* ``etl_group``: A legacy field indicating what ETL 'group' the resource belongs to.
  By default, this should be the same as the source shortcode.

Updating metadata for an existing table
.........................................

As we update data, we might typically modify resource-level metadata under one of the
following circumstances:

* Schema changes: adding, removing, or renaming columns
* Description changes: adding a new usage warning, additional context, or details
  about a new transformation or change.
* Foreign key changes: adding a foreign key relationship, or excluding additional
  tables as needed.

.. _resource_description:

Description metadata
.....................

To make sense of hundreds of resources, structured resource-level metadata is critical.
The following fields should be filled out for each new resource. You may wish to use
:ref:`one of the preview methods <preview_methods>` to guide your edits.

* ``additional_summary_text``: A ~1 line brief description of the table's contents.
  Based on the table type, this should be a fragment that completes the
  corresponding phrase:

  * assn - Association table providing connections between _
  * changelog - Changelog table tracking changes in _
  * codes - Code table containing descriptions of categorical codes for _
  * entity - Entity table containing static information about _
  * scd - Slowly changing dimension (SCD) table describing attributes of _
  * timeseries - Time series of _

* ``additional_source_text``: Use as needed. A few word refinement on the source data
  for this table, such as specifying what part of the form it refers to; usually as
  a parenthetical (e.g., "(Schedule 8A)").
* ``usage_warnings``: Use as needed. A list of keys (for common warnings) or dicts
  (for unique warnings) stating necessary precautions for using this table.
  Reserve this field for severe and/or frequent
  problems an unfamiliar user may encounter, and list lighter or edge-case problems
  in ``additional_details_text``.

  * A list of pre-defined usage warnings can be found in
    :py:const:`pudl.metadata.warnings.USAGE_WARNINGS`.
  * Custom-defined usage warnings should be formatted as a dictionary with two keys:
    ``type``: a short code for the warning, which will only be used for internal
    reference, and ``description``: a 1-2 sentence summary of the warning. E.g.,
    (``{"type": ">50states", "description": "State column contains entries from
    both Mexico and the US."}``)

* ``additional_details_text``: All other information about the table's construction and
  intended use, including guidelines and recommendations for best results. May also
  include more-detailed explanations of listed usage warnings.

The following fields are uncommon, but may be used for tables that require additional
clarification:

* ``additional_layer_text``: Usually not set. Use this to record unusual details about
  this table's level of processing that doesn't fall into the normal definition of
  raw/core/_core/out/_out, etc. This will be appended to the stock text for this layer.
* ``additional_primary_key_text``: Only set if this table has no natural primary key.
  In that case, this should be used to describe what each row contains and why a
  primary key doesn't make sense for this table

Because the description is not wholly legible in its structured state,
we have developed a few tools to help editors see what they are doing.

.. _preview_methods:

Bare-bones preview at the command line
........................................

For small edits, ``resource_description -n <table_name>`` is usually sufficient.
It does not render the description jinja template,
but it will fully resolve each section and print out the results.

Example:

.. code-block::

   $ resource_description -n core_ferc714__hourly_planning_area_demand
   Table found:

   core_ferc714__hourly_planning_area_demand
      Summary [timeseries[hourly]]: Hourly time series of electricity demand by planning area.
        Layer [core]: Data has been cleaned and organized into well-modeled tables that serve as building blocks for downstream wide tables and analyses.
       Source [ferc714]: FERC Form 714 -- Annual Electric Balancing Authority Area and Planning Area Report (Part III, Schedule 2a)
           PK [True]: respondent_id_ferc714, datetime_utc
     Warnings [2]:
      custom - The datetime_utc timestamps have been cleaned due to inconsistent datetime reporting. See below for additional details.
      ferc_is_hard - FERC data is notoriously difficult to extract cleanly, and often contains free-form strings, non-labeled total rows and lack of IDs. See `Notable Irregularities <https://catalystcoop-pudl.readthedocs.io/en/latest/data_sources/ferc1.html#notable-irregularities>`_ for details.
      Details [True]:
   This table includes data from the pre-2021 CSV raw source as well as the newer 2021 through present XBRL raw source.

   This table includes three respondent ID columns: one from the CSV raw source, one from the XBRL raw source and another that is PUDL-derived that links those two source ID's together. This table has filled in source IDs for all records so you can select the full timeseries for a given respondent from any of these three IDs.

   An important caveat to note is that there was some cleaning done to the datetime_utc timestamps. The Form 714 includes sparse documentation for respondents for how to interpret timestamps - the form asks respondents to provide 24 instances of hourly demand for each day. The form is labeled with hour 1-24. There is no indication if hour 1 begins at midnight.

   The XBRL data contained several formats of timestamps. Most records corresponding to hour 1 of the Form have a timestamp with hour 1 as T1. About two thirds of the records in the hour 24 location of the form have a timestamp with an hour reported as T24 while the remaining third report this as T00 of the next day. T24 is not a valid format for the hour of a datetime, so we convert these T24 hours into T00 of the next day. A smaller subset of the respondents reports the 24th hour as the last second of the day - we also convert these records to the T00 of the next day.


Detailed preview using the wizard
...................................

For significant edits, or writing a new description from scratch,
it is better to use the `PUDL Metadata Wizard <https://github.com/catalyst-cooperative/pudl-metadata-wizard#>`__.
The wizard is a small webserver that looks at your source code and displays
the structured metadata for the table you're working on,
the resolved description sections,
and the fully-rendered table description as it appears in our data dictionaries.

What makes the wizard better for more extensive edits is that
while the initial setup is annoying,
refreshing the page is significantly faster than re-running the command line tool.
If you're checking a single edit, use the command line,
but if you need to iterate at all, use the wizard.

Instructions for using the wizard can be found
`in the wizard repo README <https://github.com/catalyst-cooperative/pudl-metadata-wizard/blob/main/README.md>`__.

.. _field_description:

Field metadata
---------------

Metadata for each field is primarily stored in
:py:const:`pudl.metadata.fields.FIELD_METADATA`. This lets us ensure that fields with
the same name share the same general definition, no matter where you find them.

Field names should:
* Be written as snake case (e.g., ``report_date``.)
* Include units at the end where not implied (e.g., ``volume_mcf``)
* Otherwise follow our established :doc:`naming_conventions`.

To define metadata for a new field:

* First, search to make sure it doesn't already exist. Often there's an almost
  identical field from a different table.
* To define a new field, add an entry to the dictionary with the following keys:

  * ``type``: The data type - integer, string, number, or boolean.
  * ``description``: No more than a few sentences describing what the field
    contains.
  * ``unit``: The unit of the column (when applicable).
  * ``constraints``: Categorical columns should largely be encoded by coding tables,
    but you can use this field to constrain a field to a short list of items
    using the ``enum`` key, or a regex pattern using the ``pattern`` key.

There are times when a table or source needs a more specific field definition
than the one already existing in fields.py. In those cases, we can override the field
description for just one resource by adding the resource and the field definition into
:py:const:`pudl.metadata.fields.FIELD_METADATA_BY_RESOURCE`.
