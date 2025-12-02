.. _metadata:

======================
Metadata editing guide
======================

.. todo::

   * what is metadata
   * how is metadata used within PUDL
   * where is metadata visible to users
   * when are new entries created
   * when are existing entries updated

Dataset-level metadata
----------------------

For each dataset we archive, we record information about the title, a description,
who contributed to archiving the dataset, the segments into which the data files are
partitioned, its license and keywords. This information is used to communicate about
the dataset's usage and provenance to any future users:

   * Generating PUDL documentation for each dataset.
   * Annotating long-term archives of the raw input data on Zenodo.
   * Defining what data partitions can be processed using PUDL.

Defining metadata for a new dataset
...................................

Metadata for each data source is stored in :py:const:`pudl.metadata.sources.SOURCES`.
For each new dataset, add the following fields to the dictionary:

   * **A short code**: Throughout the code, the dataset you choose will be referred to by
      a shorthand code - e.g., ``eia860`` or ``nrelatb``. The standard format we use for
      naming datasets is agency name + dataset name. E.g., Form 860 from EIA becomes
      ``eia860``. When the name of the dataset is more ambiguous (e.g., MSHA's
      mine datasets), we aim to choose a name that is as indicative as possible -
      in this case, ``mshamines``. If you're unsure which name to choose, ask early in
      the contribution process as this will get encoded in many locations. This short
      code will be the key to the entry containing the relevant metadata.
   * ``title``: The title of your dataset should clearly contain the agency publishing
      the data and a non-abbreviated title (e.g., EIA Form 860 -- Annual Electric
      Generator Report, not EIA 860).
   * ``path``: The link to the dataset's "homepage", where information about the
      dataset and the path to download it can be found.
   * ``description``: A short 1-3 sentence description of the dataset.
   * ``working_partitions``: A dictionary where the key is the name of the partition
      (e.g., month, year, form), and the values are the actual available partitions
      that we currently process in PUDL (e.g., 2002-2020). This should correspond to
      the partitions in the datapackage of the dataset's raw archive.
   * ``field_namespace``: **TODO** ????
   * ``keywords``: Words that someone might use to search for this dataset. There are
      collections of common keywords by theme (e.g., electricity, finance) in the
      :py:const:`pudl.metadata.sources.KEYWORDS` dictionary.
   * ``license_raw``: We only archive data with an open source license
      (e.g., US Government Works or a Creative Commons License), so make sure any data
      you're archiving is licensed for re-distribution. See the
      :py:const:`pudl.metadata.sources.LICENSES` dictionary for licenses that should cover
      most use cases.
   * ``license_pudl``: What license we're releasing the data under. This should always
      be ``LICENSES["cc-by-4.0"]``.
   * ``contributors``: Who archived and processed this dataset? Typically this is
      ``CONTRIBUTORS["catalyst-cooperative"]``, but you can add additional contributors
      to the :py:const:`pudl.metadata.sources.CONTRIBUTORS` dictionary.

Additional fields relating to the source data are tracked under a nested
``source_file_dict`` key:

   * ``respondents``: If the dataset is a form with required respondents, who fills out
      this form?
   * ``source_format``: What is the file format of the original data? (e.g., JSON, CSV)?

Updating metadata for an existing dataset
.........................................

We rarely update metadata at the dataset level, other than updating the ``working_partitions``
field to capture a new partition (e.g., year, month) of data. This process is described
in the :doc:`existing_data_updates` documentation.

Table-level metadata
--------------------

For each table we publish, we record information about the content, the schema, the
table's data sources, and usage warnings.

Defining metadata for a new dataset
...................................

Metadata for each resource is stored in separate files for each source,
with a few additional non-source affinity groups.

.. todo::

   * new resource metadata when we add a new resource
   * update resource metadata rarely

     * fields added/removed/modified
     * discontinued
     * new caution or data effect discovered

Updating metadata for an existing dataset
.........................................

As we update data, we might typically modify table-level metadata under one of the
following circumstances:
   * Schema changes: a column has been added or removed in the underlying data, a column
      has been renamed
   * Description changes: a new usage warning, additional context, or to add details
      about a new transformation or change.
   * Availability changes: a table has been discontinued by us or by the original
      provider.

Description metadata
^^^^^^^^^^^^^^^^^^^^^

.. todo::

   * there are lots of tables. structured descriptions are important so that users can deal with that scale
   * for normal tables everything is optional
   * priority fill order: summary, usage warnings, additional details
   * link to api docs

Because the description is not wholly legible in its structured state,
we have developed a few tools to help editors see what they are doing.

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
it is better to use the PUDL Metadata Wizard.
The wizard is a small webserver that looks at your source code and displays
the structured metadata for the table you're working on,
the resolved description sections,
and the fully-rendered table description as it appears in our data dictionaries.

What makes the wizard better for more extensive edits is that
while the initial setup is annoying,
refreshing the page is significantly faster than re-running the command line tool.
If you're checking a single edit, use the command line,
but if you need to iterate at all, use the wizard.

.. todo::

   * link to wizard repo
   * add screenshot

Field metadata
---------------

Metadata for each field is primarily stored in fields.py.
This lets us ensure that fields with the same name share the same general definition,
no matter where you find them.
There are times when a table needs a more specific field definition
than the one in fields.py. In those cases, we _.
