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

Source metadata
----------------

Metadata for each data source is stored in sources.py.

.. todo::

   * new source metadata when we add a new source
   * update source metadata to expand working partitions

Resource metadata
------------------------

Metadata for each resource is stored in separate files for each source,
with a few additional non-source affinity groups.

.. todo::

   * new resource metadata when we add a new resource
   * update resource metadata rarely

     * fields added/removed/modified
     * discontinued
     * new caution or data effect discovered

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

For small edits, ``resource_description -n <resource_id>`` is usually sufficient.
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
