===============================================================================
FERC Form 1
===============================================================================

=================== ===========================================================
Source URL          https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual
Source Description  Financial and operational information from electric utilities,
                    licensees and others entities subject to FERC jurisdiction.
Respondents         Major electric utilities and licensees
Source Format       FoxPro Database (.DBC/.DBF)
Source Years        1994-2019
Size (Download)     1.3 GB
PUDL Code           ``ferc1``
Years Liberated     1994-2019
Records Liberated   ~12 million (116 raw tables), ~316,000 (7 clean tables)
Issues              `Open FERC Form 1 issues <https://github.com/catalyst-cooperative/pudl/issues?q=is%3Aissue+is%3Aopen+label%3Aferc1>`__
=================== ===========================================================

Background
^^^^^^^^^^

The FERC Form 1, otherwise known as the **Electric Utility Annual Report**, contains
financial and operating data for major utilities and licensees. Much of it is not
publicly available anywhere else.

Who is required to fill out the form?
-------------------------------------

As outlined in the Commission's Uniform System of Accounts Prescribed for Public
Utilities and Licensees Subject To the Provisions of The Federal Power Act (18 C.F.R.
Part 101), to qualify as a respondent, entities must exceed at least one of the
following criteria for three consecutive years prior to reporting:

* 1 million MWh of total sales
* 100MWh of annual sales for resale
* 500MWh of annual power exchanges delivered
* 500MWh of annual wheeling for others (deliveries plus losses)

Annual responses are due in April of the following year. FERC typically releases the
new data in October.

How much of the data is accessible through PUDL?
------------------------------------------------

Thus far we have integrated 7 tables into the full PUDL ETL pipeline. We
focused on the tables pertaining to power plants, their capital & operating
expenses, and fuel consumption; however, we have the tools required to pull
just about any other table in as well.

What does the original data look like?
--------------------------------------

.. seealso::

    Explore the full FERC Form 1 dataset at: https://data.catalyst.coop/ferc1

The data is published as a collection of Visual FoxPro databases, one per year
beginning in 1994. The databases all share a very similar structure, with a total of
116 data tables containing ~8GB of raw data (though 90% of that data is in 3 tables
containing binary data). The `final release of Visual FoxPro was v9.0 in 2007
<https://en.wikipedia.org/wiki/Visual_FoxPro>`__. Its `extended support period ended
in 2015 <https://www.foxpro.co.uk/foxpro-end-of-life-and-you/>`__. The bridge
application which allowed this database to be used in Microsoft Access has been
discontinued. FERC's continued use of this database format creates a significant
barrier to data access.

The FERC 1 database is poorly normalized, and the data itself does not appear
to be subject to much quality control. For more detaild context and
documentation on a table-by-table basis, see :doc:`ferc1_db_notes`

Notable Irregularities
^^^^^^^^^^^^^^^^^^^^^^
Sadly, the FERC Form 1 database is not particularly... relational. The only
foreign key relationships that exist map ``respondent_id`` fields in the
individual data tables back to ``f1_respondent_id``. In theory, most of the
data tables use ``report_year``, ``respondent_id``, ``row_number``,
``spplmnt_num`` and ``report_prd`` as a composite primary key


In practice, there are several thousand records (out of ~12 million), including some
in almost every table, that violate the uniqueness constraint on those primary keys.
Since there aren't many meaningful foreign key relationships anyway, rather than
dropping the records with non-unique natural composite keys, we chose to preserve all
of the records and use surrogate auto-incrementing primary keys in the cloned SQLite
database.

Lots of the data included in the FERC tables is extraneous and difficult to parse. None
of the tables have record identification, and they sometimes contain multiple rows
pertaining to the same plant or portion of a plant. For example, a utility might report
values for individual plants as well as the sum total, rendering any aggregations
performed on the column inaccurate. Sometimes there are values reported for the total
rows and not the individual plants, making them difficult to simply remove. Moreover,
these duplicate rows are incredibly difficult to identify.

To improve their usability, we have developed a complex system of regional mapping in
order to create ids for each of the plants that can then be compared to PUDL ids and
used for integration with EIA and other data. We also remove many of the duplicate rows,
and are in the midst of executing a more thorough review of the extraneous rows.

Over time we will pull in and clean up additional FERC Form 1 tables. If there's data
you need from Form 1 in bulk you can `hire us <https://catalyst.coop/hire-catalyst/>`__
to liberate it first.

PUDL Data Tables
^^^^^^^^^^^^^^^^

We've segmented the processed FERC Form 1 data into the following normalized data
tables. Clicking on the links will show you the names and descriptions of the fields
available in each table.

.. list-table::
   :header-rows: 1
   :widths: auto

   * - Data Dictionary
     - Browse Online
   * - :ref:`fuel_ferc1`
     - https://data.catalyst.coop/pudl/fuel_ferc1
   * - :ref:`plant_in_service_ferc1`
     - https://data.catalyst.coop/pudl/plant_in_service_ferc1
   * - :ref:`plants_ferc1`
     - https://data.catalyst.coop/pudl/plants_ferc1
   * - :ref:`plants_hydro_ferc1`
     - https://data.catalyst.coop/pudl/plants_hydro_ferc1
   * - :ref:`plants_pumped_storage_ferc1`
     - https://data.catalyst.coop/pudl/plants_pumped_storage_ferc1
   * - :ref:`plants_small_ferc1`
     - https://data.catalyst.coop/pudl/plants_small_ferc1
   * - :ref:`plants_steam_ferc1`
     - https://data.catalyst.coop/pudl/plants_steam_ferc1
   * - :ref:`purchased_power_ferc1`
     - https://data.catalyst.coop/pudl/purchased_power_ferc1
   * - :ref:`utilities_ferc1`
     - https://data.catalyst.coop/pudl/utilities_ferc1

PUDL Data Transformations
^^^^^^^^^^^^^^^^^^^^^^^^^

To see the transformations applied to the data in each table, you can read the
:mod:`pudl.transform.ferc1` module documentation for more details. created for their
respective transform functions.
