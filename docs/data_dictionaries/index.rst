.. _data-dictionaries:

Data Dictionaries
=================

Data Processed & Cleaned by PUDL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The PUDL data dictionary provides detailed metadata for the tables
in the PUDL database. This includes table descriptions,
field names, field descriptions, and field datatypes.

**You can find this data at the** `beta PUDL Viewer
<https://viewer.catalyst.coop>`__ **if you want interactive search, filtering,
and CSV export.**

.. toctree::
   :maxdepth: 1
   :titlesonly:

   pudl_db

Raw, Unprocessed Data
^^^^^^^^^^^^^^^^^^^^^
Certain raw datasets (e.g. FERC Form 1) require additional effort to process.
We load these raw sources into SQLite databases before feeding them into
the PUDL data pipeline. The dictionaries below provide key metadata on
these raw sources including table name, table
description, links to corresponding database tables,
raw file names, page numbers, and data reporting frequency.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   ferc1_db

Code Descriptions & Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This section contains mappings of codes in the raw tables to their
corresponding labels and descriptions in the processed PUDL database tables.
For example, the code, NV, represents the full description "Never to exceed"
in the core_eia_codes_averaging_periods table.

.. toctree::
   :maxdepth: 1
   :titlesonly:

   codes_and_labels
