===============================================================================
FERC Form 1 Data Dictionary
===============================================================================

We have mapped the Visual FoxPro DBF files to their corresponding FERC Form 1
database tables and provided a short description of the contents of each table here.

.. note::

   * The Table Names link to the contents of the database table on our `FERC Form 1
     Datasette deployment <https://data.catalyst.coop/ferc1>`__ where you can browse
     and query the raw data yourself or download the SQLite DB in its entirety.
   * The mapping of File Name to Table Name is consistent across all years of data.
   * Page numbers correspond to the pages of the FERC Form 1 PDF as it appeared in
     2015 and may not be valid for other years.
   * Many tables without descriptions were discontinued prior to 2015.
   * The "Freq" column indicates the reporting frequency -- A for Annual; Q for
     Quarterly. A/Q if the data is reported both annually and quarterly.

.. csv-table::
   :file: ferc1_db.csv
   :header-rows: 1
   :widths: auto
