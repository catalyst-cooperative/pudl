===============================================================================
FERC Form 1 Data Dictionary
===============================================================================

We have mapped the Visual FoxPro DBF files to their corresponding FERC Form 1
database tables and provided a short description of the contents of each table here.

* :download:`A diagram of the 2015 FERC Form 1 Database (PDF)
  <ferc1/ferc1_db_diagram_2015.pdf>`
* :download:`Blank FERC Form 1 (PDF, to 2014-12-31) <ferc1/ferc1_blank_2014-12-31.pdf>`
* :download:`Blank FERC Form 1 (PDF, to 2019-12-31) <ferc1/ferc1_blank_2019-12-31.pdf>`
* :download:`Blank FERC Form 1 (PDF, to 2022-11-30) <ferc1/ferc1_blank_2022-11-30.pdf>`

.. note::

   * The Table Names link to the contents of the database table on our `FERC Form 1
     Datasette deployment <https://data.catalyst.coop/ferc1>`__, where you can browse
     and query the raw data yourself, or download the SQLite DB in its entirety.
   * The mapping of File Name to Table Name is consistent across all years of data.
   * Page numbers correspond to the pages of the FERC Form 1 PDF as it appeared in
     2015, and may not be valid for other years.
   * Many tables without descriptions were discontinued prior to 2015.
   * The "Freq" column indicates the reporting frequency -- A for Annual; Q for
     Quarterly. A/Q if the data is reported both annually and quarterly.

.. csv-table::
   :file: ferc1/ferc1_db_notes.csv
   :header-rows: 1
   :widths: auto
