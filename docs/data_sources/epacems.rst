===============================================================================
EPA CEMS Hourly
===============================================================================

=================== ===========================================================
Source URL          ftp://newftp.epa.gov/dmdnload/emissions/hourly/monthly
Source Format       Comma Separated Value (.csv)
Source Years        1995-2018
Size (Download)     7.6 GB
Size (Uncompressed) ~100 GB
PUDL Code           ``epacems``
Years Liberated     1995-2018
Records Liberated   ~1 billion
Issues              `Open EPA CEMS issues <https://github.com/catalyst-cooperative/pudl/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3Aepacems>`__
=================== ===========================================================

All of the EPA's hourly Continuous Emissions Monitoring System (CEMS) data is
available. It is by far the largest dataset in PUDL at the moment, with hourly
records for thousands of plants covering decades. Note that the ETL process
can easily take all day for the full dataset. PUDL also provides a script that
converts the raw EPA CEMS data into Apache Parquet files, which can be read
and queried very efficiently from disk. For usage details run:

.. code-block:: console

    $ epacems_to_parquet --help

Thanks to `Karl Dunkle Werner <https://github.com/karldw>`_ for contributing
much of the EPA CEMS Hourly ETL code.
