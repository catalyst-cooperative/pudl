.. _datastore:

===============================================================================
Creating a Datastore
===============================================================================

The input data that PUDL processes comes from a variety of US government
agencies. These agencies typically make the data available on their websites
or via FTP without really planning for programmatic access.

The ``pudl_data`` script helps you obtain and organize this data locally, for
use by the rest of the PUDL system. It uses the routines defined in the
:mod:`pudl.workspace.datastore` module. For details on what data is available,
for what time periods, and how much of it there is, see the
:doc:`data_catalog`.

For example, if you wanted to download the 2018 :ref:`data-epacems` data for
Colorado:

.. code-block:: console

    $ pudl_data --sources epacems --states CO --years 2018

If you do not specify years, the script will retrieve all available data. So
to get everything for :ref:`data-eia860` and :ref:`data-eia923` you would run:

.. code-block:: console

    $ pudl_data --sources eia860 eia923

The script will download from all sources in parallel, so if you have a fast
internet connection and need a lot of data, doing it all in one go makes sense.
To pull down **all** the available data for all the sources (10+ GB) you would
run:

.. code-block:: console

    $ pudl_data --sources eia860 eia923 epacems ferc1 epaipm

For more detailed usage information, see:

.. code-block:: console

    $ pudl_data --help

The downloaded data will be used by the script to populate a datastore under
the ``data`` directory in your workspace, organized by data source, form, and
date::

    data/eia/form860/
    data/eia/form923/
    data/epa/cems/
    data/epa/ipm/
    data/ferc/form1/

If the download fails (e.g. the FTP server times out), this command can be run
repeatedly until all the files are downloaded. It will not try and re-download
data which is already present locally, unless you use the ``--clobber`` option.
Depending on which data sources, how many years or states you have requested
data for, and the speed of your internet connection, this may take minutes to
hours to complete, and can consume 20+ GB of disk space even when the data is
compressed.

Occasionally, the federal agencies will re-organize their websites or FTP
servers, changing the names or locations of the files, causing the download
script to fail. We try and update the version of the script in the Github
repository as quickly as possible when this happens, but it may take a while
for those changes to show up in the released software. We are working on
creating an automatically updated versioned archive of the raw source files
on `Zenodo <https://zenodo.org/communities/catalyst-cooperative/>`__ so we
don't need to refer directly to these unstable files that. See our
`scrapers <https://github.com/catalyst-cooperative/scrapers>`__ and
`zen_storage <https://github.com/catalyst-cooperative/zen_storage>`__
Github repositories for more information.
