.. _datastore:

===============================================================================
Collecting a Datastore
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



===============================================================================
Developing a new Dataset for the Datastore
===============================================================================

There are three components necessary to prepare a new datastet for use with the
PUDL datastore.

1. Prepare scraper to download the source data.
2. Prepare the zen_store to upload the data to Zenodo.
3. Prepare the datastore to retrieve the data from Zenodo.

In the event that data is already available on Zenodo, it may be possible to
skip steps 1 and 2.

-------------------------------------------------------------------------------
Prepare a scraper
-------------------------------------------------------------------------------

Where possible, we use `Scrapy <https://docs.scrapy.org/en/latest/>`__ to
handle data collection.  Our scrapy spiders, as well as any custom scripts, are
located in our `scrapers repo
<https://github.com/catalyst-cooperative/scrapers>`__.  Familiarize yourself
with scrapy, and note the following.

From a scraper, a correct ouput directory takes the form: ::
    `pudl.helpers.new_output_dir(self.settings["OUTPUT_DIR"] /
    "datastet_name")`

The ``pudl.settings`` and ``pudl.helpers`` can be imported outside the context
of a Scrapy scraper to achieve the same effect as needed.

To take advantage of the existing file saving pipeline, create a custom item in
the ``items.py`` collection.  Make sure that it inherits from the existing
``DataFile`` class, and ensure that your spider yields the new item.  See the
``items.py`` for examples.

If you follow those guidelines your new scraper should play well with the rest
of the environment.


-------------------------------------------------------------------------------
Prepare zen_store
-------------------------------------------------------------------------------

Our `zen_store <https://github.com/catalyst-cooperative/zen_storage>`__ script
initializes and updates data sources that we maintain on `Zenodo
<https://zenodo.org/>`__. It prepares `Frictionless Datapackages
https://frictionlessdata.io/` from scraped files and uploads them to the
appropriate Zenodo archive.

To add a new archive to our zen storage collection:

#. Update `zs.metadata` with a uuid and metadata for the new Zenodo archive.
These details will be used by Zenodo to identify and describe the archive on
the website.  The UUID is used to uniquely distinguish the archive *prior to
the creation of a DOI.*
#. Prepare a new library to handle the *frictionless datapackage* descriptor of
the archive.

   * The library name should take the form `frictionless.DATASET_source`.
   * The library must contain `frictionless data metadata
     <https://specs.frictionlessdata.io/data-package/#language>`__ describing
     the archive.
   * The library must contain a `datapackager(dfiles)` function that:

      #. recieves a list of `zenodo file descriptors
      <https://developers.zenodo.org/#deposition-files>`__
      #. converts each to an appropriate `frictionless datapackage resource
      descriptor <https://specs.frictionlessdata.io/data-resource/#language>`__

         * **Important**: The resource descriptor must include and
            additional ``descriptor["parts"]["remote_url"]`` that contains
            the zenodo url to download its resource.  This will be the same
            as the ``descriptor["path"]`` at this stage.
         * If there are criteria by which you wish to be able to discover or
           filter specific resources, ``discriptor["parts"][...]`` should be
           used to denote those details.  For example,
           ``descriptor["parts"]["year"] = 2018`` would be appropriate to
           allow filtering by year.

      #. Combines the resource descriptors and frictionless metadata to produce
         the complete datapackage descriptor as a python dict.

#. In the ``bin/zen_store.py`` script:

   * Import the new frictionless library.
   * Add the new source to the ``archive_selection`` function; follow the
     format of the existing selectors.
   * Add the new source name to the help text in the ``parse_main() ..
     deposition`` argument.

The above steps should be sufficient to allow automatic initialization and
updates of the new data source on Zenodo.


You initialize an archive (preferably starting with the sandbox) by running
zen_store.py --initialize --verbose --sandbox

If successful, the DOI and url for your archive will be printed.  You will
need to visit the url to review and publish the Zenodo archive  before it can
be used.

If you lose track of the DOI, you can look up the archive on Zenodo using the
UUID from zs.metadata.

-------------------------------------------------------------------------------
Prepare the Datastore
-------------------------------------------------------------------------------

If you have used a scraper and zen_store to prepare a Zenodo archive as above,
you can add support for your archive to the datastore by adding the DOI to
pudl.workspace.datastore.DOI, under "sandbox" or "production" as appropriate.

If you want to prepare an archive for the datastore separately, the following
are required.

#. The root path must contain a ``datapackage.json`` file that conforms to the
`frictionless datapackage spec
<https://specs.frictionlessdata.io/data-package/>`__
#. Each listed resource among the ``datapackage.json`` resources must include:

   * ``path`` containing the zenodo download url for the specific file.
   * ``parts.remote_url`` with the same url as the ``path``
   * ``name`` of the file
   * ``hash`` with the md5 hash of the file

#. For resources, any additional metadata to be used for filtering /
identification should be included as a key / value pair under the ``parts``
section.
