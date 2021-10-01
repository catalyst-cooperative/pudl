.. _datastore:

===============================================================================
Working with the Datastore
===============================================================================

The input data that PUDL processes comes from a variety of US government agencies.
However, these agencies typically make the data available on their websites or via FTP
without planning for programmatic access. To ensure reproducible, programmatic access,
we periodically archive the input files on the
`Zenodo <https://zenodo.org/communities/catalyst-cooperative/>`__
research archiving service maintained by CERN. (See our
`pudl-scrapers <https://github.com/catalyst-cooperative/pudl-scrapers>`__ and
`pudl-zenodo-storage <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
repositories on GitHub for more information.)

When PUDL needs a data resource, it will attempt to automatically retrieve it from
Zenodo and store it locally in a file hierarchy organized by dataset and the
versioned DOI of the corresponding Zenodo deposition.

The ``pudl_datastore`` script can also be used to pre-download the raw input data in
bulk. It uses the routines defined in the :mod:`pudl.workspace.datastore` module. For
details on what data is available, for what time periods, and how much of it there
is, see the PUDL :doc:`/data_sources/index`. At present the ``pudl_datastore`` script
downloads the entire collection of data available for each dataset. For the FERC Form
1 and EPA CEMS datasets, this is several gigabytes.

For example, to download the full :doc:`/data_sources/eia860` dataset
(covering 2001-present) you would use:

.. code-block:: console

    $ pudl_datastore --dataset eia860

For more detailed usage information, see:

.. code-block:: console

    $ pudl_datastore --help

The downloaded data will be used by the script to populate a datastore under
the ``data`` directory in your workspace, organized by data source, form, and
date::

    data/censusdp1tract/
    data/eia860/
    data/eia861/
    data/eia923/
    data/epacems/
    data/ferc1/
    data/ferc714/

If the download fails to complete successfully, the script can be run repeatedly until
all the files are downloaded. It will not try and re-download data which is already
present locally.

-------------------------------------------------------------------------------
Adding a new Dataset to the Datastore
-------------------------------------------------------------------------------

There are three components necessary to prepare a new datastet for use with the PUDL
datastore.

1. Create a ``pudl-scraper`` to download the raw data.
2. Use ``pudl-zenodo-storage`` to upload the data to Zenodo.
3. Prepare the datastore to retrieve the data from Zenodo.

In the event that data is already available on Zenodo in the appropriate format, it may
be possible to skip steps 1 and 2.

Create a scraper
^^^^^^^^^^^^^^^^

Where possible, we use `Scrapy <https://docs.scrapy.org/en/latest/>`__ to handle data
collection. Our scrapy spiders, as well as any custom scripts, are located in our
`scrapers repo <https://github.com/catalyst-cooperative/pudl-scrapers>`__.
Familiarize yourself with scrapy, and note the following.

From a scraper, a correct ouput directory takes the form: ::

    `pudl_scrapers.helpers.new_output_dir(self.settings["OUTPUT_DIR"] /
    "datastet_name")`

The ``pudl_scrapers.settings`` and ``pudl_scrapers.helpers`` can be imported
outside the context of a Scrapy scraper to achieve the same effect as needed.

To take advantage of the existing file saving pipeline, create a custom item in
the ``items.py`` collection.  Make sure that it inherits from the existing
``DataFile`` class, and ensure that your spider yields the new item.  See the
``items.py`` for examples.

If you follow those guidelines, your new scraper should play well with the rest
of the environment.

Prepare zenodo_store
^^^^^^^^^^^^^^^^^^^^

Our `zenodo_store <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
script initializes and updates data sources that we maintain on
`Zenodo <https://zenodo.org/>`__ . It prepares
`Frictionless Datapackages <https://frictionlessdata.io/>`__ from scraped files and
uploads them to the appropriate Zenodo archive.

To add a new archive to our Zenodo storage collection:

* Update ``zs.metadata`` with a UUID and metadata for the new Zenodo archive.
   These details will be used by Zenodo to identify and describe the archive on
   the website.  The UUID is used to uniquely distinguish the archive **prior to
   the creation of a DOI.**
* Prepare a new library to handle the **frictionless datapackage** descriptor of
  the archive.

  * The library name should take the form ``frictionless.DATASET_raw``.
  * The library must contain
    `frictionless data metadata <https://specs.frictionlessdata.io/data-package/#language>`__
    describing the archive.
  * The library must contain a ``datapackager(dfiles)`` function that:

    * receives a list of
      `zenodo file descriptors <https://developers.zenodo.org/#deposition-files>`__
    * converts each to an appropriate
      `frictionless datapackage resource descriptor <https://specs.frictionlessdata.io/data-resource/#language>`__
    * **Important**: The resource descriptor must include an
      additional ``descriptor["remote_url"]`` that contains
      the zenodo url to download its resource.  This will be the same
      as the ``descriptor["path"]`` at this stage.
    * If there are criteria by which you wish to be able to discover or
      filter specific resources, ``descriptor["parts"][...]`` should be
      used to denote those details.  For example,
      ``descriptor["parts"]["year"] = 2018`` would be appropriate to
      allow filtering by year.
    * Combines the resource descriptors and frictionless metadata to produce
      the complete datapackage descriptor as a python dict.
* In the ``bin/zenodo_store.py`` script:

  * Import the new frictionless library.
  * Add the new source to the ``archive_selection`` function; follow the
    format of the existing selectors.
  * Add the new source name to the help text in the ``parse_main() ..
    deposition`` argument.

The above steps should be sufficient to allow automatic initialization and
updates of the new data source on Zenodo.

You initialize an archive (preferably starting with the sandbox) by running
``zenodo_store.py --initialize --verbose --sandbox``

If successful, the DOI and url for your archive will be printed.  You will
need to visit the url to review and publish the Zenodo archive before it can
be used.

If you lose track of the DOI, you can look up the archive on Zenodo using the
UUID from ``zs.metadata``.

Prepare the Datastore
^^^^^^^^^^^^^^^^^^^^^

If you have used a scraper and zenodo_store to prepare a Zenodo archive as above, you
can add support for your archive to the datastore by adding the DOI to
pudl.workspace.datastore.DOI, under "sandbox" or "production" as appropriate.

If you want to prepare an archive for the datastore separately, the following
are required.

#. The root path must contain a ``datapackage.json`` file that conforms to the
`frictionless datapackage spec <https://specs.frictionlessdata.io/data-package/>`__
#. Each listed resource among the ``datapackage.json`` resources must include:

   * ``path`` containing the zenodo download url for the specific file.
   * ``remote_url`` with the same url as the ``path``
   * ``name`` of the file
   * ``hash`` with the md5 hash of the file
   * ``parts`` a set of key / value pairs defining additional attributes that
     can be used to select a subset of the whole datapackage. For example, the
     ``epacems`` dataset is partitioned by year and state, and
     ``"parts": {"year": 2010, "state": "ca"}`` would indicate that the
     resource contains data for the state of California in the year 2010.
     Unpartitioned datasets like the ``ferc714`` which includes all years in
     a single file, would have an empty ``"parts": {}``
