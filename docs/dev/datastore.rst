.. _datastore:

===============================================================================
Working with the Datastore
===============================================================================

The input data that PUDL processes comes from a variety of US government
agencies. However, these agencies typically make the data available on their
websites or via FTP without planning for programmatic access. To ensure
reproducible, programmatic access, we periodically archive the input files on
the `Zenodo <https://zenodo.org/communities/catalyst-cooperative/>`__ research
archiving service maintained by CERN. (See our `pudl-archiver
<https://github.com/catalyst-cooperative/pudl-archiver>`__ repository on GitHub
for more information.)

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

We maintain a tool at `pudl-archiver
<https://www.github.com/catalyst-cooperative/pudl-archiver>`__ that manages the
archival and versioning of datasets. See the `documentation
<https://github.com/catalyst-cooperative/pudl-archiver#adding-a-new-dataset>`__
for information on adding datasets to the datastore.


Prepare the Datastore
^^^^^^^^^^^^^^^^^^^^^

If you have used pudl-archiver to prepare a Zenodo archive as above, you
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
