===============================================================================
Published Data Packages
===============================================================================

We've chosen `tabular data packages <https://frictionlessdata.io/specs/tabular-data-package/>`__ as the main distribution format for PUDL because they:

* are based on a free and open standard that should work on any platform,
* are relatively easy for both humans and computers to understand,
* are easy to archive and distribute,
* provide rich metadata describing their contents,
* do not force users into any particular platform.

We our hope this will allow the data to reach the widest possible audience.

.. seealso::

    The `Frictionless Data <https://frictionlessdata.io/>`__ software and
    specifications, a project of
    `the Open Knowledge Foundation <https://okfn.org>`__

-------------------------------------------------------------------------------
Downloading Data Packages
-------------------------------------------------------------------------------

After the initial release of the PUDL software, we will automate the creation
of a standard bundle of data packages containing all of the currently
integrated data. Users who aren't working with Python, or who don't want to
set up and run the data processing pipeline themselves will be able to just
download and use the data packages directly. We intend to publish them to the
following locations:

Zenodo
^^^^^^

Integration between `Zenodo <https://zenodo.org/>`__ and
`Github <https://github.com>`__ makes it easy to automatically
`archive and issue digital object ids (DOIs) <https://guides.github.com/activities/citable-code/>`__
for any tagged release. On a regular basis, we will also upload a standard set
of data packages to Zenodo alongside the PUDL release that was used to generate
them, and the packages will also be issued citeable DOIs so they can be easily
referenced in research and other publications. Our goal is to make replication
of any analyses that depend on the released code and published data as easy to
replicate as possible.

Datahub
^^^^^^^

We also intend to regularly publish new data packages via `Datahub.io
<https://datahub.io/catalystcooperative>`__, a open data
portal which natively understands data packages, parses the included metadata,
and can help integrate the PUDL data with other open public data.

Other Sites?
^^^^^^^^^^^^

Are there other data archiving and access platforms that you'd like to see the
pudl data packages published to?  If so feel free to
`create an issue on Github <https://github.com/catalyst-cooperative/pudl/issues>`__
to let us know about it, and explain what it would add to the project. Other
sites we've thought about include:

* `Open EI <https://openei.org/wiki/Main_Page>`__
* `data.world <https://data.world/>`__

-------------------------------------------------------------------------------
Using Data Packages
-------------------------------------------------------------------------------

Once you've downloaded or generated your own tabular data packages you can use
them to do analysis on almost any platform. Below are a few examples.
`Open an issue on Github <https://github.com/catalyst-cooperative/pudl/issues>`__ and let us know if you have another example we can add.

Python, Pandas, and Jupyter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can read the datapackages into :mod:`pandas.DataFrame` for interactive
in-memory use within
`JupyterLab <https://jupyterlab.readthedocs.io/en/stable/>`__,
or for programmatic use in your own Python modules. Several example Jupyter
notebooks are deployed into your PUDL workspace ``notebooks`` directory by the
``pudl_setup`` script.

With the ``pudl`` conda environment activated you can start up a notebook
server and experiment with those notebooks by running the following from within
your PUDL workspace:

.. code-block:: console

    $ jupyter-lab --notebook-dir=notebooks

Then select the ``pudl_intro.ipynb`` notebook from the file browser on the left
hand side of the JupyterLab interface.

.. todo::

    Update ``pudl_intro.ipynb`` to read the example datapackage.

If you're using Python and need to work with larger-than-memory data,
especially the :ref:`data-epacems` dataset, we recommend checking out
`the Dask project <https://dask.org>`__, which extends the interface to
:mod:`pandas.DataFrame` objects enabling serialized, parallel and distributed
processing tasks. It can also speed up processing for in-memory tasks,
especially if you have a powerful system with multiple cores, a solid state
disk, and plenty of memory.

The R programming language
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. todo::

    Get someone who uses R to give us an example here... maybe we can get
    someone from OKFN to do it?

SQLite
^^^^^^

If you'd rather access the data via SQL, you can easily load the datapackages
into a local :mod:`sqlite3` database.

.. todo::

    Write and document datapackage bundle to SQLite script.

Microsoft Access / Excel
^^^^^^^^^^^^^^^^^^^^^^^^^

If you'd rather do spreadsheet based analysis, here's how you can pull the
datapackages into Microsoft Access and Excel.

.. todo::

    Document process for pulling data packages or datapackage bundles into
    Microsoft Access / Excel

Other Platforms
^^^^^^^^^^^^^^^

Want to submit another example? Check out :doc:`the documentation on
contributing <CONTRIBUTING>`. Wish there was an example here for your favorite
data analysis tool, but don't know what it would look like? Feel free to
`open a Github issue <https://github.com/catalyst-cooperative/pudl/issues>`__
requesting it.
