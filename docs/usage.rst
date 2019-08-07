===============================================================================
Basic Usage
===============================================================================

-------------------------------------------------------------------------------
Quickstart
-------------------------------------------------------------------------------

If you've already :ref:`installed PUDL <install-pudl>` using ``conda``,
:ref:`set up a workspace <install-workspace>`, and :ref:`activated the PUDL
conda environment <install-conda-env>` then from within your workspace you
should be able to bring up an example `Jupyter <https://jupyter.org>`__
notebook that works with PUDL data by running:

.. code-block:: console

    $ pudl_etl settings/pudl_etl_example.yml
    $ jupyter-lab --notebook-dir=notebooks

.. note::

    This example only downloads and processes a small portion of the available
    data as a demonstration. You can copy the example settings file and edit it
    to add more data. See the :doc:`data catalog <data_catalog>` for a full
    listing of all the available data.

.. _usage-etl:

-------------------------------------------------------------------------------
Running the ETL Pipeline
-------------------------------------------------------------------------------

PUDL implements a data processing pipeline. This pipeline takes raw data
provided by public agencies in a variety of formats and integrates it together
into a single (more) coherent whole. In the data-science world this is often
called "ETL" which stands for "Extract, Transform, Load."

* **Extract** the data from its original source formats and into
  :mod:`pandas.DataFrame` objects for easy manipulation.
* **Transform** the extracted data into tidy tabular data structures, applying
  a variety of cleaning routines, and creating connections both within and
  between the various datasets.
* **Load** the data into a standardized output, in our case CSV/JSON based
  `Tabular Data Packages <https://frictionlessdata.io/specs/tabular-data-package/>`__.

The PUDL python package is organized into these steps as well, with
:mod:`pudl.extract` and :mod:`pudl.transform` subpackages that contain dataset
specific modules like :mod:`pudl.extract.ferc1` and
:mod:`pudl.transform.eia923`. The Load step is currently just a single module
called :mod:`pudl.load`.

The ETL pipeline is coordinated by the top-level :mod:`pudl.etl` module, which
has a command line interface accessible via the ``pudl_etl`` script that is
installed by the PUDL Python package. The script reads a YAML file as input.
An example is provided in the ``settings`` folder that is created when you run
``pudl_setup`` (see: :ref:`install-workspace`).

To run the ETL pipeline for the example, from within your PUDL workspace you
would do:

.. code-block:: console

    $ pudl_etl settings/pudl_etl_example.yml

This should result in a bunch of Python :mod:`logging` output, describing what
the script is doing, and some outputs in the ``sqlite`` and ``datapackage``
directories within your workspace. In particular, you should see new file at
``sqlite/ferc1.sqlite`` and a new directory at ``datapackage/pudl-example``.

Under the hood, the ``pudl_etl`` script has downloaded data from the federal
agencies and organized it into a datastore locally, cloned the original FERC
Form 1 database into that ``ferc1.sqlite`` file, extracted a bunch of data from
that database and a variety of Microsoft Excel spreadsheets and CSV files, and
combined it all into the ``pudl-example`` `tabular datapackage
<https://frictionlessdata.io/specs/tabular-data-package/>`__. The metadata
describing the overall structure of the output is found in
``datapackage/pudl-example/datapackage.json`` and the associated data is
stored in a bunch of CSV files (some of which may be :mod:`gzip` compressed) in
the ``datapackage/pudl-example/data/`` directory.

You can use the ``pudl_etl`` script to download and process more or different
data by copying and editing the ``settings/pudl_etl_example.yml`` file, and
running the script again with your new settings file as an argument. Comments
in the example settings file explain the available parameters.

.. todo::

    * Create updated example settings file, ensure it explains all available
      options.
    * Integrate datastore management and ferc1 DB cloning into ``pudl_etl``
      script.

It's sometimes useful to :doc:`update the datastore <datastore>` or :doc:`clone
the FERC Form 1 database <clone_ferc1>` independent of running the full ETL
pipeline. Those (optional) processes are explained next.
