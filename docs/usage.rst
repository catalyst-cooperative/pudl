===============================================================================
Basic Usage
===============================================================================

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
  `Tabular Data Packages <https://frictionlessdata.io/specs/tabular-data-package/>`__, and subsequently an SQLite database or Apache Parquet files.

The PUDL python package is organized around these steps as well, with
:mod:`pudl.extract` and :mod:`pudl.transform` subpackages that contain dataset
specific modules like :mod:`pudl.extract.ferc1` and
:mod:`pudl.transform.eia923`. The Load step is handled by the :mod:`pudl.load`,
subpackage, which contains modules that deal separately with generating CSVs
containing the output data (:mod:`pudl.load.csv`), and the JSON files that
contain the corresponding metadata (:mod:`pudl.load.metadata`).

The ETL pipeline is coordinated by the top-level :mod:`pudl.etl` module, which
has a command line interface accessible via the ``pudl_etl`` script which is
installed by the PUDL Python package. The script reads a YAML file as input.
An example is provided in the ``settings`` folder that is created when you run
``pudl_setup`` (see: :ref:`install-workspace`).

To run the ETL pipeline for the example, from within your PUDL workspace you
would need to run four commands, which
:doc:`clone the FERC Form 1 database <clone_ferc1>`, convert
that and other raw data into datapackages, and loads those datapackages into an
SQLite database, respectively:

.. code-block:: console

    $ ferc1_to_sqlite settings/ferc1_to_sqlite_example.yml
    $ pudl_etl settings/etl_example.yml
    $ datapkg_to_sqlite \
        -o datapkg/pudl-example/pudl-merged \
        datapkg/pudl-example/ferc1-example/datapackage.json \
        datapkg/pudl-example/eia-example/datapackage.json \
        datapkg/pudl-example/epaipm-example/datapackage.json
    $ epacems_to_parquet datapkg/pudl-example/epacems-eia-example/datapackage.json

These commands should result in a bunch of Python :mod:`logging` output,
describing what the script is doing, and outputs in the ``sqlite``,
``datapkg``, and ``parquet`` directories within your workspace. In particular,
you should see new files at ``sqlite/ferc1.sqlite`` and ``sqlite/pudl.sqlite``,
and a new directory at ``datapkg/pudl-example`` containing several datapackage
directories, one for each of the ``ferc1``, ``eia`` (Forms 860 and 923),
``epacems-eia``, and ``epaipm`` datasets.

Under the hood, these scripts are extracting data from the datastore, including
spreadsheets, CSV files, and binary DBF files, generating a SQLite database
containing the raw FERC Form 1 data, and combining it all into
``pudl-example``, which is a bundle of
`tabular datapackages <https://frictionlessdata.io/specs/tabular-data-package/>`__.
that can be used together to create a database.

Each of the data packages which are part of the bundle have metadata describing
their structure, stored in a file called ``datapackage.json`` The data itself
is stored in a bunch of CSV files (some of which may be :mod:`gzip` compressed)
in the ``data/`` directories of each data package.

You can use the ``pudl_etl`` script to process more or different data by
copying and editing the ``settings/etl_example.yml`` file, and running the
script again with your new settings file as an argument. Comments in the
example settings file explain the available parameters.

If you want to re-run ``pudl_etl`` and replace an existing bundle of data
packages, you can use ``--clobber``. If you want to generate a new data
packages with a new or modified settings file, you can change the name of the
output datapackage bundle in the configuration file.
