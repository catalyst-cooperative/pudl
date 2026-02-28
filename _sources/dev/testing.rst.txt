.. _testing:

===============================================================================
Testing PUDL
===============================================================================

We use `pytest <https://pytest.org>`__ to specify software unit & integration tests,
including calling ``dbt build`` to run our :doc:`data_validation_quickstart` tests.
Several common test commands are available as pixi tasks for convenience.

To run the tests that will be run on a PR by our continuous integration (CI) on GitHub
before it's merged into the ``main`` branch you can use the following command:

.. code-block:: console

    $ pixi run pytest-ci

This includes building the documentation, running unit & integration tests, and checking
to make sure we've got sufficient test coverage.

.. note::

    If you aren't familiar with pytest already, you may want to check out:

    * `Getting Started with pytest <https://docs.pytest.org/en/latest/getting-started.html>`__

-------------------------------------------------------------------------------
Software Tests
-------------------------------------------------------------------------------
Our ``pytest`` based software tests are all stored under the ``test/``
directory in the main repository. They are organized into 2 main categories
each with its own subdirectory:

* **Software Unit Tests** (``test/unit/``) can be run in seconds and don't
  require any external data. They test the basic functionality of various
  functions and classes, often using minimal inline data structures that are
  specified in the test modules themselves.
* **Software Integration Tests** (``test/integration/``) test larger
  collections of functionality including the interactions between different
  parts of the overall software system and in some cases interactions with
  external systems requiring network connectivity. The main thing our
  integration tests do is run the full PUDL data processing pipeline for the
  most recent year of data. These tests take around 45 minutes to run.

-------------------------------------------------------------------------------
Running the tests and other tasks with pixi
-------------------------------------------------------------------------------

The pixi tasks that pertain to software and data tests coordinated by
``pytest`` are prefixed with ``pytest-``. To see all available pixi tasks:

.. code-block:: console

    $ pixi task list

-------------------------------------------------------------------------------
Selecting Input Data for Integration Tests
-------------------------------------------------------------------------------
The software integration tests need a year's worth of input data to process. By
default they will look in your local PUDL datastore to find it. If the data
they need isn't available locally, they will download it from Zenodo and put it
in the local datastore.

However, if you're editing code that affects how the datastore works, you probably don't
want to risk contaminating your working datastore. You can use a disposable temporary
datastore instead by using our custom ``--tmp-data`` with ``pytest``:

.. code-block:: console

   $ pytest --tmp-data test/integration

.. seealso::

    * :doc:`dev_setup` for more on how to set up a PUDL workspace and datastore.
    * :doc:`datastore` for more on how to work with the datastore in general.

-------------------------------------------------------------------------------
Running pytest Directly
-------------------------------------------------------------------------------
Running tests directly with ``pytest`` gives you the ability to run only tests from a
particular test module or even a single individual test case.  This is convenient if
you're debugging something specific or developing new test cases.

You can run pytest directly without the ``pixi run`` prefix if you're working
within the activated pixi environment, or use ``pixi run pytest`` to run it
explicitly.

If you are working on integration tests, note that most of them require processed PUDL
outputs. If you try to run a single integration test directly with pytest it will
likely end up running the fast ETL which will take 45 minutes. If you have processed
PUDL outputs locally already, you can use ``--live-dbs`` instead. This is only helpful
if the thing you're testing isn't part of the ETL itself.

Running specific tests
^^^^^^^^^^^^^^^^^^^^^^
To run the software unit tests with ``pytest`` directly:

.. code-block:: console

   $ pytest test/unit

To run only the unit tests for the Excel spreadsheet extraction module:

.. code-block:: console

   $ pytest test/unit/extract/excel_test.py

To run only the unit tests defined by a single test class within that module:

.. code-block:: console

   $ pytest test/unit/extract/excel_test.py::TestGenericExtractor

Custom PUDL pytest flags
^^^^^^^^^^^^^^^^^^^^^^^^
We have defined several custom flags to control pytest's behavior when running the PUDL
tests.

You can always check to see what custom flags exist by running ``pytest --help`` and
looking at the ``custom options`` section:

.. code-block:: console

   Custom options:
     --live-dbs            Use existing PUDL/FERC1 DBs instead of creating temporary ones.
     --tmp-data            Download fresh input data for use with this test run only.
     --etl-settings=ETL_SETTINGS
                           Path to a non-standard ETL settings file to use.
     --bypass-local-cache  If enabled, the local file cache for datastore will not be used.
     --save-unmapped-ids   Write the unmapped IDs to disk.
     --ignore-foreign-key-constraints
                           If enabled, do not check the foreign keys.

The main flexibility that these custom options provide is in selecting where the raw
input data comes from and what data the tests should be run against. Being able to
specify the tests to run and the data to run them against independently simplifies the
test suite and keeps the data and tests very clearly separated.

The ``--live-dbs`` option lets you use your existing FERC 1 and PUDL databases instead
of building a new database at all. This can be useful if you want to test code that only
operates on an existing database, and has nothing to do with the construction of that
database. For example, the EPA CEMS specific tests:

.. code-block:: console

  $ pytest --live-dbs test/integration/epacems_test.py

Assuming you do want to run the ETL and build new databases as part of the test you're
running, the contents of that database are determined by an ETL settings file. By
default, the settings file that's used is
``src/pudl/package_data/settings/etl_fast.yml`` But it's also possible to use a
different input file, generating a different database, and then run some tests against
that database.

We use the ``src/pudl/package_data/etl_full.yml`` settings file to specify an exhaustive
collection of input data.

The raw input data that all the tests use is ultimately coming from our `archives on
Zenodo <https://zenodo.org/communities/catalyst-cooperative>`__. A copy of that data
is cached locally so that it can be re-used later without needing to be downloaded
every time. Because downloading data directly from Zenodo can be slow and unreliable,
by default we download from a cached copy in Amazon's S3 storage, in a free bucket
provided by the AWS Open Data Registry at ``s3://pudl.catalyst.coop/zenodo``.

You can also force the tests to download of a fresh copy of the data to use just once,
even if you already have a local copy, which is useful when you are testing the
datastore functionality specifically.

.. code-block:: console

   $ pytest --tmp-data test/integration/etl_test.py
