.. _testing:

===============================================================================
Testing PUDL
===============================================================================

We use `pytest <https://pytest.org>`__ to specify software unit
& integration tests, including calling ``dbt build`` to run our
:doc:`data_validation_quickstart` tests. There are several ``pytest`` commands
stored as targets in the PUDL ``Makefile`` for convenience and to ensure that
we're all running the tests in similar ways by default.

To run the tests that will be run on a PR by our continuous integration (CI) on GitHub
before it's merged into the ``main`` branch you can use the following command:

.. code-block:: console

    $ make pytest-coverage

This includes building the documentation, running unit & integration tests, and checking
to make sure we've got sufficient test coverage.

.. note::

    If you aren't familiar with pytest and Make already, you may want to check out:

    * `Getting Started with pytest <https://docs.pytest.org/en/latest/getting-started.html>`__
    * `Makefile Tutorial <https://makefiletutorial.com/>`__

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
Running tests with Make
-------------------------------------------------------------------------------

The ``Makefile`` targets that pertain to software and data tests which are coordinated
by ``pytest`` are prefixed with ``pytest-``

In addition to running the ``pytest-unit`` and ``pytest-integration`` targets mentioned
above there are also:

* ``pytest-integration-full``: The integration tests, but run on all years of data
  rather than just the most recent year. This test assumes you already have the
  complete outputs from the full PUDL ETL in your ``$PUDL_OUTPUT`` directory.
* ``pytest-jupyter``: Check that select Jupyter notebooks checked into the repository
  can run successfully. (Currently disabled)
* ``pytest-coverage``: Run all the software tests and generate a test coverage report.
  This will fail if test coverage has fallen below the threshold defined in
  ``pyproject.toml``.
* ``pytest-ci``: Run the unit and integration tests (those tests that get run in CI).

Running Other Commands with Make
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are several non-test ``make`` targets. To see them all open the ``Makefile``.

* ``ferc``: Delete all existing XBRL and DBF derived FERC databases and metadata and
  re-extract them from scratch
* ``pudl``: Delete your existing ``pudl.sqlite`` DB and Parquet outputs and re-run the
  full ETL from scratch. Assumes that the FERC DBs already exist.
* ``nuke``: delete your existing FERC and PUDL databases, rebuild
  them from scratch, and run all of the tests and data validations (akin to running
  the nightly builds) for an extensive check of everything. This will take 3 hours or
  more to complete, and likely fully utilize your computer's CPU and memory.
* ``install-pudl``: Remove your existing ``pudl-dev`` ``conda`` environment and
  reinstall all dependencies as well as the ``catalystcoop.pudl`` package defined by
  the repository in ``--editable`` mode for development.
* ``docs-build``: Remove existing PUDL documentation outputs and rebuild from scratch.
* ``jlab``: start up a JupyerLab notebook server (will remain running in your terminal
  until you kill it with ``Control-C``).

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
particular test module or even a single individual test case. It's also faster because
there's no testing environment to set up. Instead, it just uses your Python environment
which should be the ``pudl-dev`` conda environment discussed in :doc:`/dev/dev_setup`.
This is convenient if you're debugging something specific or developing new test cases.

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

  custom options:
  --live-dbs            Use existing PUDL/FERC1 DBs instead of creating temporary ones.
  --tmp-data            Download fresh input data for use with this test run only.
  --etl-settings=ETL_SETTINGS
                        Path to a non-standard ETL settings file to use.
  --gcs-cache-path=GCS_CACHE_PATH
                        If set, use this GCS path as a datastore cache layer.

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
Zenodo <https://zenodo.org/communities/catalyst-cooperative>`__. However, you can
optionally tell the tests to look in a different places for more rapidly accessible
caches of that data and to force the download of a fresh copy (especially useful when
you are testing the datastore functionality specifically). By default, the tests will
use the datastore that's part of your local PUDL workspace.

For example, to run the ETL portion of the integration tests and download fresh input
data to a temporary datastore that's later deleted automatically:

.. code-block:: console

   $ pytest --tmp-data test/integration/etl_test.py
