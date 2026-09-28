.. _testing:

===============================================================================
Testing PUDL
===============================================================================

We use `pytest <https://pytest.org>`__ to specify software unit, integration, and
pipeline tests, including calling ``dbt build`` to run our
:doc:`data_validation_quickstart` tests. Several common test commands are available as
pixi tasks for convenience.

For day-to-day work, the most commonly used pixi testing tasks are:

.. code-block:: console

   $ pixi run pytest-unit         # runs in ~1 minute
   $ pixi run pytest-integration  # runs in ~2-5 minutes, no Dagster ETL required

``pytest-unit`` also runs automatically as a pre-commit hook on every commit, and
both ``pytest-unit`` and ``pytest-integration`` runs in GitHub Actions on every push.

To run everything that's required before a PR can merge -- including the slower
pipeline and data validation tests that only run in the merge queue -- use:

.. code-block:: console

    $ pixi run pytest-ci  # runs in ~45-60 minutes, does a "fast" ETL

This includes building the documentation, running the unit, integration, and pipeline
tests, all dbt data validations other than the row count checks, and checking to make
sure we've got sufficient test coverage.

.. note::

    If you aren't familiar with pytest already, you may want to check out:

    * `Getting Started with pytest <https://docs.pytest.org/en/latest/getting-started.html>`__

-------------------------------------------------------------------------------
Software Tests
-------------------------------------------------------------------------------
Our ``pytest`` based software tests are all stored under the ``tests/`` directory in
the main repository. They're organized into four tiers, primarily distinguished by how
long they take to run:

* **Software Unit Tests** (``tests/unit/``) can be run in seconds and don't
  require any external data or a Dagster ETL run. They test the basic functionality of
  various functions and classes, often using minimal inline data structures that are
  specified in the test modules themselves. These run as a pre-commit hook on every
  commit.
* **Software Integration Tests** (``tests/integration/``) test the interactions
  between different parts of the overall software system, and in some cases
  interactions with external systems requiring network connectivity (e.g. downloading
  a small amount of data from Zenodo). Like the unit tests, they do **not** depend on a
  full Dagster ETL run, so they stay fast (a few minutes at most) and run in GitHub
  Actions on every push.
* **Pipeline Tests** (``tests/pipeline/``) run a Dagster-managed fast ETL
  using ``dg_pytest.yml`` and then exercise code against those outputs -- an
  end-to-end smoke test of the pipeline. Because running the fast ETL takes ~45
  minutes these only run in the merge queue as a final check before a PR merges into
  ``main``, rather than on every push.
* **Data Validation Tests** (``tests/validate/``) check that the outputs of a
  pipeline test run are valid using our ``dbt`` data test suite. These tests depend on
  the pipeline tests having run and produced outputs. They also run in the merge queue
  (with the exception of the dbt row-count checks, which expect all years of data to be
  processed).

A ``pytest_collection_modifyitems`` hook in ``tests/conftest.py`` enforces this split
automatically: a test outside ``tests/pipeline/``/``tests/validate/`` that depends on
the ETL, or a test inside either of those directories that doesn't, will fail
collection with a message telling you where to move it.

-------------------------------------------------------------------------------
Running the tests and other tasks with pixi
-------------------------------------------------------------------------------

The pixi tasks that pertain to software and data tests coordinated by
``pytest`` are prefixed with ``pytest-``. To see all available pixi tasks:

.. code-block:: console

    $ pixi task list

-------------------------------------------------------------------------------
Selecting Input Data for Pipeline Tests
-------------------------------------------------------------------------------
The pipeline tests need a year or two of input data to process. By
default they will look in your local PUDL datastore to find it. If the data
they need isn't available locally, they will download it from Zenodo and put it
in the local datastore.

However, if you're editing code that affects how the datastore works, you probably don't
want to risk contaminating your working datastore. You can use a disposable temporary
datastore instead by using our custom ``--temp-pudl-input`` with ``pytest``:

.. code-block:: console

   $ pixi run pytest --temp-pudl-input tests/pipeline

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

If you are working on pipeline tests, note that they require processed PUDL outputs.
If you try to run a single pipeline test directly with pytest it will likely end up
running the fast ETL which will take 45 minutes. If you have processed PUDL outputs
locally already, you can use ``--live-pudl-output`` instead. This is only helpful if
the thing you're testing isn't part of the ETL itself.

Running specific tests
^^^^^^^^^^^^^^^^^^^^^^
To run the software unit tests with ``pytest`` directly:

.. code-block:: console

   $ pixi run pytest tests/unit

To run only the unit tests for the Excel spreadsheet extraction module:

.. code-block:: console

   $ pixi run pytest tests/unit/extract/excel_test.py

To run only the unit tests defined by a single test class within that module:

.. code-block:: console

   $ pixi run pytest tests/unit/extract/excel_test.py::TestGenericExtractor

Custom PUDL pytest flags
^^^^^^^^^^^^^^^^^^^^^^^^
We have defined several custom flags to control pytest's behavior when running the PUDL
tests.

You can always check to see what custom flags exist by running ``pytest --help`` and
looking at the ``custom options`` section:

.. code-block:: console

   Custom options:
     --live-pudl-output    Use existing PUDL/FERC1 DBs instead of creating temporary ones.
     --temp-pudl-input     Download fresh input data for use with this test run only.
     --dg-config=PATH      Path to a non-standard Dagster config file to use.
     --bypass-local-cache  If enabled, the local file cache for datastore will not be used.

The main flexibility that these custom options provide is in selecting where the raw
input data comes from and what data the tests should be run against. Being able to
specify the tests to run and the data to run them against independently simplifies the
test suite and keeps the data and tests very clearly separated.

The ``--live-pudl-output`` option lets you use your existing FERC 1 and PUDL databases
instead of building a new database at all. This can be useful if you want to test code
that only operates on an existing database, and has nothing to do with the construction
of that database. For example:

.. code-block:: console

   $ pixi run pytest --live-pudl-output tests/pipeline/analysis

The dbt data validations can be selected separately from the rest of the pipeline suite
by running the dedicated validation module directly. For example:

.. code-block:: console

   $ pixi run pytest --live-pudl-output tests/validate/data_test.py

Assuming you do want to run the ETL and build new databases as part of the test you're
running, the contents of that database are determined by the Dagster config file passed
via ``--dg-config``. By default, pytest uses
``src/pudl/package_data/settings/dg_pytest.yml``. That Dagster config file points at an
data config YAML file and contains the runtime settings needed for the prebuild.

If you want to run tests against an existing local full build instead, use the pixi
tasks we've defined for the nightly builds, which use
``--live-pudl-output`` and ``--dg-config src/pudl/package_data/settings/dg_full.yml``:

.. code-block:: console

   $ pixi run pytest-pipeline-nightly
   $ pixi run pytest-validate-nightly

.. note::

   ``--live-pudl-output`` is intentionally guarded against running unit and integration
   tests in the same pytest session, since the two suites need incompatible
   ``PUDL_OUTPUT`` environment variable handling.

The raw input data that all the tests use is ultimately coming from our `archives on
Zenodo <https://zenodo.org/communities/catalyst-cooperative>`__. A copy of that data
is cached locally so that it can be re-used later without needing to be downloaded
every time. Because downloading data directly from Zenodo can be slow and unreliable,
by default we download from a cached copy in Amazon's S3 storage, in a free bucket
provided by the AWS Open Data Registry at ``s3://pudl.catalyst.coop/zenodo``.

You can also force the tests to download a fresh copy of the data to use just once,
even if you already have a local copy, which is useful when you are testing the
datastore functionality specifically.

The tests that most directly exercise the datastore download path are the CSV and Excel
extractor tests (which read archived data files via the ``zenodo_datastore`` fixture)
and the Zenodo datapackage tests (which verify that datapackage descriptors are
reachable):

.. code-block:: console

   $ pixi run pytest --temp-pudl-input \
       tests/integration/extract/csv_test.py \
       tests/integration/extract/excel_test.py \
       tests/integration/workspace/zenodo_datapackage_test.py

The FERC extractor pipeline tests (``ferc_dbf_extract_test.py`` and
``ferc_xbrl_extract_test.py`` under ``tests/pipeline/extract/``) also use the
datastore but additionally require building a FERC SQLite database, so they are more
heavyweight and slower to run.
