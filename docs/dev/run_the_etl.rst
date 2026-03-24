.. _run_the_etl:

===============================================================================
Running the ETL Pipeline
===============================================================================

So you want to run the PUDL data processing pipeline? This is the most involved way
to get access to PUDL data. It's only recommended if you want to edit the ETL process
or contribute to the codebase. Check out the :doc:`/data_access` documentation if you
just want to use the data we process and distribute.

These instructions assume you have already gone through the :ref:`dev_setup`.

Alembic
-------

PUDL uses `Alembic <https://alembic.sqlalchemy.org>`__ to manage the creation our
database and migrations of the schema as it changes over time. However, we only use
file-based databases (SQLite, DuckDB) and these migrations are mostly a way to allow
us to change the schema without needing to repopulate the entire database from scratch.
They are not used in production.

Database initialization
^^^^^^^^^^^^^^^^^^^^^^^

Before we run anything, we'll need to make sure that the schema in the database
actually matches the schema defined by the code. Run ``pixi run alembic upgrade head``
to create the database with the right schema. If you already have a ``pudl.sqlite``
you'll probably need to delete it first.

Database schema migration
^^^^^^^^^^^^^^^^^^^^^^^^^

If you've changed the database schema locally (by renaming a column, adding a table,
defining a new primary key, changing a datatype, etc.), you'll need to make a migration
reflecting that change and apply the migration to the database to keep the database
schema synchronized with the code:

.. code-block:: bash

    $ pixi run alembic revision --autogenerate -m "Add my cool table"
    $ pixi run alembic upgrade head
    $ git add migrations
    $ git commit -m "Migration: added my cool table"

When switching branches, Alembic may refer to a migration version that is not
on your current branch. This will manifest as an error like this when running an
Alembic command::

    FAILED: Can't locate revision identified by '29d443aadf25'

If you encounter that, you will want to check out the git branch that *does*
include that migration in the ``migrations`` directory. Then you should run
``alembic downgrade head-1`` to revert the database to the prior version. Then
you can go back to the branch that doesn't have your migration, and use Alembic
in peace.

If the migrations have diverged for more than one revision, you can specify the
specific version you would like to downgrade to with its hash. You may also
want to keep a copy of the old SQLite database around, so you can easily switch
between branches without having to regenerate data.

More information can be found in the `Alembic docs
<https://alembic.sqlalchemy.org/en/latest/tutorial.html>`__.

Dagster
-------

Catalyst uses `Dagster <https://docs.dagster.io/>`__ to manage our data pipelines.
Dagster is an open source data orchestration framework written in Python. It makes it
easy to manage data dependences, parallelize processes, cache results and handle IO.

If you are interested in contributing to PUDL, you may want to familiarize yourself with
Dagster's excellent documentation:

* `Getting Started (open source) <https://docs.dagster.io/getting-started/quickstart>`__
* `Dagster Core Concepts <https://docs.dagster.io/getting-started/concepts>`__
* `Dagster Basics Tutorial <https://docs.dagster.io/dagster-basics-tutorial>`__
* `Dagster Essentials <https://courses.dagster.io/courses/dagster-essentials>`__ (Dagster Course)

If you use coding agents, you may also want to check out `the Dagster agent skills
<https://github.com/dagster-io/skills>`__:

* `dagster-expert <https://github.com/dagster-io/skills/blob/master/skills/dagster-expert/skills/dagster-expert/SKILL.md>`__
* `dignified-python <https://github.com/dagster-io/skills/blob/master/skills/dignified-python/skills/dignified-python/SKILL.md>`__
* `AI Driven Data Engineering <https://courses.dagster.io/courses/ai-driven-data-engineering>`__ (Dagster Course)

These skills are also configured in the PUDL repo and can be installed with this pixi
task (which uses `npx skill <https://www.npmjs.com/package/skills>`__).

.. code-block:: console

   $ pixi run install-skills

Because Dagster's documentation is extensive and constantly being updated, the rest of
this section will focus only on the specifics of the PUDL project, with links to the
Dagster docs for more info.

Core Dagster concepts used in PUDL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **`Definitions <https://docs.dagster.io/getting-started/concepts#definitions>`__**
  are the top-level collection of Dagster objects that get loaded into a code location.
  They bundle together the assets, asset checks, resources, jobs, schedules, and
  sensors that Dagster can see and execute. In PUDL, the canonical Dagster assembly
  lives in :mod:`pudl.dagster` and is exposed via :data:`pudl.dagster.defs`, while
  :mod:`pudl.definitions` remains the stable top-level code location used by ``dg``.
  The package is split by Dagster abstraction so contributors can edit the relevant
  layer directly:

  - :mod:`pudl.dagster.assets` loads and groups assets.
  - :mod:`pudl.dagster.asset_checks` defines Dagster asset checks.
  - :mod:`pudl.dagster.resources` defines the default resource set.
  - :mod:`pudl.dagster.jobs` defines the standard PUDL jobs.
  - :mod:`pudl.dagster.sensors` defines Dagster sensors.
  - :mod:`pudl.dagster.config` contains reusable run-configuration helpers.
  - :mod:`pudl.dagster.build` assembles :class:`dagster.Definitions` via
    :func:`pudl.dagster.build_defs`.

* **`Assets <https://docs.dagster.io/guides/build/assets>`__** are the
  primary building blocks in Dagster. They represent the underlying entities in our
  pipelines, such as database tables or machine learning models. In PUDL, most assets
  represent a :py:class:`pandas.DataFrame` that is written to Parquet
  and SQLite files on disk. Depending on which part of the PUDL DAG you are looking at,
  assets might represent messy raw dataframes extracted from spreadsheets, partially
  cleaned intermediary dataframes, or fully normalized tables ready for distribution.
* **`Resources <https://docs.dagster.io/guides/build/external-resources>`__** are
  objects used by Dagster assets to provide access to external systems, databases, or
  services. In PUDL, we've defined a :py:class:`pudl.workspace.datastore.Datastore`
  Resource that pulls our raw input data from `archives on Zenodo
  <https://zenodo.org/communities/catalyst-cooperative/>`__ identified by DOI. The
  :py:class:`pudl.workspace.datastore.ZenodoDoiSettings` Resource defines the current
  Zenodo DOI for each dataset. We also store our dataset-specific ETL settings (like
  what years of EIA-861 data to process) in a Resource
  :py:class:`pudl.dagster.resources.PudlEtlSettingsResource`.
* **`IO Managers <https://docs.dagster.io/guides/build/io-managers>`__** in Dagster let
  us keep the code for data processing separate from the code for reading and writing
  data. PUDL defines I/O Managers for reading data out of the FERC SQLite databases we
  curate, for reading and writing Parquet files, and for writing out to SQLite. For
  example :class:`pudl.dagster.io_managers.PudlMixedFormatIOManager` allows assets to
  read and write dataframes to SQLite and Parquet-backed outputs using a single logical
  interface.
* **`Jobs <https://docs.dagster.io/guides/build/jobs>`__** are preconfigured collections
  of assets, resources and IO Managers.  Jobs are the main unit of execution in Dagster.
  The main jobs assembled in :mod:`pudl.dagster` are:

  - ``ferc_to_sqlite`` to rebuild the raw FERC prerequisite databases only.
  - ``pudl`` to run the main PUDL ETL assuming those raw FERC databases already exist.
  - ``pudl_with_ferc_to_sqlite`` to run the full end-to-end build in one Dagster job.
  - ``ferceqr`` a DuckDB based pipeline to process the very large FERC EQR dataset.

* **`Configs <https://docs.dagster.io/guides/operate/configuration/run-configuration>`__**
  are the runtime settings passed to Dagster jobs, assets, and resources to control
  what gets executed and how. In PUDL, we usually store these settings in YAML files
  like ``dg_fast.yml``, ``dg_full.yml``, ``dg_pytest.yml``, and ``dg_nightly.yml``,
  which configure execution options and shared resources like ``etl_settings``. The
  reusable helpers that assemble these run configs live in
  :mod:`pudl.dagster.config`.

The Dagster Web UI
^^^^^^^^^^^^^^^^^^

`The Dagster UI <https://docs.dagster.io/guides/operate/webserver>`__ is the main
interactive interface for inspecting the PUDL Dagster code location, launching jobs
and asset materializations, reviewing logs, and debugging runs without dropping down
to the CLI for every operation.

The Dagster CLI: ``dg``
^^^^^^^^^^^^^^^^^^^^^^^

``dg`` is `Dagster's official CLI <https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference>`__.
It can perform many of the same actions managed through the Dagster UI, but is better
suited to programmatic usage.  PUDL is configured as a ``dg`` project.  Some PUDL
specific usage examples:

.. code-block:: console

    # Start up the Dagster UI webserver and daemons
    $ pixi run dg dev
    # Launch a full job with its default config
    $ pixi run dg launch --job pudl
    # Select a subset of assets to materialize
    $ pixi run dg launch --assets "group:raw_eia861"
    # List all of the Dagster definitions
    $ pixi run dg list defs

.. _run-dagster-ui:

Running the ETL via the Dagster UI
----------------------------------

Dagster needs a directory to store run logs system state, and interim assets that are
not written to Parquet or SQLite for distribution. Create a new directory
**outside of your cloned PUDL repository** and then define an environment variable
named ``DAGSTER_HOME`` to the path of the new directory. E.g.

.. code-block:: console

    $ echo "export DAGSTER_HOME=/path/to/dagster_home" >> ~/.zshrc # zsh
    $ echo "export DAGSTER_HOME=/path/to/dagster_home" >> ~/.bashrc # bash
    $ set -Ux DAGSTER_HOME /path/to/dagster_home # fish

Add ``DAGSTER_HOME`` to the current session with

.. code-block:: console

    $ export DAGSTER_HOME=/path/to/dagster_home

Once ``DAGSTER_HOME`` is set, launch the dagster UI by running:

.. code-block:: console

    $ pixi run dg dev

.. note::

    If ``DAGSTER_HOME`` is not set, you will still be able to execute jobs but dagster
    logs and outputs of assets that use the default `fs_io_manager
    <https://docs.dagster.io/_apidocs/io-managers#dagster.fs_io_manager>`__ will be
    saved to a temporary directory that is deleted when the ``dagster`` process exits.

This will launch the Dagster UI on `localhost:3000 <http://localhost:3000/>`__. See the
`Dagster UI docs <https://docs.dagster.io/guides/operate/webserver>`__` for all the
details on how to use the UI.

Cloning the FERC databases
^^^^^^^^^^^^^^^^^^^^^^^^^^

The raw FERC SQLite databases are part of the ``raw_ferc_to_sqlite`` asset group.  If
you only need those outputs, select the ``ferc_to_sqlite`` job and hit ``Materialize
All``, or you can select the specific FERC Form you actually need. If you want to run
the whole ETL from scratch, use the ``pudl_with_ferc_to_sqlite`` job. The ``pudl`` job
is intended for day-to-day development once compatible raw FERC outputs have been
materialized locally. See :doc:`/dev/clone_ferc1` for more background on this process.

Running the PUDL ETL
^^^^^^^^^^^^^^^^^^^^

For most day-to-day development, you will want to select the ``pudl`` job. This will
bring you to a window that displays all of the assets and their dependencies. Subsets
of the asset graph are organized by asset groups, which are helpful for visualizing and
executing subsets of the asset graph.

To execute the whole ``pudl`` job end-to-end click "Materialize all". Depending on how
many CPUs and how much memory your computer has, this may take hours. On an M1 Macbook
Pro with 32GB of RAM and 10 CPUs it takes about 90 minutes. To run the full ETL you'll
need at least 16GB of RAM.

Read the
:ref:`resource_config` section to learn more.  To view the status of the run, click the
date next to "Latest run:".

You can also re-execute specific assets by selecting one or multiple assets in the
"Overview" tab and clicking "Materialize selected".  This is helpful if you are updating
the logic of a specific asset and don't want to rerun the entire ETL.

.. note::

  To process a subset of years for a specific asset group, select the asset group,
  shift+click "Materialize all" and configure the ``etl_settings`` resource with the
  desired years.

See :ref:`troubleshooting_dagster` for tips on how to fix common issues we run into.

Running the FERC EQR ETL
^^^^^^^^^^^^^^^^^^^^^^^^^^
All processing for FERC EQR data is contained in a separate ETL from the
rest of PUDL. This is because the dataset is too large to archive the raw
data on Zenodo. This means the ETL can only be run by developers with credentials
to access private cloud storage containing the raw data. Any external
contributors interested in working on this ETL should contact the Catalyst team
to set up access to the raw data.

The FERC EQR ETL is contained in a Dagster job called ``ferceqr``.
Executing this job from the Dagster UI is slightly different from the main
PUDL ETL jobs because the EQR job uses Dagster partitions. After selecting
"Materialize All" (or "Materialize selected" for a selection of assets),
a screen will popup allowing you to select the partitions to execute.
From here you can select a set of year-quarter combinations. This will
trigger a ``backfill``, which will execute each partition in its own ``run``.
To properly handle a ``backfill``, you will need to configure dagster to use a
``QueuedRunCoordinator``. This can be done using a ``dagster.yaml`` file in your
``DAGSTER_HOME`` directory with the following content:

.. code-block:: yaml

   run_coordinator:
     module: dagster.core.run_coordinator
     class: QueuedRunCoordinator
     config:
       tag_concurrency_limits:
         - key: "dagster/backfill"
           limit: 2

The ``config`` section shown above is not strictly necessary, but will limit the
number of concurrent ``runs`` Dagster will start, which can be helpful to avoid
out-of-memory issues while running many quarters in one ``backfill``.

.. _run-cli:

Running the ETL via CLI
-----------------------

The ``dg`` command line interface is Dagster's official tool and has a ton of built-in
functionality. For full documentation see the Dagster docs:
`dg CLI reference <https://docs.dagster.io/api/clis/dg-cli/dg-cli-reference>`__.

These commands are a quick way to confirm your local Dagster setup is healthy before
launching runs:

.. code-block:: console

    $ pixi run dg check toml
    $ pixi run dg check defs --verbose
    $ pixi run dg list defs

You can also kick off jobs directly with ``dg launch``.  The Dagster UI does not need to
be running for this to work, but if it is running, you'll see the run appear in it.

.. code-block:: console

  $ pixi run dg launch --job ferc_to_sqlite
  $ pixi run dg launch --job pudl
  $ pixi run dg launch --job pudl_with_ferc_to_sqlite --config src/pudl/package_data/settings/dg_full.yml

You can also target specific assets rather than an entire job, and use Dagster's rich
`asset selection syntax <https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference>`__
to pick and choose:

.. code-block:: console

  # Materialize all assets in the raw_eia861 group
  $ pixi run dg launch --assets "group:raw_eia861"
  # Materialize all assets upstream and downstream of a table
  $ pixi run dg launch --assets "+key:core_eia923__fuel_receipts_costs+"

We also have a ``pixi`` task defined in ``pyproject.toml`` to process all data with
the full default configuration (this can take hours):

.. code-block:: console

    $ pixi run pudl

Dagster Config and PUDL ETL Settings Files
------------------------------------------

The ``dg launch`` command can read run configuration from YAML files. This avoids
undue command line complexity and preserves a record of how the pipeline was run.
The standard Dagster config files we use are:

- ``src/pudl/package_data/settings/dg_fast.yml`` for smaller, faster local runs.
- ``src/pudl/package_data/settings/dg_full.yml`` for full local builds.
- ``src/pudl/package_data/settings/dg_pytest.yml`` for integration-test prebuilds.
- ``src/pudl/package_data/settings/dg_nightly.yml`` for the nightly cloud build.

.. warning::

  The Dagster config file selects resources and execution settings. The referenced
  ETL settings YAML still determines partitions, years, and other dataset-specific
  parameters, but job and asset selection determine which parts of the graph run.

Each Dagster config file includes execution options and resource configuration,
including the ``etl_settings_path`` used by the shared ``etl_settings`` resource.
The referenced ETL settings YAML files specify which partitions of each dataset should
be processed, and are generally structured like this:

.. code-block::

   # FERC-to-SQLite settings
   ferc_to_sqlite:
     ├── ferc1_dbf
     |   └── years
     ├── ferc1_xbrl
     |   └── years
     └── ferc2_xbrl
         └── years

   # PUDL ETL settings
   name : unique name identifying the etl outputs
   title : short human readable title for the etl outputs
   description : a longer description of the etl outputs
   datasets:
     ├── dataset name
     │    └── dataset etl parameter (e.g. years) : editable list of years
     └── dataset name
          └── dataset etl parameter (e.g. years) : editable list of years

.. seealso::

   For an exhaustive listing of the available parameters, see the ETL settings models in
   :mod:`pudl.settings` and the packaged settings files under
   ``src/pudl/package_data/settings/``.

In general, you should not fiddle with these settings unless you are actually adding a
new year of data. We only test the combinations of inputs found in the full and fast
ETL settings that are checked into the PUDL repo. Many other combinations are obviously
possible, but most of them probably don't work!

The Fast ETL
^^^^^^^^^^^^
Running the Fast ETL processes a limited subset of data for each dataset. This is
similar to what we do in our :doc:`software integration tests <testing>`. Depending on
your computer, it may take up to an hour to run.

.. code-block:: console

  $ pixi run dg launch --job pudl --config src/pudl/package_data/settings/dg_fast.yml

The Full ETL
^^^^^^^^^^^^
The Full ETL settings includes all available data that PUDL can process. All
the years, all the states, and all the tables, including the ~1 billion record
EPA CEMS dataset. Assuming you already have the data downloaded, on a computer
with at least 16 GB of RAM, and a solid-state disk, the Full ETL including EPA
CEMS should take around 2 hours.

.. code-block:: console

  $ pixi run dg launch --job pudl --config src/pudl/package_data/settings/dg_full.yml

Custom ETL
^^^^^^^^^^
If you need a custom run profile, copy one of the existing Dagster config files,
change its ``etl_settings_path`` or other resource settings, and point ``dg launch`` at
the new file.

.. code-block:: console

  $ pixi run dg launch --job pudl --config the/path/to/your/custom_dg_config.yml

Additional Notes
----------------

Logging
^^^^^^^

The commands above should result in a bunch of Python :mod:`logging` output describing
what Dagster is doing, and file outputs in the directory you specified via the
``$PUDL_OUTPUT`` environment variable. When the ETL is complete, you should see new
files at e.g. ``$PUDL_OUTPUT/ferc1_dbf.sqlite``, ``$PUDL_OUTPUT/pudl.sqlite`` and
``$PUDL_OUTPUT/core_epacems__hourly_emissions.parquet``.

The Dagster CLI also has built-in help if you want additional information:

.. code-block:: console

  $ pixi run dg launch --help

Foreign Key Constraints
^^^^^^^^^^^^^^^^^^^^^^^
The order assets are loaded into ``pudl.sqlite`` is non-deterministic because the
assets are executed in parallel so foreign key constraint violations can't be identified
in real time. However, foreign key constraints can be checked after all of the data
has been loaded into the database successfully. To check the constraints, run:

.. code-block:: console

  $ pixi run pudl_check_fks

The foreign key check is also run as part of the PUDL integration tests.
