.. _settings_files:

===============================================================================
Settings Files
===============================================================================

Several of the scripts provided as part of PUDL require more arguments than can be
easily managed on the command line. It's also useful to preserve a record of how the
data processing pipeline was run in one instance so that it can be re-run in exactly the
same way. We have these scripts read their settings from YAML files, examples of
which are included in the distribution.

There are two example files that are deployed into a users workspace with the
``pudl_setup`` script (see: :ref:`install-workspace`). The two settings files direct
PUDL to process 1 year ("fast") and all years ("full") of data respectively. Each
file contains parameters for both the ``ferc1_to_sqlite`` and the ``pudl_etl``
scripts.

-------------------------------------------------------------------------------
Setttings for ferc1_to_sqlite
-------------------------------------------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: auto

   * - Parameter
     - Description
   * - ``years``
     - A list of years to be included in the cloned FERC Form 1 database. You
       should only use a continuous range of years. 1994 is the earliest year
       available.
   * - ``tables``
     - A list of strings indicating what tables to load. The list of acceptable
       tables can be found in the the example settings file and corresponds to
       the keys of :py:const:`pudl.extract.ferc1.DBF_TABLES_FILENAMES`.

-------------------------------------------------------------------------------
Settings for pudl_etl
-------------------------------------------------------------------------------

The ``pudl_etl`` script requires a YAML settings file. In the repository we
provide two example files, which live in ``src/pudl/package_data/settings``.
Both the ``etl_fast.yml`` and ``etl_full.yml`` examples are deployed onto a
user's system in the ``settings`` directory within the PUDL workspace when the
``pudl_setup`` script is run. Once this file is in the settings directory, users
can copy it and modify it as appropriate for their own use. See
:doc:`run_the_etl` for more details

While PUDL largely keeps datasets disentangled for ETL purposes (enabling
stand-alone ETL), the EPA CEMS and EIA datasets are exceptions. EPA CEMS cannot
be loaded without having the EIA data available because it relies on IDs that
come from EIA 860. However, EPA CEMS can be loaded without EIA if you have an existing
PUDL database. Similarly, EIA Forms 860 and 923 are very tightly related.
You can load only EIA 860, but the settings verification will automatically add
in a few 923 tables that are needed to generate the complete list of plants and
generators. The settings verification will also automatically add all 860 tables
if only 923 is specified.

.. warning::

    If you are processing the EIA 860/923 data, we **strongly recommend**
    including the same years in both datasets. We only test two combinations of
    inputs, as specified by the ``etl_fast.yml`` and ``etl_full.yml`` settings
    distributed with the package.  Other combinations of years may yield
    unexpected results.

Structure of the pudl_etl Settings File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The general structure of the settings file and the names of the keys of the
dictionaries should not be changed, but the values of those dictionaries
can be edited. The elements and structure of the ETL settings
are described below::

    name : unique name identifying the etl outputs
    title : short human readable title for the etl outputs
    description : a longer description of the etl outputs
    datasets
      ├── dataset name
      │    ├── dataset etl parameter (e.g. states) : list of states
      │    └── dataset etl parameter (e.g. years) : list of years
      └── dataset name
      │    ├── dataset etl parameter (e.g. states) : list of states
      │    └── dataset etl parameter (e.g. years) : list of years

The dataset names must not be changed. The dataset names enabled include:
``eia`` (which includes Forms 860/923 only for now), ``ferc1``, and ``epacems``.
Any other dataset name will result in an validation error.

.. note::

    We strongly recommend leaving the arguments that specify which database
    tables are generated unchanged -- i.e. always include all of the tables;
    many analyses require data from multiple tables, and removing a few
    tables doesn't change how long the ETL process takes by much.

Dataset ETL parameters (like years, states, tables) will only register if they
are a part of the correct dataset. If you put some FERC Form 1 ETL parameter in
an EIA dataset specification, FERC Form 1 will not be loaded as a part of that
dataset. For an exhaustive listing of the available parameters, see the
``etl_full.yml`` file.
