.. _settings_files:

===============================================================================
Settings Files
===============================================================================

Several of the scripts provided as part of PUDL require more arguments than can
be easily managed on the command line, and it's useful to preserve a record of
how the data processing pipeline was run, so they read their settings from YAML
files, examples of which are included in the distribution.

-------------------------------------------------------------------------------
ferc1_to_sqlite
-------------------------------------------------------------------------------

=========================== ===================================================
Parameter                   Description
=========================== ===================================================
``ferc1_to_sqlite_refyear`` A single 4-digit year to use as the reference for
                            inferring FERC Form 1 database's structure.
``ferc1_to_sqlite_years``   A list of years to be included in the cloned FERC
                            Form 1 database. These years must be present in the
                            datastore, and available from FERC (1994 onward).
``ferc1_to_sqlite_tables``  A list of strings indicating what tables to load.
                            The list of acceptable tables can be found in the
                            the example settings file, and corresponds to the
                            values found in the ``ferc1_dbf2tbl`` dictionary
                            in :mod:`pudl.constants`.
=========================== ===================================================

-------------------------------------------------------------------------------
pudl_etl
-------------------------------------------------------------------------------

The ``pudl_etl`` script requires a YAML settings file. In the repository this
example file is lives in ``src/pudl/package_data/settings``. This example file
(``etl_example.yml``) is deployed onto a user's system in the
``settings`` directory within the PUDL workspace when the ``pudl_setup`` script
is run. Once this file is in the settings directory, users can copy it and
modify it as appropriate for their own use.

This settings file allows users to determine the scope of the  integrated by
PUDL. Most datasets can be used to generate stand-alone data packages. If you
only want to use FERC Form 1, you can remove the other data package
specifications, or alter their parameters such that none of their data is
processed (e.g. by setting the list of years to be an empty list). The settings
are verified early on in the ETL process so if you got something wrong, you
should get an assertion error quickly.

While PUDL largely keeps datasets disentangled for ETL purposes (enabling
stand-alone ETL) the EPA CEMS and EIA datasets are exceptions. EPA CEMS cannot
be loaded without EIA because it relies on IDs that come from EIA 860.
Similarly, EIA Forms 860 and 923 are very tightly related. You can load only
EIA 860, but the settings verification will automatically add in a few 923
tables that are needed to generate the complete list of plants and generators.

.. note::

    **If you are processing the EIA 860/923 data, we strongly recommend
    including the same years in both datasets.** Furthermore only test that
    either **all** the years can be processed together, and that the most
    recent year can be processed alone. Other combinations of years may yield
    unexpected results.

Structure of the ETL Settings File
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The general structure of the settings file and the names of the keys of the
dictionaries should not be changed, but the values of those dictionaries
can be edited. There are two high-level elements of the settings file which
pertain to the entire bundle of tabular data packages which will be generated:
``datapkg_bundle_name`` and ``datapkg_bundle_settings``. The
``datapkg_bundle_name`` determines which directory the data packages are
written into. The elements and structure of the ``datapkg_bundle_settings``
are described below::

    datapkg_bundle_settings
      ├── name : unique name identifying the data package
      │   title : short human readable title for the data package
      │   description : a longer description of the data package
      │   datasets
      │    ├── dataset name
      │    │    ├── dataset etl parameter (e.g. states) : list of states
      │    │    └── dataset etl parameter (e.g. years) : list of years
      │    └── dataset name
      │    │    ├── dataset etl parameter (e.g. states) : list of states
      │    │    └── dataset etl parameter (e.g. years) : list of years
      └── another data package...

The dataset names must not be changed. The dataset names enabled include:
``eia`` (which includes Forms 860/923 only for now), ``ferc1``, ``epacems``,
and ``epaipm``. Any other dataset name will result in an assertion error.

.. note::

    We strongly recommend leaving the arguments that specify which database
    tables are generated unchanged -- i.e. always include all of the tables,
    as many analyses require data from multiple tables, and removing a few
    tables doesn't change how long the ETL process takes by much.

Dataset ETL parameters (like years, states, tables), will only register if they
are a part of the correct dataset. If you put some FERC Form 1 ETL parameter in
an EIA dataset specification, FERC Form 1 will not be loaded as a part of that
dataset. For an exhaustive listing of the available parameters, see the
``etl_example.yml`` file.
