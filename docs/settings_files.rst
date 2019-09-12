===============================================================================
Settings Files
===============================================================================

-------------------------------------------------------------------------------
ETL Settings
-------------------------------------------------------------------------------

The ```pudl_etl`` script requires a yml settings file. An example file is
included in ``pudl/package_data/settings``. This example file
(``etl_example.yml``) is deployed onto a users system in ``pudl_out`` in
the ``settings`` directory when the ``pudl_setup`` script is run. Once this
file is in the settings directory, users can modify this file, or copy it and
have multiple versions of settings files to use with ``pudl_etl`` for different
scenarios.

This settings file is meant to be edited to enable users to set the scope of
data that they would like to use in PUDL. Most datasets can be stand-alone data
packages. If you only want to use FERC Form 1, you can remove the other data
package descriptors, or alter their parameters such that no data would be
loaded. The settings are verified early on in the ETL process so if you got
something wrong, you should get an assertion error quickly.

While PUDL largely keeps datasets disentangled for ETL purposes (enabling
stand-alone ETL) the EPA CEMS and EIA datasets are exceptions. EPA CEMS cannot
be loaded without EIA - it relies on id's in EIA 860. EIA 860 and 923 are very
tightly interdependent. You can specify to load only EIA 860, but the settings
verification will add back in a few 923 tables that are needed for 860.

The settings verification also removes empty datasets and data packages - the
data packages described in the settings file that do not include any years or
states, which would generate an empty data package.

Structure of the ETL Settings File
----------------------------------

The general structure of the settings file and the names of the keys of the
dictionaries should not be changed, but the values of those dictionaries
should be edited. There are two high-level elements of the settings file:
``pkg_bundle_name`` and ``pkg_bundle_settings``. The ``pkg_bundle_name`` will
be the directory that the bundle of packages described in the settings file.
The elements and structure of the ``pkg_bundle_settings`` is described below:

pkg_bundle_settings
  ├──── name : name of data package
  │     title : short title of data package
  │     description : longer description of data package
  │     datasets
  │     ├── dataset name
  │     │   ├── dataset etl parameter like states : list of states
  │     │   └── dataset etl parameter like years : list of years
  │     └── dataset name
  │         ├── dataset etl parameter like states : list of states
  │         └── dataset etl parameter like years : list of years
  └── another data package...

The dataset name's must not be changed. The dataset names enabled include: eia,
ferc1, epacems, glue and epaipm. Any other dataset name will raise assertion
errors.

Dataset ETL parameters (like years, states, tables), will only register if they
are a part of its dataset. If you put some FERC etl parameter in an EIA dataset
dictionary, FERC will not be loaded as a part of that dataset.
