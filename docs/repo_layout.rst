Developers Guide
=======================

The PUDL repository is organized generally around the recommendations from `Good enough practices in scientific computing <https://doi.org/10.1371/journal.pcbi.1005510>`_\ )

data/
^^^^^

A data store containing the original data from FERC, EIA, EPA and other agencies. It's organized first by agency, then by form, and (in most cases) finally year. For example, the FERC Form 1 data from 2014 would be found in ``./data/ferc/form1/f1_2014`` and the EIA data from 2010 can be found in ``./data/eia/form923/f923_2010``. The year-by-year directory structure is determined by the reporting agency, based on what is provided for download.

The data itself is too large to be conveniently stored within the git repository, so we use `a datastore management script </scripts/update_datastore.py>`_ that can pull down the most recent version of all the data that's needed to build the PUDL database, and organize it so that the software knows where to find it. Run ``python ./scripts/update_datastore.py --help`` for more info.

docs/
^^^^^

Documentation related to the data sources, our results, and how to go about
getting the PUDL database up and running on your machine. We try to keep these
in text or Jupyter notebook form. Other files that help explain the data
sources are also stored under here, in a hierarchy similar to the data store.
E.g. a blank copy of the FERC Form 1 is available in ``./docs/ferc/form1/`` as a
PDF.

pudl/
^^^^^

The PUDL python package, where all of our actual code ends up. The modules and packages are organized by data source, as well as by what step of the database initialization process (extract, transform, load) they pertain to. For example:


* `\ ``./pudl/extract/eia923.py`` </pudl/extract/eia923.py>`_
* `\ ``./pudl/transform/ferc1.py`` </pudl/transform/ferc1.py>`_

The load step is currently very simple, and so it just has a single top level module dedicated to it.

The database models (table definitions) are also organized by data source, and are kept in the models subpackage. E.g.:


* `\ ``./pudl/models/eia923.py`` </pudl/models/eia923.py>`_
* `\ ``./pudl/models/eia860.py`` </pudl/models/eia860.py>`_

We are beginning to accumulate analytical functionality in the `analysis subpackage </pudl/analysis/>`_\ , like calculation of the marginal cost of electricity (MCOE) on a per generator basis. The `output subpackage </pudl/output/>`_ contains data source specific output routines and an `output class definition </pudl/output/pudltabl.py>`_.

Other miscellaneous bits:
^^^^^^^^^^^^^^^^^^^^^^^^^


*
  `\ ``./pudl/constants.py`` </pudl/constants.py>`_ stores a variety of static
  data for loading, like the mapping of FERC Form 1 line numbers to FERC
  account numbers in the plant in service table, or the mapping of EIA923
  spreadsheet columns to database column names over the years, or the list of
  codes describing fuel types in EIA923.

*
  `\ ``./pudl/helpers.py`` </pudl/helpers.py>`_ contains a collection of
  helper functions that are used throughout the project.

results/
^^^^^^^^

The results directory contains derived data products. These are outputs from our manipulation and combination of the original data, that are necessary for the integration of those data sets into the central database. It also contains outputs we've generated for others.

The results directory also contains `a collection of Jupyter notebooks </results/notebooks>`_ (which desperately needs organizing) presenting various data processing or analysis tasks, such as pulling the required IDs from the cloned FERC database to use in matching up plants and utilities between FERC and EIA datasets.

scripts/
^^^^^^^^

A collection of command line tools written in Python and used for high level
management of the PUDL database system, e.g. the initial download of and
ongoing updates to the datastore, and the initialization of your local copy of
the PUDL database.  These scripts are generally meant to be run from within the
``./scripts`` directory, and should all have built-in Documentation as to their
usage. Run ``python script_name.py --help`` to for more information.

test/
^^^^^

The test directory holds test cases that we use to ensure that ``pudl`` is in good working order before we make commits. The tests are run with ``pytest``.  For more information on how to run all of the tests, check read through the `test_pudl.sh </scripts/test_pudl.sh>`_ script. To test pretty much everything looks like:

.. code-block:: sh

   # Make sure that the datastore management routines work:
   pytest test/datastore_test.py

   # Then actually update your datastore, if need be.
   # This may take hours if you pull down everything over a slow connection:
   python scripts/update_datastore.py --sources eia860 eia923 ferc1 epacems

   # Do a small subset of the ETL to ensure nothing is horrible b0rken:
   pytest test/travis_ci_test.py

   # Do a full test of the ETL process:
   pytest test/etl_test.py

   # If that worked, actually run the ETL process
   # First clone the FERC 1 database:
   python scripts/ferc1_to_sqlite.py scripts/settings_ferc1_to_sqlite_full.yml

   # Now run the PUDL DB initialization script:
   python scripts/init_pudl.py scripts/settings_init_pudl_custom.yml

   # Do some basic validation of the data now in the database:
   pytest --live_pudl_db test/validation

   # Validate the testing and documentation Jupyter Notebooks:
   pytest --nbval-lax test/notebooks docs/notebooks

More information on PyTest can be found at: http://docs.pytest.org/en/latest/
