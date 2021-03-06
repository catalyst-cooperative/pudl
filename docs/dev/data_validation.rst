===============================================================================
Data Validation
===============================================================================
The data validation tests are organized into datasource specific modules under
``test/validate``. They test the quality and internal consistency of the data
that is output by the PUDL ETL pipeline. Currently they only work on the full
dataset, and do not have a ``--fast`` option. While it is possible to run the
full ETL process and output it in a temporary directory, to then be used by the
data validation tests, that takes a long time, and you don't get to keep the
processed data afterward. Typically we validate outputs that we're hoping to
keep around, so we advise running the data validation on a pre-generated PUDL
SQLite database.

To point the tests at already processed data, use the ``--live_pudl_db`` and
``--live_ferc1_db`` options. The ``--pudl_in`` and ``--pudl_out`` options work
the same as above. E.g.

.. code-block:: console

    $ pytest --live_pudl_db=AUTO --live_ferc1_db=AUTO \
        --pudl_in=AUTO --pudl_out=AUTO test/validate

Data Validation Notebooks
^^^^^^^^^^^^^^^^^^^^^^^^^
We maintain and test a collection of Jupyter Notebooks that use the same
functions as the data validation tests and also produce some visualizations of
the data to make it easier to understand what's wrong when validation fails.
These notebooks are stored in ``test/notebooks`` and they can be validated
with:

.. code-block:: console

    $ pytest --nbval-lax test/notebooks

The notebooks will only run successfully when there's a full PUDL SQLite
database available in your PUDL workspace.

If the data validation tests are failing for some reason, you may want to
launch those notebooks in Jupyter to get a better sense of what's gong on. They
are integrated into the test suite to ensure that they remain functional as the
project evolves.

For the moment, the data validation cases themselves are stored in the
:mod:`pudl.validate` module, but we intend to separate them from the code and
store them in a more compact, programmatically readable format.
