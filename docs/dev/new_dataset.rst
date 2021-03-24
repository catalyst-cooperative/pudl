
===============================================================================
Integrating a New Dataset
===============================================================================

.. warning::

    We are in the process of re-organizing PUDL's datastore management and
    making the ETL process more object-oriented, so the documentation below may
    be a bit out of date. See these Github issues to get a sense of our
    progress:
    `#182 <https://github.com/catalyst-cooperative/pudl/issues/182>`__
    `#370 <https://github.com/catalyst-cooperative/pudl/issues/370>`__
    `#510 <https://github.com/catalyst-cooperative/pudl/issues/510>`__
    `#514 <https://github.com/catalyst-cooperative/pudl/issues/514>`__

If you're already working with US energy system data in Python, or have been
thinking about doing so, and would like to have the added benefit of access to
all the other information that's already part of PUDL, you might consider
adding a new data source. That way other people can use the data too, and we
can all share the responsibility for ensuring that the code continues to work,
and improves over time.

Right now the process for adding a new data source looks something like this:

#. Define well normalized data tables for the new data source in the
   metadata, which is stored in
   ``src/pudl/package_data/meta/datapkg/datapackage.json``.
#. Add a module to the :mod:`pudl.extract` subpackage that generates raw
   dataframes containing the new data source's information from whatever its
   original format was.
#. Add a module to the :mod:`pudl.transform` subpackage that takes those raw
   dataframes, cleans them up, and re-organizes them to match the new database
   table definitions.
#. If necessary, add a module to the :mod:`pudl.load` subpackage that takes
   these clean, transformed dataframes and exports them to data packages.
#. If appropriate, create linkages in the table schemas between the tabular
   resources so they can be used together. Often this means creating some
   skinny "glue" tables that link one set of unique entity IDs to another.
#. Update the :mod:`pudl.etl` module so that it includes your new data source
   as part of the ETL (Extract, Transform, Load) process, and any necessary
   code to the :mod:`pudl.cli` entrypoint module.
#. Add an output module for the new data source to the :mod:`pudl.output`
   subpackage.
#. Write some unit tests for the new data source, and add them to the
   ``pytest`` suite in the ``test`` directory.

-------------------------------------------------------------------------------
Add dataset to the datastore
-------------------------------------------------------------------------------

Scripts
^^^^^^^

This means editing the :mod:`pudl.workspace.datastore` module and the
``pudl_datastore`` script so that they can acquire the data from the
reporting agencies, and organize it locally in advance of the ETL process.
New data sources should be organized under ``data/<agency>/<source>/`` e.g.
``data/ferc/form1`` or ``data/eia/form923``. Larger data sources that are
available as compressed zipfiles can be left zipped to save local disk space,
since ``pandas`` can read zipfiles directly.

Organization
^^^^^^^^^^^^

The exact organization of data within the source directory may vary, but should
be as uniform as possible. For data which is compiled annually, we typically
make one subdirectory for each year, but some data sources provide all the data
in one file for all years (e.g. the MSHA mine info).

User Options
^^^^^^^^^^^^

The datastore update script can be run at the command line to pull down new
data, or to refresh old data if it’s been updated. Someone running the script
should be able to specify subsets of the data to pull or refresh -- e.g. a set
of years, or a set of states -- especially in the case of large datasets. In
some cases, opening several download connections in parallel may dramatically
reduce the time it takes to acquire the data (e.g. pulling don the EPA CEMS
dataset over FTP). The :mod:`pudl.constants` module contains several
dictionaries which define what years etc. are available for each data source.

Describe Table Metadata
^^^^^^^^^^^^^^^^^^^^^^^

Add table description into `resources` in the  the mega-data: the metadata file
that contains all of the PUDL table descriptions
(``src/pudl/package_data/meta/datapkg/datapackage.json``). The resource
descriptions must conform to the `Frictionless Data specifications <https://frictionlessdata.io/specs/>`__,
specifically the specifications for a `tabular data resource <https://frictionlessdata.io/specs/tabular-data-resource/>`__.
The `table schema specification <https://frictionlessdata.io/specs/table-schema/>`__ will be particularly helpful.

There is also a dictionary in the megadata called "autoincrement", which is
used for compiling table names that require an auto incremented id column when
exporting to a database. This is for tables with no natural primary key. The id
column is not required for the datapackages but when exporting to a database,
we will read this dictionary in the ```datapkg_to_sqlite`` script to determine
which tables need these auto increment id column. Make sure your tables are
normalized -- see Design Guidelines below.

Extract the data from its original format.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The raw inputs to the extract step should be the pointers to the datastore and
any parameters on grabbing the dataset (i.e. the working years, locational
constraints if applicable). The outcome of the extract module should be a
dictionary of dataframes with keys that correspond to the original datasource
table/tab/file name with each row corresponding to one record. These raw
dataframes should not be largely altered from their original structures in this
step, with the exception of creating records. For example, the EIA 923 often
reports a year’s worth of monthly data in one row and the extract step
transforms the single row into twelve monthly records.  If possible, attempt to
keep the dataset in its most compressed format on disk during the extract step.
For large data sources stored in zip files (e.g. epacems), there is no need to
unzip the files as pandas is able to read directly from zipped files. For
extracting data from other databases (as opposed to CSV files, spreadsheets,
etc.) you may need to populate a live database locally, and read from it (e.g.
the FERC Form 1 database, which we clone into postgres from the FoxPro/DBF
format used by FERC).

Transform the data into clean normalized dataframes.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The inputs to the transform step should be the dictionary of raw dataframes and
any dataset constraints (i.e. working years, tables, and geographical
constraints). The output should be a dictionary of transformed dataframes which
look exactly like what you want to end up in the database tables. The key of
the dictionary should be the name of the database tables as defined in the
models. Largely, there is one function per data table. If one database table
needs any information such as the index from another table (see
``fuel_receipts_costs_eia923`` and ``coalmine_eia923`` for an example), this
will require the transform functions to be called in a particular order but the
process is largely the same. All the organization of the data into normalized
tables happens in the transform step.

During this step, any cleaning of the original data is done. This includes
operations like:

* Standardizing units and unit conversions,
* Casting to appropriate data types (string, int, float, date...),
* Conversion to appropriate NA or NaN values for missing data,
* Coding of categorical variables (e.g. fuel type)
* Coding/categorization of freeform strings (e.g. fuel types in FERC Form 1)
* Correction of glaring reporting errors if possible (e.g. when someone
  reports MWh instead of kWh for net generation, or BTU instead of MMBTU)

Load the data into the datapackages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each of the dataframes that comes out of the transform step represents a
resource that needs to be loaded into the datapackage. Pandas has a native
:meth:`pandas.DataFrame.to_csv` method for exporting a dataframe to a CSV
file, which is used to output the data to disk.

Because we have not yet taken advantage the new pandas extension arrays, and
Python doesn’t have a native NA value for integers, just before the dataframes
are written to disk we convert any integer NA sentinel values using a little
helper function :func:`pudl.helpers.fix_int_na`.

Glue the new data to existing data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We refer to the links between different data sources as the "glue". The glue
The glue should be able to be thoroughly independent from the ingest of the
dataset (there should be no PUDL glue id’s in any of the datasource tables and
there should be no foreign key relationships from any of the glue tables to the
datasource specific tables). These connector keys can be added in the output
functions but having them be integral to the database ingestion would make the
glue a dependency for adding new datasources, which we want to avoid. The
process for adding glue will be very different depending on the datasets you're
trying to glue together. The EIA and FERC plants and utilities are currently
mapped by hand in a spreadsheet and pulled into tables. The FERC and EIA units
ids that will end up living in a glue table will be created through the
datazipper. There should be one module in the glue subpackage for each
inter-dataset glue (i.e. ferc1_eia or  cems_eia) as well as table definitions
in the models.glue.py module. If possible, there should be foreign key
constraints from the underlying dataset entity tables (i.e. plants_entity_eia)
to the glue tables so that we do not accidentally store glue that does not
refer to the underlying dataset.

Create an output module
^^^^^^^^^^^^^^^^^^^^^^^

The :mod:`pudl.output` subpackage compiles interesting information from the
database in tabular form for interactive use in dataframes, or for export. Each
data source should have its own module in the output subpackage, and within
that module there should be a function allowing the output of each of the core
tables in the database which come from that data source.  These tabular outputs
can and should be denormalized, and include additional information a user might
commonly want to work with -- for example including the names of plants and
utilities rather than just their IDs. In addition to those data source specific
tabular output modules, there’s also :class:`pudl.output.pudltabl.PudlTabl`, a
tabular output class. This class can be used to pull and store subsets of the
data from the database, and can also use modules within the analysis subpackage
to calculate interesting derived quantities, and provide it as a tabular
output. See the :mod:`pudl.analysis.mcoe` module as an example for how this
works.

Write some tests
^^^^^^^^^^^^^^^^

Test cases need to be created for each new dataset, verifying that the ETL
process works, and sanity checking the data itself. This is somewhat different
than traditional software testing, since we're not just testing our code --
we're also trying to make sure that the data is in good shape. Those
exhaustive tests are currently only run locally. See :ref:`testing` for more
details.
