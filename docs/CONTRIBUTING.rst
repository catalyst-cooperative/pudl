Contributing to PUDL
====================

PUDL is an open source project that has been supported by a combination of
:doc:`volunteer contributors and grant funding <acknowledgments>`. The work is
currently being coordinated by the members of `Catalyst Cooperative
<https://catalyst.coop>`_. PUDL is meant to serve a wide variety of public
interests including academic research, climate advocacy, data journalism, and
public policy making.

For more on the origins and philosophy of the project, have a look at
:doc:`this background info <background>`.

Code of Conduct
^^^^^^^^^^^^^^^

We want to make the PUDL project welcoming to contributors with different
levels of experience and diverse personal backgrounds. If you're considering
contributing please read our :doc:`Code of Conduct <CODE_OF_CONDUCT>`, which
is based on the `Contributor Covenant
<https://www.contributor-covenant.org/>`_.

Development Setup
^^^^^^^^^^^^^^^^^

If you want to contribute code or documentation, you'll need to create your own
fork of the project on Github, set up the :doc:`build and testing
<testing>` environment on your local system, and make pull requests.
If you're new to git and Github, you may want to check out `this article on
collaborative development models
<https://help.github.com/en/articles/about-collaborative-development-models>`_
and `this one on the Github workflow
<https://guides.github.com/introduction/flow/>`_

In brief, the setup process ought to look something like this...

.. todo::

    Update to reflect the exact details of a real setup.

.. code-block:: console

    $ git clone git@github.com:catalyst-cooperative/pudl.git pudl
    $ cd pudl
    $ conda env create --name pudl --file environment.yml
    $ pip install -e ./
    $ pudl_setup --pudl_dir=../pudl_workspace
    $ tox -e etl -- --fast --pudl_in=AUTO --pudl_out=AUTO


Clone the PUDL Repository
--------------------------

PUDL has not yet been released via one of the common Python package systems, so you have to download the code directly from GitHub by `cloning the repository <https://help.github.com/articles/cloning-a-repository/>`_ to your own computer. Depending on your platform (Linux, OSX, Windows...) and the way you access GitHub, the exact process will differe. If you don’t have a GitHub account, you’ll probably want to create one at `github.com <https://github.com>`_. PUDL is a public, open source repository, so a free account is all you need.

If you're using a UNIX-like terminal, the command will look like this:

.. code-block:: console

   $ git clone https://github.com/catalyst-cooperative/pudl.git


Create a PUDL conda environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

From the top level of your cloned PUDL repository, in a terminal window type:

.. code-block:: console

   $ conda env create --name=pudl --file=environment.yml

If you get an error ``No such file or directory: environment.yml``\ , make sure
you're in the ``pudl`` repository downloaded in step 2. See the ``conda``
documentation for more on
`managing environments <https://conda.io/docs/user-guide/tasks/manage-environments.html>`__.

Activate the PUDL environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

From within a UNIX-like shell, use the command:

.. code-block:: console

   $ conda activate pudl

Now you should probably see ``(pudl)`` to the left of your command line prompt, indicating that the environment is active.

Install the PUDL package for development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above commands installed the packages that ``pudl`` depends on, but not ``pudl`` itself. Until we've released the package to PyPI, you need to install it manually from your clone of the repository. This will allow you to use the PUDL library as if it were a normal package installed from the Python Package Index. Make sure you're in the top level directory of the repository, and run:

.. code-block:: sh

   pip install --editable ./

The ``--editable`` option keeps ``pip`` from copying files into to the
``site-packages`` directory, and just creates references directly to the
current directory (aka ``./``\ ).

Project Management
^^^^^^^^^^^^^^^^^^

The people working on PUDL are distributed all over North America.
Collaboration takes place online. We make extensive use of Github's project
management tools.

Issues and Project Tracking
----------------------------

We use `Github issues <https://github.com/catalyst-cooperative/pudl/issues>`_
to track bugs, enhancements, support requests, and just about any other work
that goes into the project. The issues are organized into several different
streams of work, using `Github projects
<https://github.com/catalyst-cooperative/pudl/projects>`_

We are happy to accept pull requests that improve our existing code, expand the
data that's available via PUDL, and and make our documentation more readable
and complete. Feel free to report bugs, comment on existing issues, suggest
other data sources that might be worth integrating, or ask questions about how
to use PUDL if you can't find the answer in our documentation.

Release Management
------------------

We are developing and releasing software, but we're also using that software to
process and publish data. Our goal is to make the data pipeline as easily and
reliably replicable as possible.

Whenever we tag a release on Github, the repository is archived on `Zenodo
<https://zenodo.org>`_ and issued a DOI. Then the package is uploaded to the
Python Package Index for distribution. Our goal is to make a software release
at least once a quarter.

Data releases will also be archived on Zenodo, and consist of a software
release, a collection of input files, and the resulting data packages. The goal
is to make the data package output reproducible given the archived input files
and software release, with a single command. Our goal is to make data releases
quarterly as well.

User Support
------------

We don't (yet) have funding to do user support, so it's currently all community
and volunteer based. In order to ensure that others can find the answers to
questions that have already been asked, we try to do all support in public
using Github issues.

Coding Guidelines
^^^^^^^^^^^^^^^^^

* Pure Python 3.7+ that will work on Linux, Mac OS, or Windows
* Ensure the full tests pass locally
* Don't decrease test coverage
* All the code has to be open source!
* Use Flake8 for linting / formatting.
* Write good docstrings, using Google docstring format.

.. todo::

    Flesh out the code style / quality guidelines based on examples from other
    projects.

Data Integration Guidelines
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* To the extent possible, it should be possible to process each data source
  independent of the others (e.g. FERC Form 1 can be processed without EIA
  923).
* In some cases this isn't practical because the data is so intertwined (e.g.
  EIA 860 and EIA 923).
* If the data is a time series, it should be possible to load any continuous
  subset of the time series, at it's natural frequency (e.g. EIA 923 is annual
  so it should be possible to load the data for 2017, or 2015-2017, or
  2009-2012, but not necessarily the individual years 2009, 2012, and 2017 or
  the last half of 2016 and the first half of 2017).
* Relationships that connect distinct data sets (which we call "glue") should
  be an optional part of the relational structure.
* Data types must be compatible with the Frictionless Data `Table Schema
  specification <https://frictionlessdata.io/specs/table-schema/>`_
* Data should be minimally altered from what is reported, for instance to
  standardize units, data types, and ``NA`` values, to create linkages between
  and within the datasets which have been integrated, to create well normalized
  database tables with minimal duplication of data, and in some cases to
  categorize free-form strings into well defined taxonomies of categorical
  values.


Original vs. Derived Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Much more complex and interesting analysis can be performed on top of this
underlying archive of structured data, but at this point it not our intention
to store and distribute the results of such analyses, or to integrate them into
the ETL process. Instead, we provide analytical routines that take the
processed data as inputs. (e.g. calculating the marginal cost of electricity at
the generator level).


Adding a New Data Source
------------------------

If you're already working with US energy system data in Python, or have been
thinking about doing so, and would like to have the added benefit of access to
all the other information that's already part of PUDL, you might consider
adding a new data source. That way other people can use the data too, and we
can all share the responsibility for ensuring that the code continues to work,
and improves over time.

In general the process for adding a new data source looks like this:


#. Add the new data source to the ``datastore.py`` module and the ``update_datastore.py`` script.
#. Define well normalized database tables for the new data source in the ``models`` subpackage.
#. Add a module to the ``extract`` subpackage that generates raw dataframes containing the new data source's information from whatever its original format was.
#. Add a module to the ``transform`` subpackage that takes those raw dataframes, cleans them up, and re-organizes them to match the new database table definitions.
#. If necessary add a module to the ``load`` subpackage that takes these clean, transformed dataframes and pushes their contents into the postgres database.
#. Create linkages between the new database tables and other existing data in the database, so they can be used together, if appropriate. Often this means creating some skinny "glue" tables that link one set of unique entity IDs to another.
#. Update the ``init.py`` module so that it includes your new data source as part of the ETL (Extract, Transform, Load) process, and add the necessary code to the ``init_pudl.py`` script.
#. Add an output module for the new data source to the ``output`` subpackage.
#. Write some unit tests for the new data source, and add them to the ``pytest`` suite in the ``test`` directory.

Add the new data source to the datastore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Scripts
~~~~~~~

This means editing the ``datastore.py`` module and the datastore update script
(\ ``scripts/update_datastore.py``\ ) so that they can acquire the data from the
reporting agencies, and organize it locally in advance of the ETL (Extract,
Transform, and Load) process. New data sources should be organized under
``data/<agency>/<source>/`` e.g. ``data/ferc/form1`` or ``data/eia/form923``.
Larger data sources that are available as compressed zipfiles can be left
zipped to save local disk space, since ``pandas`` can read zipfiles directly.

Organization
~~~~~~~~~~~~

The exact organization of data within the source directory may vary, but should
be as uniform as possible. For data which is compiled annually, we typically
make one subdirectory for each year, but some data sources provide all the data
in one file for all years (e.g. the MSHA mine info).

User Options
~~~~~~~~~~~~

The datastore update script can be run at the command line to pull down new
data, or to refresh old data if it’s been updated. Someone running the script
should be able to specify subsets of the data to pull or refresh -- e.g. a set
of years, or a set of states -- especially in the case of large datasets. In
some cases, opening several download connections in parallel may dramatically
reduce the time it takes to acquire the data (e.g. pulling don the EPA CEMS
dataset over FTP). The ``constants.py`` module contains several dictionaries
which define what years etc. are available for each data source.

Define the database tables.
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a new module in the models subpackage. The name of the module should be
the datasource name. New modules need to import the entities module (\ ``import
pudl.models.entities``\ ) and must be imported in the init.py module (\ ``import
pudl.models.datasource``\ ). This way the database schema will include this module
when it is first initialized. PUDL uses SQLAlchemy. Each datatable must have a
class definition (see the ``PlantAnnualEIA`` class in ``pudl.models.eia`` for a
simple example). Make sure your tables are normalized -- see Design Guidelines
below.

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
the dictionary should be the name of the database tables as defined in the models.
Largely, there is one function per data table. If one database table needs any
information such as the index from another table (see
``fuel_receipts_costs_eia923`` and ``coalmine_eia923`` for an example), this will
require the transform functions to be called in a particular order but the
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

Load the data into the database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each of the dataframes that comes out of the transform step represents a
database table that needs to be loaded into the database. Pandas has a native
``DataFrame.to_sql()`` method for inserting records from a dataframe into a
atabase , but it’s extremely slow. Instead, we use postgres’ native ``COPY_FROM``
function, which is designed for loading large CSV files directly into the
database very efficiently. Instead of writing the dataframe out to a file on
disk, we create an in-memory file-like object, and read directly from that. For
this to work, the corresponding dataframe and database columns need to be named
identically, and the strings that are read by postgres from the in-memory CSV
file need to be readily interpretable as the data type that is associated with
the column in the table definition. Because Python doesn’t have a native NA
value for integers, but postgres does, just before the dataframes are loaded
into the database we convert any integer NA sentinel values using a little
helper function named ``fix_int_na()``.

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

The ``pudl.output`` subpackage compiles interesting information from the database
in tabular form for interactive use in dataframes, or for export. Each data
source should have its own module in the output subpackage, and within that
module there should be a function allowing the output of each of the core
tables in the database which come from that data source.  These tabular outputs
can and should be denormalized, and include additional information a user might
commonly want to work with -- for example including the names of plants and
utilities rather than just their IDs. In addition to those data source specific
tabular output modules, there’s also a tabular output class PudlTabl defined in
pudltabl.py. This class can be used to pull and store subsets of the data from
the database, and can also use modules within the analysis subpackage to
calculate interesting derived quantities, and provide it as a tabular output.
See the analysis.mcoe module as an example for how this works.

Write some tests
^^^^^^^^^^^^^^^^

Test cases need to be created for each new dataset, verifying that the ETL
process works, and sanity checking the data itself. This is somewhat different
than traditional software testing, since we're not just testing our code --
we're also trying to make sure that the data is in good shape. Those
exhaustive tests are currently only run locally. Less extensive tests that are
meant to just check that the code is still working correctly need to be
integrated into the ``test/travis_ci_test.py`` module, which downloads a small
sample of each dataset for use in testing.

----

Design Guidelines
-----------------

Some technical norms and expectations that we strive to adhere to, and hope
that contributors can also follow. Some of these are aspirational -- we still
need to go back and apply better naming conventions to much of the data we've
brought in, and the way dates and times are dealt with still needs some work.
Also, we're all learning as we go here -- if you have a suggestion for making
these guidelines better, let us know!

Use well normalized database tables.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This means keeping as little duplicate or derived information as is practical.
For example, we wouldn't compute heat rates for generators in a table that
contains both fuel heat content and net electricity generation, since the heat
rate would be a derived value. Similarly, we wouldn't apply an inflation
adjustment to reported financial values in the database, since there are a
variety of possible inflation indices users might want to use -- that kind of
transformation would be applied in the output layer that sits on top of the
database.

Use consistent units across data sources.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Different data sources often
use different units to describe the same type of quantities. Rather than force
users to do endless conversions while using the data, we try to convert similar
quantities into the same units during ETL. For example, we typically convert
all electrical generation to MWh, plant capacities to MW, and heat content to
MMBTUs.

Use rich data types.
^^^^^^^^^^^^^^^^^^^^

Postgres provides a huge collection of different native data types. In database
table definitions, we try to use the data types which most accurately represent
the data we are storing. This has often come up in the context of time --
rather than storing just the report year or month as a couple of integers, or a
timestamp representing the beginning of the reporting period, we’d prefer store
an interval that explicitly describes the reporting period.

Compartmentalize the ETL process for each data source.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To the extent possible, it should be possible to bring in each data source
independent of all the others, so that users can pick and choose which data is
pulled in, based on what they need access to locally. In some cases like the
EIA 860 and EIA 923 data, two data sources may be so intertwined that keeping
them separate doesn't relaly make sense, but that should be the exception.

Separate integration between data sources from the data itself.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To the extent possible, the glue that relates the data sources to each other
should be applied after or alongside the ETL process, and not as a mandatory
part of ETL. This makes it easy to pull individual data sources in and work
with them even when the glue isn't working… which happens sometimes, especially
at the beginning of the integration process.

Keep large data sets manageable for smaller users.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For now, our goal is that users should be able to initialize the PUDL database
on a decent laptop. However, some of the utility datasets are hundreds of
gigabytes in size (EPA CEMS, FERC EQR, ISO pricing). Many users will not need
to use the entire dataset for the work they are doing. Allow them to pull in
only certain years, or certain states, or other sensible partitions of the data
if need be, so that they don’t run out of memory or disk space, or have to wait
hours for a bunch of data they don't need to get pulled in.

Give comparable quantities the same names in different tables.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If two columns in different tables record the same quantity in the same units,
give them the same name -- if they end up in the same dataframe for comparison
it's easy to automatically rename them with suffixes indicating where they came
from. For example net electricity generation is reported to both FERC and EIA,
so we've named columns net_generation_mwh in each of those data sources.
Similarly, give non-comparable quantities reported in different data sources
different column names. This helps make it clear that the quantities are
actually different.

Minimize the use of raw SQL.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We’re trying to stick to using SQL Alchemy’s abstractions  for defining tables,
relationships between tables, constraints, and queries. We find it more
readable generally, and hopefully it will also help keep the project somewhat
more independent of the underlying database, which may be helpful if and when
we need to be able to host the data both on local machines and a platform like
AWS.

Follow `naming conventions <naming_conventions.md>`_.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We are trying to use consistent naming conventions for database tables,
columns, data sources, and functions. Generally speaking PUDL is a collection
of subpackages organized by purpose (extract, transform, load, analysis,
models, output, datastore…), containing a module for each data source. Each
data source has a short name that is used everywhere throughout the project,
composed of the reporting agency and the form number or another identifying
abbreviation: ferc1, epacems, eia923, mshamines, etc.

Have suggestions?
^^^^^^^^^^^^^^^^^^^^^
If there are other valuable public datasets related to the US energy system that aren't already easily accessible for use in programmatic analysis, you can `create a a new issue <https://github.com/catalyst-cooperative/pudl/issues/new/choose>`_ describing the data, explaining why it's interesting, and linking to it. Tag it `new data <https://github.com/catalyst-cooperative/pudl/issues?q=is%3Aissue+is%3Aopen+label%3A%22new+data%22>`_.

.. todo::

    Integrate contact info and Github issue templates into contributing docs.

If you've made it this far, congratulations! Hopefully you've now got gigabytes
of US energy system data at your fingertips! We would love to get your
suggestions and feedback. For instance you could...

* Check out our `Code of Conduct </docs/CODE_OF_CONDUCT.md>`_
* File a `bug report <https://github.com/catalyst-cooperative/pudl/issues/new?template=bug_report.md>`_ if you find something that's broken.
* Make a `feature request <https://github.com/catalyst-cooperative/pudl/issues/new?template=feature_request.md>`_ if you think there's something we should add.
* Email us at `pudl@catalyst.coop <mailto:pudl@catalyst.coop>`_
* Chat with us on `Gitter <https://gitter.im/catalyst-cooperative/pudl>`_.
* Ask for an invite to `our Slack <https://catalystcooperative.slack.com/>`_.
* Sign up for our (irregular, infrequently published) `e-mail newsletter <https://catalyst.coop/updates/>`_.
* Follow `@CatalystCoop on Twitter <https://twitter.com/CatalystCoop>`_



.. toctree::
    :hidden:

    testing
    naming_conventions
    repo_layout
    CODE_OF_CONDUCT
    LICENSE
