# Contributing to PUDL

**Welcome electricity data nerds!**

We want to make the PUDL project welcoming to contributors with different
levels of experience and diverse personal backgrounds. If you're considering
contributing please read our [Code of Conduct](CODE_OF_CONDUCT.md), which is
based on the [Contributor Covenant](https://www.contributor-covenant.org/).

### Where to Start?
* **Issues:** We coordinate PUDL project tasks using [GitHub's issue tracker](https://github.com/catalyst-cooperative/pudl/issues). Take a look and see if there's anything that looks interesting. Feel free to report bugs or comment on existing issues there.
* **Slack:** If you're actively using PUDL or are interested in contributing, we'd also be happy to invite you to our [join our Slack](https://catalystcooperative.slack.com/).
* **Email:** If you just want occasional updates about the project, you can [join our email list](https://catalyst.coop/updates/).
* **Gitter:** We've also set up a [Gitter channel](https://gitter.im/catalyst-cooperative/pudl), if you want to chat, though it hasn't yet gotten a lot of traffic.

### Platform Independent, Python 3
We're trying to keep as much of the code as possible written in pure **Python
3**, with the intent that the whole system should work on both Unix-like (Linux
& Mac OS X) and Windows platforms.

---
## The Nature of the PUDL Database
Our intention is for the PUDL database to provide programmatic access to public
data reported to various US public agencies, which is not otherwise easily
accessible. The data stored in the database itself is minimally altered from
what is reported in order to standardize units, data types, and `NA` values, to
create linkages between and within the datasets which have been integrated, to
create well normalized database tables with minimal duplication of data, and in
some cases to categorize free-form strings into well defined taxonomies of
categorical values.

### Original vs. Derived Values
More complicated derived values and analysis can then be performed on top of
this underlying archive of structured data, but it not our intention to store
that kind of information within the database. Instead, we provide an output
layer that can pull easy to use tabular summaries of the database tables for
export or interactive analysis, as well as compiling the results of common
analyses (e.g. calculating the marginal cost of electricity at the generator
level).

---
## Adding a New Data Source
If you're already working with US energy system data in Python, or have been
thinking about doing so, and would like to have the added benefit of access to
all the other information that's already part of PUDL, you might consider
adding a new data source. That way other people can use the data too, and we
can all share the responsibility for ensuring that the code continues to work,
and improves over time.

In general the process for adding a new data source looks like this:

 1. Add the new data source to the `datastore.py` module and the `update_datastore.py` script.
 2. Define well normalized database tables for the new data source in the `models` subpackage.
 3. Add a module to the `extract` subpackage that generates raw dataframes containing the new data source's information from whatever its original format was.
 4. Add a module to the `transform` subpackage that takes those raw dataframes, cleans them up, and re-organizes them to match the new database table definitions.
 5. If necessary add a module to the `load` subpackage that takes these clean, transformed dataframes and pushes their contents into the postgres database.
 6. Create linkages between the new database tables and other existing data in the database, so they can be used together, if appropriate. Often this means creating some skinny "glue" tables that link one set of unique entity IDs to another.
 7. Update the `init.py` module so that it includes your new data source as part of the ETL (Extract, Transform, Load) process, and add the necessary code to the `init_pudl.py` script.
 8. Add an output module for the new data source to the `output` subpackage.
 9. Write some unit tests for the new data source, and add them to the `pytest` suite in the `test` directory.

### Add the new data source to the datastore
#### Scripts
This means editing the `datastore.py` module and the datastore update script
(`scripts/update_datastore.py`) so that they can acquire the data from the
reporting agencies, and organize it locally in advance of the ETL (Extract,
Transform, and Load) process. New data sources should be organized under
`data/<agency>/<source>/` e.g. `data/ferc/form1` or `data/eia/form923`.
Larger data sources that are available as compressed zipfiles can be left
zipped to save local disk space, since `pandas` can read zipfiles directly.

#### Organization
The exact organization of data within the source directory may vary, but should
be as uniform as possible. For data which is compiled annually, we typically
make one subdirectory for each year, but some data sources provide all the data
in one file for all years (e.g. the MSHA mine info).

#### User Options
The datastore update script can be run at the command line to pull down new
data, or to refresh old data if it’s been updated. Someone running the script
should be able to specify subsets of the data to pull or refresh -- e.g. a set
of years, or a set of states -- especially in the case of large datasets. In
some cases, opening several download connections in parallel may dramatically
reduce the time it takes to acquire the data (e.g. pulling don the EPA CEMS
dataset over FTP). The `constants.py` module contains several dictionaries
which define what years etc. are available for each data source.

### Define the database tables.
Create a new module in the models subpackage. The name of the module should be
the datasource name. New modules need to import the entities module (`import
pudl.models.entities`) and must be imported in the init.py module (`import
pudl.models.datasource`). This way the database schema will include this module
when it is first initialized. PUDL uses SQLAlchemy. Each datatable must have a
class definition (see the `PlantAnnualEIA` class in `pudl.models.eia` for a
simple example). Make sure your tables are normalized -- see Design Guidelines
below.

### Extract the data from its original format.
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

### Transform the data into clean normalized dataframes.
The inputs to the transform step should be the dictionary of raw dataframes and
any dataset constraints (i.e. working years, tables, and geographical
constraints). The output should be a dictionary of transformed dataframes which
look exactly like what you want to end up in the database tables. The key of
the dictionary should be the name of the database tables as defined in the models.
Largely, there is one function per data table. If one database table needs any
information such as the index from another table (see
`fuel_receipts_costs_eia923` and `coalmine_eia923` for an example), this will
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

### Load the data into the database
Each of the dataframes that comes out of the transform step represents a
database table that needs to be loaded into the database. Pandas has a native
`DataFrame.to_sql()` method for inserting records from a dataframe into a
atabase , but it’s extremely slow. Instead, we use postgres’ native `COPY_FROM`
function, which is designed for loading large CSV files directly into the
database very efficiently. Instead of writing the dataframe out to a file on
disk, we create an in-memory file-like object, and read directly from that. For
this to work, the corresponding dataframe and database columns need to be named
identically, and the strings that are read by postgres from the in-memory CSV
file need to be readily interpretable as the data type that is associated with
the column in the table definition. Because Python doesn’t have a native NA
value for integers, but postgres does, just before the dataframes are loaded
into the database we convert any integer NA sentinel values using a little
helper function named `fix_int_na()`.

### Glue the new data to existing data
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

### Create an output module
The `pudl.output` subpackage compiles interesting information from the database
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

### Write some tests
Test cases need to be created for each new dataset, verifying that the ETL
process works, and sanity checking the data itself. This is somewhat different
than traditional software testing, since we're not just testing our code --
we're also trying to make sure that the data is in good shape. Those
exhaustive tests are currently only run locally. Less extensive tests that are
meant to just check that the code is still working correctly need to be
integrated into the `test/travis_ci_test.py` module, which downloads a small
sample of each dataset for use in testing.

---

## Design Guidelines
Some technical norms and expectations that we strive to adhere to, and hope
that contributors can also follow. Some of these are aspirational -- we still
need to go back and apply better naming conventions to much of the data we've
brought in, and the way dates and times are dealt with still needs some work.
Also, we're all learning as we go here -- if you have a suggestion for making
these guidelines better, let us know!

### Use well normalized database tables.
This means keeping as little duplicate or derived information as is practical.
For example, we wouldn't compute heat rates for generators in a table that
contains both fuel heat content and net electricity generation, since the heat
rate would be a derived value. Similarly, we wouldn't apply an inflation
adjustment to reported financial values in the database, since there are a
variety of possible inflation indices users might want to use -- that kind of
transformation would be applied in the output layer that sits on top of the
database.

### Use consistent units across data sources.
Different data sources often
use different units to describe the same type of quantities. Rather than force
users to do endless conversions while using the data, we try to convert similar
quantities into the same units during ETL. For example, we typically convert
all electrical generation to MWh, plant capacities to MW, and heat content to
MMBTUs.

### Use rich data types.
Postgres provides a huge collection of different native data types. In database
table definitions, we try to use the data types which most accurately represent
the data we are storing. This has often come up in the context of time --
rather than storing just the report year or month as a couple of integers, or a
timestamp representing the beginning of the reporting period, we’d prefer store
an interval that explicitly describes the reporting period.

### Compartmentalize the ETL process for each data source.
To the extent possible, it should be possible to bring in each data source
independent of all the others, so that users can pick and choose which data is
pulled in, based on what they need access to locally. In some cases like the
EIA 860 and EIA 923 data, two data sources may be so intertwined that keeping
them separate doesn't relaly make sense, but that should be the exception.

### Separate integration between data sources from the data itself.
To the extent possible, the glue that relates the data sources to each other
should be applied after or alongside the ETL process, and not as a mandatory
part of ETL. This makes it easy to pull individual data sources in and work
with them even when the glue isn't working… which happens sometimes, especially
at the beginning of the integration process.

### Keep large data sets manageable for smaller users.
For now, our goal is that users should be able to initialize the PUDL database
on a decent laptop. However, some of the utility datasets are hundreds of
gigabytes in size (EPA CEMS, FERC EQR, ISO pricing). Many users will not need
to use the entire dataset for the work they are doing. Allow them to pull in
only certain years, or certain states, or other sensible partitions of the data
if need be, so that they don’t run out of memory or disk space, or have to wait
hours for a bunch of data they don't need to get pulled in.

### Give comparable quantities the same names in different tables.
If two columns in different tables record the same quantity in the same units,
give them the same name -- if they end up in the same dataframe for comparison
it's easy to automatically rename them with suffixes indicating where they came
from. For example net electricity generation is reported to both FERC and EIA,
so we've named columns net_generation_mwh in each of those data sources.
Similarly, give non-comparable quantities reported in different data sources
different column names. This helps make it clear that the quantities are
actually different.

### Minimize the use of raw SQL.
We’re trying to stick to using SQL Alchemy’s abstractions  for defining tables,
relationships between tables, constraints, and queries. We find it more
readable generally, and hopefully it will also help keep the project somewhat
more independent of the underlying database, which may be helpful if and when
we need to be able to host the data both on local machines and a platform like
AWS.

### Follow [naming conventions](naming_conventions.md).
We are trying to use consistent naming conventions for database tables,
columns, data sources, and functions. Generally speaking PUDL is a collection
of subpackages organized by purpose (extract, transform, load, analysis,
models, output, datastore…), containing a module for each data source. Each
data source has a short name that is used everywhere throughout the project,
composed of the reporting agency and the form number or another identifying
abbreviation: ferc1, epacems, eia923, mshamines, etc.
