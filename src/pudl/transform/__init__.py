"""
Modules implementing the "Transform" step of the PUDL ETL pipeline.

Each module in this subpackage transforms the tabular data associated with a single data
source from the PUDL :ref:`data-sources`. This process begins with a dictionary of
"raw" :class:`pandas.DataFrame` objects produced by the corresponding data source
specific routines from the :mod:`pudl.extract` subpackage, and ends with a dictionary of
:class:`pandas.DataFrame` objects that are fully normalized, cleaned, and ready to be
loaded into external databases and Parquet files by the :mod:`pudl.load` subpackage.

Inputs to the transform functions are a dictionary of dataframes, each of which
represents a concatenation of records with common column names from across some set of
years of reported data. The names of those columns are determined by the Excel
spreadsheet mapping metadata associated with the given dataset in PUDL's
``package_data``.

This raw data is transformed in 3 main steps:

1. Structural transformations that re-shape / tidy the data and turn it into rows that
   represent a single observation, and columns that represent a single variable. These
   transformations should not require knowledge of or access to the contents of the
   data, which may or may not yet be usable at this point, depending on the true data
   type and how much cleaning has to happen. One exception to this that may come up is
   the need to clean up columns that are part of the primary composite key, since you
   can't usefully index on NA values. Alternatively this might mean removing rows that
   have invalid key values.

2. Data type compatibility: whatever massaging of the data is required to ensure that it
   can be cast to the appropriate data type, including identifying NA values and
   assigning them to an appropriate type-specific NA value. At the end of this you can
   assign all the columns their (preferably nullable) types. Note that because some of
   the columns that exist at this point may not end up in the final database table, you
   may need to set them individually, rather than using the systemwide dictionary of
   column data types.

3. Value based data cleaning: At this point every column should have a known, homogenous
   type, allowing it to be reliably manipulated as a Series, so we can move on to
   cleaning up the values themselves. This includes re-coding freeform string fields to
   impose a controlled vocabulary, converting column units (e.g. kWh to MWh) and
   renaming the columns appropriately, as well as correcting clear data entry errors.

At the end of the main coordinating transform() function, every column that remains in
each of the transformed dataframes should correspond to a column that will exist in the
database and be associated with the EIA datasets, which means it is also part of the EIA
column namespace. It's important that you make sure these column names match the naming
conventions that are being used, and if any of the columns exist in other tables, that
they have exactly the same name and datatype.

If you find that you need to rename a column for it to conform to those requirements, in
many cases that should happen in the Excel spreadsheet mapping metadata, so that column
renamings can be kept to a minimum and only used for real semantic transformations of a
column (like a unit conversion).

At the end of this step, it should be easy to categorize every column in every
dataframe as to whether it is a "data" column (containing data unique to the table it
is found in) or whether it is part of the primary key for the table (the minimal set of
columns whose values are required to uniquely specify a record), and/or whether it is a
"denormalized" column whose home table is really elsewhere in the database. Note that
denormalized columns may also be part of the primary key. This information is important
for the step after the intra-table transformations during which the collection of EIA
tables is normalized as a whole.

"""
