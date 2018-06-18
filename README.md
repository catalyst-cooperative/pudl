# The Public Utility Data Liberation (PUDL) Project
[![Build Status](https://travis-ci.org/catalyst-cooperative/pudl.svg?branch=master)](https://travis-ci.org/catalyst-cooperative/pudl)

The Public Utility Data Liberation project aims to provide a useful interface
to publicly available electric utility data in the US.  It uses information
from the Federal Energy Regulatory Commission (FERC), the Energy Information
Administration (EIA), and the Environmental Protection Agency (EPA), among
others.

https://github.com/catalyst-cooperative/pudl

For more information, get in touch with:
 - Catalyst Cooperative
 - https://catalyst.coop
 - hello@catalyst.coop

# Quickstart
Just want to get started building a database? Read [the documentation for your
operating system](/docs).

---
# Project Status
As of June, 2018 the data which have been integrated into the PUDL database
include:

## FERC Form 1
A subset of the FERC Form 1 data, mostly pertaining to power plants, their
capital & operating expenses, and fuel consumption. This data is available for
the years 2004-2016. Earlier data is available from FERC, but the structure of
their database differs slightly from the present version somewhat before 2004,
and so more work will be required to integrate that information.

## EIA Form 923
Nearly all of EIA Form 923 is being pulled into the PUDL database, for years
2009-2016. Earlier data is available from EIA, but the reporting format for
earlier years is substantially different from the present day, and will require
more work to integrate.

## EIA Form 860
Nearly all of the data reported to the EIA on Form 860 is being pulled into the
PUDL database, for the years 2011-2016. Earlier years use a different reporting
format, and will require more work to integrate.

## EPA Continuous Emissions Monitoring System (CEMS)
The EPA's hourly CEMS data is in the process of being integrated. However, it
is a much larger dataset than the FERC or EIA data we've already brought in, and
so has required some changes to the overall ETL process.

---
# Project Layout
A brief layout and explanation of the files and directories you should find
here.  This is generally based on the "Good Enough Practices in Scientific
Computing" white paper from the folks at Software Carpentry, which you can and
should read in its entirety here: https://arxiv.org/pdf/1609.00037v2.pdf

## LICENSE.md
Copyright and licensing information.

## README.md
The file you're reading right now...

## REQUIREMENTS.md
An explanation of the other software and data you'll need to have installed in
order to make use of PUDL.

## data/
A data store containing the original data from FERC, EIA, EPA and other
agencies. It's organized first by agency, then by form, and finally year. For
example, the FERC Form 1 data from 2014 would be found in
`./data/ferc/form1/f1_2014` and the EIA data from 2010 can be found in
`./data/eia/form923/f923_2010`. The year-by-year directory structure is
determined by the reporting agency, based on what is provided for download.

The data itself is too large to be conveniently stored within the git
repository, so we have a script (`./scripts/update_datastore.py`) that can pull
down the most recent version of all the data that's needed to build the PUDL
database, and organize it so that the software knows where to find it. Run
`python ./scripts/update_datastore.py --help` for more info.

## docs/
Documentation related to the data sources, our results, and how to go about
getting the PUDL database up and running on your machine. We try to keep these
in text or Jupyter notebook form. Other files that help explain the data
sources are also stored under here, in a hierarchy similar to the data store.
E.g. a blank copy of the FERC Form 1 is available in `./docs/ferc/form1/` as a
PDF.

## pudl/
The PUDL python package, where all of our actual code ends up. The modules and
packages are organized by data source, as well as by what step of the database
initialization process (extract, transform, load) they pertain to. For example:
 - `./pudl/extract/eia923.py`
 - `./pudl/transform/ferc1.py`

The load step is currently very simple, and so it just has a single top level
module dedicated to it.

The database models (table definitions) are also organized by data source, and
are kept in the models subpackage. E.g.:
 - `./models/eia923.py`
 - `./models/eia860.py`

We are beginning to accumulate analytical functionality in additional modules.
`./pudl/analysis.py` is a staging area where new analysis functions go before
getting organized into their own thematic modules like `./pudl/outputs.py`
which generates some useful tabular outputs for folks who would like to work
with CSV files, or `./pudl/mcoe.py` which calculates the marginal cost of
electricity by generator for every power plant in the US which reports to EIA
and FERC. Eventually these analytical tools that sit on top of the database
will need to get organized into their own subpackages, but for now they're kind
of chaotic.

### Other miscellaneous bits:
 - `./pudl/settings.py` contains global settings for things like connecting to
   the postgres database, and a few paths to resources within the repository.

 - `./pudl/constants.py` stores a variety of static data for loading, like the
   mapping of FERC Form 1 line numbers to FERC account numbers in the plant in
   service table, or the mapping of EIA923 spreadsheet columns to database
   column names over the years, or the list of codes describing fuel types in
   EIA923.

## results/
The results directory contains derived data products. These are outputs from
our manipulation and combination of the original data, that are necessary for
the integration of those data sets into the central database. It will also be
the place where outputs we're going to hand off to others get put.

The results directory also contains a collection of Jupyter notebooks
presenting various data processing or analysis tasks, such as pulling the
required IDs from the cloned FERC database to use in matching up plants and
utilities between FERC and EIA datasets.

## scripts/
A collection of command line tools written in Python and used for high level
management of the PUDL database system, e.g. the initial download of and
ongoing updates to the datastore, and the initialization of your local copy of
the PUDL database.  These scripts are generally meant to be run from within the
`./scripts` directory, and should all have built-in Documentation as to their
usage. Run `python script_name.py --help` to for more information.

## test/
The test directory holds test cases which are run with `pytest`. To run a
complete suite of tests that ingests all of the available and working data, and
performs some post-ingest analysis, you simply run `pytest` from the top level
PUDL directory. You can choose to run the post-analysis tests using an
existing, live (rather than test) database by adding `--live_pudl_db` and
`--live_ferc_db` to the command line. Populating the test database from scratch
can take ~20 minutes. After the tests have completed, the test database is
dropped. See `pytest --help` for more information.

More information on PyTest can be found at: http://docs.pytest.org/en/latest/
