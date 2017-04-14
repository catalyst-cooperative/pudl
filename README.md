The Public Utility Data Liberation project aims to provide a useful interface
to publicly available electric utility data in the US.  It uses information
from the Federal Energy Regulatory Commission (FERC), the Energy Information
Administration (EIA), and the Environmental Protection Agency (EPA), among
others. Until January 1st, 2018, the project is private. After that date it
will be available publicly under an MIT License. The software has been
developed in collaboration with the Climate Policy Initiative (CPI) on a
philanthropically funded project. Most of the code is written in Python.
Development is managed through a shared Git repository, which can be found at:

https://github.com/catalyst-cooperative/pudl

For more information, get in touch with:
 - Catalyst Cooperative
 - http://catalyst.coop
 - hello@catalyst.coop

---
# Project Status
As of v0.1, the data which have been integrated into the PUDL database include:

## FERC Form 1
A subset of the FERC Form 1 data, mostly pertaining to power plants, their
capital & operating expenses, and fuel consumption. This data is available for
the years 2007-2015. Earlier data is available from FERC, but the structure of
their database differs from the present version somewhat before 2007, and so
more work will be required to integrate that information.

## EIA Form 923
Nearly all of EIA Form 923 is being pulled into the PUDL database, for years
2009-2016. Earlier data is available from EIA, but the reporting format for
earlier years is substantially different from the present day, and will require
more work to integrate.

---
# Project Layout
A brief layout and explanation of the files and directories you should find
here.  This is generally based on the "Good Enough Practices in Scientific
Computing" white paper from the folks at Software Carpentry, which you can
and should read in its entirety here: https://arxiv.org/pdf/1609.00037v2.pdf

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

The data itself is too large to conveniently be stored within the git
repository, but several shell scripts (e.g. `./data/eia/form923/get_eia923.sh`) are provided to automatically download
the data form the federal agencies and re-organize the directory hierarchy
to be compatible with the PUDL modules. These scripts also set the permissions
on the downloaded data to read only, to prevent it from being corrupted or
edited by the user accidentally.

## docs/
Documentation related to the data sources or our results, preferably in text
format if we're creating them, and under revision control. Other files that
help explain the data sources are also stored under here, in a hierarchy
similar to the data store.  E.g. a blank copy of the FERC Form 1 is available
in `./docs/ferc/form1/` as a PDF.

## pudl/
The PUDL python package, where all of our actual code ends up.  Each data
source has its own module (e.g. `./pudl/eia923.py`, `./pudl/ferc1.py`),
containing routines specific to extracting,  processing, and loading it from
the original data sources into a form which is accessible for integration into
the PUDL database.  In addition, each data source has a module which defines
the data model associated with it in the PUDL database (e.g.
`./pudl/models_eia923.py`, `./pudl/models_ferc1.py`)

Database structures that are not associated with a particular data source are
defined in `./pudl/models.py` and the high level functions that initialize
the PUDL database are found in `./pudl/pudl.py`.  Currently this includes
many functions specific to ingesting the tables specific to particular data
sources.  Those ingest functions will likely be moved to their own source
specific modules in a future release.

## results/
The results directory contains derived data products. These are outputs from
our manipulation and combination of the original data, that are necessary for
the integration of those data sets into the central database. It will also
be the place where outputs we're going to hand off to others get put.

The results directory also contains a collection of Jupyter notebooks
presenting various data processing or analysis tasks, such as pulling the
required IDs from the cloned FERC database to use in matching up plants and
utilities between FERC and EIA datasets.

## test/
The test directory holds test cases which are run with `pytest`.  Each data
source has its own test cases (e.g. `./test/test_eia923.py`) which exercises
the routines required to prepare that data source for loading into PUDL. The
loading of all data sources which ought to be functional can be tested using
`./test/test_pudl.py` by calling `pytest -s test/test_pudl.py`. As of v0.1
the full PUDL test takes about 20 minutes to run.  After the tests have been
run the test database is deleted.

More information on pytest can be found at: http://docs.pytest.org/en/latest/

## Other miscellaneous bits:
 - `settings.py` contains global settings for things like connecting to the
 postgres database, and a few paths to resources within the repository.
 - `constants.py` stores a variety of static data for loading, like the mapping
 of FERC Form 1 line numbers to FERC account numbers in the plant in service
 table, or the mapping of EIA923 spreadsheet columns to database column names
 over the years, or the list of codes describing fuel types in EIA923.
 - `init_pudl.py` is a command line program that can be used to load the
 currently available PUDL data into a live database for use.
