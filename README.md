# The Public Utility Data Liberation (PUDL) Project
---
[![Build Status](https://travis-ci.org/catalyst-cooperative/pudl.svg?branch=master)](https://travis-ci.org/catalyst-cooperative/pudl)
[![Coverage Status](https://coveralls.io/repos/github/catalyst-cooperative/pudl/badge.svg?branch=master)](https://coveralls.io/github/catalyst-cooperative/pudl?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/2fead07adef249c08288d0bafae7cbb5)](https://app.codacy.com/app/zaneselvans/pudl?utm_source=github.com&utm_medium=referral&utm_content=catalyst-cooperative/pudl&utm_campaign=Badge_Grade_Dashboard)
[![Join the chat at https://gitter.im/catalyst-cooperative/pudl](https://badges.gitter.im/catalyst-cooperative/pudl.svg)](https://gitter.im/catalyst-cooperative/pudl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/catalyst-cooperative/pudl/master)

The [Public Utility Data Liberation (PUDL)
project](https://catalyst.coop/pudl/) aims to provide
a useful interface to publicly available electric utility data in the US.  It
uses information from the Federal Energy Regulatory Commission (FERC), the
Energy Information Administration (EIA), and the Environmental Protection
Agency (EPA), among others.

### Quickstart
Just want to get started building a database? Read the setup guide for your
operating system: **[Linux](/docs/getting_started_linux.md)**, **[Mac OS X](/docs/getting_started_mac.md)**, or **[Windows](/docs/getting_started_pc.md)**.

### Contact [Catalyst Cooperative](https://catalyst.coop):
 - Email: [pudl@catalyst.coop](mailto:pudl@catalyst.coop)
 - Chat: https://gitter.im/catalyst-cooperative/pudl
 - Twitter: [@CatalystCoop](https://twitter.com/CatalystCoop)

---
# Project Motivation
The Public Utility Data Liberation (PUDL) project grew out of frustration with
how difficult it is to make use of public data about the US electricity system.
In our own climate activism and policy work we found that many non-profit
organizations, academic researchers, grassroots climate activists, energy
journalists, smaller businesses, and even members of the public sector were
scraping together the same data repeatedly, for one campaign or project at a
time, without accumulating many shared, reusable resources. We decided to try
and create a platform that would serve the many folks who have a stake in our
electricity and climate policies, but may not have the financial resources to
obtain commercially integrated data.

Our energy systems affect everyone, and they are changing rapidly. We hope this
shared resource will improve the efficiency, quality, accessibility and
transparency of research & analysis related to US energy systems.

These ideas have been explored in more depth in papers from Stefan
Pfenninger at ETH Zürich and some of the other organizers of the European [Open
Energy Modeling Initiative](https://openmod-initiative.org/) and [Open Power
System Data](https://open-power-system-data.org/) project:

 * [The importance of open data and software: Is energy research lagging behind?](https://doi.org/10.1016/j.enpol.2016.11.046) (Energy Policy, 2017) Open community modeling frameworks have become common in many scientific disciplines, but not yet in energy. Why is that, and what are the consequences?
 * [Opening the black box of energy modeling: Strategies and lessons learned](https://doi.org/10.1016/j.esr.2017.12.002) (Energy Strategy Reviews, 2018). A closer look at the benefits available from using shared, open energy system models, including less duplicated effort, more transparency, and better research reproducibility.
 * [Open Power System Data: Frictionless Data for Open Power System Modeling](https://doi.org/10.1016/j.apenergy.2018.11.097) (Applied Energy, 2019). An explanation of the motivation and process behind the European OPSD project, which is analogous to our PUDL project, also making use of Frictionless Data Packages.
 * [Open Data for Electricity Modeling](https://www.bmwi.de/Redaktion/EN/Publikationen/Studien/open-Data-for-electricity-modeling.html) (BWMi, 2018). A white paper exploring the legal and technical issues surrounding the use of public data for academic energy system modeling. Primarily focused on the EU, but more generally applicable. Based on a BWMi hosted workshop Catalyst took part in during September, 2018.

We also want to bring best practices from the world of software engineering and data science to energy research and advocacy communities. These papers by Greg Wilson and some of the other organizers of the [Software and Data Carpentries](https://carpetries.org) are good accessible introductions, aimed primarily at an academic audience:

 * [Best practices for scientific computing](https://doi.org/10.1371/journal.pbio.1001745) (PLOS Biology, 2014)
 * [Good enough practices in scientific computing](https://doi.org/10.1371/journal.pcbi.1005510) (PLOS Computational Biology, 2017)

---
# Status (as of 2019-02-10)

## Available Data
### [FERC Form 1 (2004-2017)](https://www.ferc.gov/docs-filing/forms/form-1/data.asp)
A subset of the FERC Form 1 data, mostly pertaining to power plants, their
capital & operating expenses, and fuel consumption. This data is available for
the years 2004-2017. Earlier data is available from FERC, but the structure
of their database differs slightly from the present version somewhat before
2004, and so more work will be required to integrate that information.

### [EIA Form 923 (2009-2017)](https://www.eia.gov/electricity/data/eia923/)
Nearly all of EIA Form 923 is being pulled into the PUDL database, for years
2009-2017. Earlier data is available from EIA, but the reporting format for
earlier years is substantially different from the present day, and will require
more work to integrate. Monthly year to date releases are not yet being
integrated.

### [EIA Form 860 (2011-2017)](https://www.eia.gov/electricity/data/eia860/)
Nearly all of the data reported to the EIA on Form 860 is being pulled into the
PUDL database, for the years 2011-2017. Earlier years use a different
reporting format, and will require more work to integrate. Monthly year to date
releases are not yet being integrated.

### [EPA CEMS (1995-2018)](https://ampd.epa.gov/ampd/)
The EPA's hourly Continuous Emissions Monitoring System (CEMS) data is in the
process of being integrated. However, it is a much larger dataset than the FERC
or EIA data we've already brought in, and so has required some changes to the
overall ETL process. Data from 1995-2017 can be loaded, but it has not yet
been fully integrated. The ETL process for all states and all years takes about
8 hours on a fast laptop.

## Current Work:
Our present focus is on:
* **[Issue #144](https://github.com/catalyst-cooperative/pudl/issues/144)** Automatically assigning unique IDs to plants reported in FERC Form 1 so that time series can be extracted from the multi-year data, and integrating FERC non-fuel operating expenses with EIA fuel expenses to estimate per-generator MCOE.
* **[Issue #204](https://github.com/catalyst-cooperative/pudl/issues/204)** Performing regressions on the non-fuel expenses reported by plants in FERC Form 1, to estimate the fixed vs. variable costs for different categories of plants. This will allow us to estimate the overall marginal cost of electricity for a plant at a given capacity factor and fuel price.
* **[Issue #211](https://github.com/catalyst-cooperative/pudl/issues/211)** Automating the creation of human and machine readable [tabular data packages](https://frictionlessdata.io) containing all of the above data for platform independent redistribution.
* Generally cleaning up some of the data integration and adding more complete test cases, as well as some curated Jupyter notebooks that automatically provide both quantitative and visual sanity checks on the data as it's loaded into the database.

## Future Work:
PUDL is mostly a volunteer effort at the moment, but if we are able to secure some stable support for the project, or build up a larger community of contributors, there's a lot of interesting work we could do!

### Additional Data:
There's a huge variety and quantity of data about the US electric utility system
available to the public. The data listed above is just the beginning! Other data we've heard demand for are listed below. If you're interested in using one of them, and would like to add it to PUDL, check out [our page on contributing](/docs/CONTRIBUTING.md)
* **FERC Form 714:** which includes hourly loads, reported by load balancing
  authorities annually. This is a modestly sized dataset, in the 100s of MB, distributed as Microsoft Excel spreadsheets.
* **FERC EQR:** Aka the Electricity Quarterly Report, this includes the details of many transactions between different utilities, and between utilities and merchant generators. It covers ancillary services as well as energy and capacity, time and location of delivery, prices, contract length, etc. It's one of the few public sources of information about renewable energy power purchase agreements (PPAs). This is a large (~100s of GB) dataset, composed of a very large number of relatively clean CSV files, but it requires fuzzy processing to get at some of the interesting  and only indirectly reported attributes.
* **EIA Form 861:** Includes information about utility demand side management programs, distribution systems, total sales by customer class, net generation, ultimate disposition of power, and other information. This is a smaller dataset (~100s of MB) distributed as Microsoft Excel spreadsheets.
* **ISO/RTO LMP:** Locational marginal electricity pricing information from the various grid operators (e.g. MISO, CAISO, NEISO, PJM, ERCOT...). At high time resolution, with many different delivery nodes, this can become a very large dataset (100s of GB). The format for the data is different for each of the ISOs. Physical location of the delivery nodes is not always publicly available.
* **MSHA Mines & Production:** Describes coal production by mine and operating company, along with statistics about labor productivity and safety. This is a smaller dataset (100s of MB) available as relatively clean and well structured CSV files.
* **PHMSA Natural Gas Pipelines:** the Pipeline and Hazardous Materials Safety Administration (which is part of the US Dept. of Transportation) collects data about the natural gas transmission and distribution system, including their age, length, diameter, materials, and carrying capacity.
* **US Grid Models:** In order to run electricity system operations models and cost optimizations, you need some kind of model of the interconnections between generation and loads. There doesn't appear to be a generally accepted, publicly available set of these network descriptions (yet!).

### Other Data?
If there are other valuable public datasets related to the US energy system that aren't already easily accessible for use in programmatic analysis, you can [create a a new issue](https://github.com/catalyst-cooperative/pudl/issues/new/choose) describing the data, explaining why it's interesting, and linking to it. Tag it [new data](https://github.com/catalyst-cooperative/pudl/issues?q=is%3Aissue+is%3Aopen+label%3A%22new+data%22).

### PUDL in the Cloud
As the volume of data integrated into PUDL increases, the idea of distributing
the data by asking users to either run the processing pipeline themselves, or
to increasingly large collections of data packages to do their own analyses
becomes less and less practical. One alternative is to deploy our ready to use
data alongside other publicly available data of interest in a cloud computing
platform that also offers computational resources for working with the data.
This way there could be a single copy of the data shared by many people, which
is updated frequently, minimizing the administrative overhead required to work
with it.

The [Pangeo](https://pangeo.io) platform does exactly this for various
communities of Earth scientists, using a
[JupyterHub](https://jupyterhub.readthedocs.io/en/stable/) deployment that
includes commonly used scientific software packages and a shared domain
specific data repository. If there's interest from potential users, we'd like
to set up this kind of open and collaborative analysis platform.

---
# Project Organization
The PUDL repository is organized generally around the recommendations from [Good enough practices in scientific computing](https://doi.org/10.1371/journal.pcbi.1005510))

### data/
A data store containing the original data from FERC, EIA, EPA and other agencies. It's organized first by agency, then by form, and (in most cases) finally year. For example, the FERC Form 1 data from 2014 would be found in `./data/ferc/form1/f1_2014` and the EIA data from 2010 can be found in `./data/eia/form923/f923_2010`. The year-by-year directory structure is determined by the reporting agency, based on what is provided for download.

The data itself is too large to be conveniently stored within the git repository, so we use [a datastore management script](/scripts/update_datastore.py) that can pull down the most recent version of all the data that's needed to build the PUDL database, and organize it so that the software knows where to find it. Run `python ./scripts/update_datastore.py --help` for more info.

### docs/
Documentation related to the data sources, our results, and how to go about
getting the PUDL database up and running on your machine. We try to keep these
in text or Jupyter notebook form. Other files that help explain the data
sources are also stored under here, in a hierarchy similar to the data store.
E.g. a blank copy of the FERC Form 1 is available in `./docs/ferc/form1/` as a
PDF.

### pudl/
The PUDL python package, where all of our actual code ends up. The modules and packages are organized by data source, as well as by what step of the database initialization process (extract, transform, load) they pertain to. For example:
 - [`./pudl/extract/eia923.py`](/pudl/extract/eia923.py)
 - [`./pudl/transform/ferc1.py`](/pudl/transform/ferc1.py)

The load step is currently very simple, and so it just has a single top level module dedicated to it.

The database models (table definitions) are also organized by data source, and are kept in the models subpackage. E.g.:
 - [`./pudl/models/eia923.py`](/pudl/models/eia923.py)
 - [`./pudl/models/eia860.py`](/pudl/models/eia860.py)

We are beginning to accumulate analytical functionality in the [analysis subpackage](/pudl/analysis/), like calculation of the marginal cost of electricity (MCOE) on a per generator basis. The [output subpackage](/pudl/output/) contains data source specific output routines and an [output class definition](/pudl/output/pudltabl.py).

### Other miscellaneous bits:
 - [`./pudl/constants.py`](/pudl/constants.py) stores a variety of static
   data for loading, like the mapping of FERC Form 1 line numbers to FERC
   account numbers in the plant in service table, or the mapping of EIA923
   spreadsheet columns to database column names over the years, or the list of
   codes describing fuel types in EIA923.

 - [`./pudl/helpers.py`](/pudl/constants.py) contains a collection of
   helper functions that are used throughout the project.

### results/
The results directory contains derived data products. These are outputs from our manipulation and combination of the original data, that are necessary for the integration of those data sets into the central database. It also contains outputs we've generated for others.

The results directory also contains [a collection of Jupyter notebooks](/pudl/results/notebooks) (which desperately needs organizing) presenting various data processing or analysis tasks, such as pulling the required IDs from the cloned FERC database to use in matching up plants and utilities between FERC and EIA datasets.

### scripts/
A collection of command line tools written in Python and used for high level
management of the PUDL database system, e.g. the initial download of and
ongoing updates to the datastore, and the initialization of your local copy of
the PUDL database.  These scripts are generally meant to be run from within the
`./scripts` directory, and should all have built-in Documentation as to their
usage. Run `python script_name.py --help` to for more information.

### test/
The test directory holds test cases which are run with `pytest`. To run a
complete suite of tests that ingests all of the available and working data, and
performs some post-ingest analysis, you simply run `pytest` from the top level
PUDL directory. You can choose to run the post-analysis tests using an
existing, live (rather than test) database by adding `--live_pudl_db` and
`--live_ferc_db` to the command line. Populating the test database from scratch
can take ~20 minutes. After the tests have completed, the test database is
dropped. See `pytest --help` for more information.

More information on PyTest can be found at: http://docs.pytest.org/en/latest/
