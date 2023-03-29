===============================================================================
The Public Utility Data Liberation Project (PUDL)
===============================================================================

.. readme-intro

.. image:: https://www.repostatus.org/badges/latest/active.svg
   :target: https://www.repostatus.org/#active
   :alt: Project Status: Active

.. image:: https://github.com/catalyst-cooperative/pudl/workflows/tox-pytest/badge.svg
   :target: https://github.com/catalyst-cooperative/pudl/actions?query=workflow%3Atox-pytest
   :alt: Tox-PyTest Status

.. image:: https://img.shields.io/codecov/c/github/catalyst-cooperative/pudl?style=flat&logo=codecov
   :target: https://codecov.io/gh/catalyst-cooperative/pudl
   :alt: Codecov Test Coverage

.. image:: https://img.shields.io/readthedocs/catalystcoop-pudl?style=flat&logo=readthedocs
   :target: https://catalystcoop-pudl.readthedocs.io/en/latest/
   :alt: Read the Docs Build Status

.. image:: https://img.shields.io/pypi/v/catalystcoop.pudl
   :target: https://pypi.org/project/catalystcoop.pudl/
   :alt: PyPI Latest Version

.. image:: https://img.shields.io/conda/vn/conda-forge/catalystcoop.pudl
   :target: https://anaconda.org/conda-forge/catalystcoop.pudl
   :alt: conda-forge Version

.. image:: https://img.shields.io/pypi/pyversions/catalystcoop.pudl
   :target: https://pypi.org/project/catalystcoop.pudl/
   :alt: Supported Python Versions

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Any color you want, so long as it's black.

.. image:: https://results.pre-commit.ci/badge/github/catalyst-cooperative/pudl/main.svg
   :target: https://results.pre-commit.ci/latest/github/catalyst-cooperative/pudl/main
   :alt: pre-commit CI

.. image:: https://zenodo.org/badge/80646423.svg
   :target: https://zenodo.org/badge/latestdoi/80646423
   :alt: Zenodo DOI

.. image:: https://img.shields.io/badge/calend.ly-officehours-darkgreen
   :target: https://calend.ly/catalyst-cooperative/pudl-office-hours
   :alt: Schedule a 1-on-1 chat with us about PUDL.

What is PUDL?
-------------

The `PUDL <https://catalyst.coop/pudl/>`__ Project is an open source data processing
pipeline that makes US energy data easier to access and use programmatically.

Hundreds of gigabytes of valuable data are published by US government agencies, but
it's often difficult to work with. PUDL takes the original spreadsheets, CSV files,
and databases and turns them into a unified resource. This allows users to spend more
time on novel analysis and less time on data preparation.

What data is available?
-----------------------

PUDL currently integrates data from:

* `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`__: 2001-2021
* `EIA Form 860m <https://www.eia.gov/electricity/data/eia860m/>`__: 2022-06
* `EIA Form 861 <https://www.eia.gov/electricity/data/eia861/>`__: 2001-2021
* `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`__: 2001-2021
* `EPA Continuous Emissions Monitoring System (CEMS) <https://campd.epa.gov/>`__: 1995-2021
* `FERC Form 1 <https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual>`__: 1994-2021
* `FERC Form 714 <https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric/data>`__: 2006-2020
* `US Census Demographic Profile 1 Geodatabase <https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html>`__: 2010

Thanks to support from the `Alfred P. Sloan Foundation Energy & Environment
Program <https://sloan.org/programs/research/energy-and-environment>`__, from
2021 to 2023 we will be integrating the following data as well:

* `EIA Form 176 <https://www.eia.gov/dnav/ng/TblDefs/NG_DataSources.html#s176>`__
  (The Annual Report of Natural Gas Supply and Disposition)
* `FERC Electric Quarterly Reports (EQR) <https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr>`__
* `FERC Form 2 <https://www.ferc.gov/industries-data/natural-gas/overview/general-information/natural-gas-industry-forms/form-22a-data>`__
  (Annual Report of Major Natural Gas Companies)
* `PHMSA Natural Gas Annual Report <https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids>`__
* Machine Readable Specifications of State Clean Energy Standards

Who is PUDL for?
----------------

The project is focused on serving researchers, activists, journalists, policy makers,
and small businesses that might not otherwise be able to afford access to this data
from commercial sources and who may not have the time or expertise to do all the
data processing themselves from scratch.

We want to make this data accessible and easy to work with for as wide an audience as
possible: anyone from a grassroots youth climate organizers working with Google
sheets to university researchers with access to scalable cloud computing
resources and everyone in between!

How do I access the data?
-------------------------

There are four main ways to access PUDL outputs. For more details you'll want
to check out `the complete documentation
<https://catalystcoop-pudl.readthedocs.io>`__, but here's a quick overview:

Datasette
^^^^^^^^^
We publish a lot of the data on https://data.catalyst.coop using a tool called
`Datasette <https://datasette.io>`__ that lets us wrap our databases in a relatively
friendly web interface. You can browse and query the data, make simple charts and
maps, and download portions of the data as CSV files or JSON so you can work with it
locally. For a quick introduction to what you can do with the Datasette interface,
check out `this 17 minute video <https://simonwillison.net/2021/Feb/7/video/>`__.

This access mode is good for casual data explorers or anyone who just wants to grab a
small subset of the data. It also lets you share links to a particular subset of the
data and provides a REST API for querying the data from other applications.

Docker + Jupyter
^^^^^^^^^^^^^^^^
Want access to all the published data in bulk? If you're familiar with Python
and `Jupyter Notebooks <https://jupyter.org/>`__ and are willing to install Docker you
can:

* `Download a PUDL data release <https://sandbox.zenodo.org/record/764696>`__ from
  CERN's `Zenodo <https://zenodo.org>`__ archiving service.
* `Install Docker <https://docs.docker.com/get-docker/>`__
* Run the archived image using ``docker-compose up``
* Access the data via the resulting Jupyter Notebook server running on your machine.

If you'd rather work with the PUDL `SQLite <https://sqlite.org>`__ Databases and
`Apache Parquet <https://parquet.apache.org>`__ files directly, they are accessible
within the same Zenodo archive.

The `PUDL Examples repository <https://github.com/catalyst-cooperative/pudl-examples>`__
has more detailed instructions on how to work with the Zenodo data archive and Docker
image.

JupyterHub
^^^^^^^^^^
Do you want to use Python and Jupyter Notebooks to access the data but aren't
comfortable setting up Docker? We are working with `2i2c <https://2i2c.org>`__ to host
a JupyterHub that has the same software and data as the Docker container and Zenodo
archive mentioned above, but running in the cloud.

* `Request an account <https://forms.gle/TN3GuE2e2mnWoFC4A>`__
* `Log in to the JupyterHub <https://bit.ly/pudl-examples-01>`__

**Note:** you'll only have 4-6GB of RAM and 1 CPU to work with on the JupyterHub, so
if you need more computing power, you may need to set PUDL up on your own computer.
Eventually we hope to offer scalable computing resources on the JupyterHub as well.

The PUDL Development Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you're more familiar with the Python data science stack and are comfortable working
with git, ``conda`` environments, and the Unix command line, then you can set up the
whole PUDL Development Environment on your own computer. This will allow you to run the
full data processing pipeline yourself, tweak the underlying source code, and (we hope!)
make contributions back to the project.

This is by far the most involved way to access the data and isn't recommended for
most users. You should check out the Development section of the main `PUDL
documentation <https://catalystcoop-pudl.readthedocs.io>`__ for more details.

Nightly Data Builds
^^^^^^^^^^^^^^^^^^^
If you are less concerned with reproducibility and want the freshest possible data
we also upload the outputs of our nightly builds to public S3 storage buckets. This
data is produced by the `dev branch <https://github.com/catalyst-cooperative/pudl/tree/dev>`__,
of PUDL, and is updated most weekday mornings. It is also the data used to populate
Datasette:

* `PUDL SQLite DB <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/pudl.sqlite>`__
* `EPA CEMS Hourly Emissions Parquet (1995-2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/hourly_emissions_epacems.parquet>`__
* `Census DP1 SQLite DB (2010) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/censusdp1tract.sqlite>`__

* Raw FERC Form 1:

  * `FERC-1 SQLite derived from DBF (1994-2020) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1.sqlite>`__
  * `FERC-1 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1_xbrl.sqlite>`__
  * `FERC-1 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1_xbrl_datapackage.json>`__
  * `FERC-1 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc1_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 2:

  * `FERC-2 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2_xbrl.sqlite>`__
  * `FERC-2 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2_xbrl_datapackage.json>`__
  * `FERC-2 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc2_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 6:

  * `FERC-6 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6_xbrl.sqlite>`__
  * `FERC-6 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6_xbrl_datapackage.json>`__
  * `FERC-6 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc6_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 60:

  * `FERC-60 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60_xbrl.sqlite>`__
  * `FERC-60 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60_xbrl_datapackage.json>`__
  * `FERC-60 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc60_xbrl_taxonomy_metadata.json>`__

* Raw FERC Form 714:

  * `FERC-714 SQLite derived from XBRL (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc714_xbrl.sqlite>`__
  * `FERC-714 Datapackage (JSON) describing SQLite derived from XBRL <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc714_xbrl_datapackage.json>`__
  * `FERC-714 XBRL Taxonomy Metadata as JSON (2021) <https://s3.us-west-2.amazonaws.com/intake.catalyst.coop/dev/ferc714_xbrl_taxonomy_metadata.json>`__

Contributing to PUDL
--------------------
Find PUDL useful? Want to help make it better? There are lots of ways to help!

* First, be sure to read our `Code of Conduct <https://catalystcoop-pudl.readthedocs.io/en/latest/code_of_conduct.html>`__.
* You can file a bug report, make a feature request, or ask questions in the
  `Github issue tracker <https://github.com/catalyst-cooperative/pudl/issues>`__.
* Feel free to fork the project and make a pull request with new code, better
  documentation, or example notebooks.
* `Make a recurring financial contribution <https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PZBZDFNKBJW5E&source=url>`__
  to support our work liberating public energy data.
* `Hire us to do some custom analysis <https://catalyst.coop/hire-catalyst/>`__ and
  allow us to integrate the resulting code into PUDL.
* For more information check out the Contributing section of the
  `PUDL Documentation <https://catalystcoop-pudl.readthedocs.io>`__

Licensing
---------

In general, our code, data, and other work are permissively licensed for use by anybody,
for any purpose, so long as you give us credit for the work we've done.

* The PUDL software is released under
  `the MIT License <https://opensource.org/licenses/MIT>`__.
* The PUDL data and documentation are published under the
  `Creative Commons Attribution License v4.0 <https://creativecommons.org/licenses/by/4.0/>`__
  (CC-BY-4.0).

Contact Us
----------

* For bug reports, feature requests, and other software or data issues please make a
  `GitHub Issue <https://github.com/catalyst-cooperative/pudl/issues>`__.
* For more general support, questions, or other conversations around the project
  that might be of interest to others, check out the
  `GitHub Discussions <https://github.com/catalyst-cooperative/pudl/discussions>`__
* If you'd like to get occasional updates about the project
  `sign up for our email list <https://catalyst.coop/updates/>`__.
* Want to schedule a time to chat with us one-on-one about your PUDL use case, ideas
  for improvement, or get some personalized support? Join us for
  `Office Hours <https://calend.ly/catalyst-cooperative/pudl-office-hours>`__
* Follow us on Twitter: `@CatalystCoop <https://twitter.com/CatalystCoop>`__
* More info on our website: https://catalyst.coop
* For private communication about the project or to hire us to provide customized data
  extraction and analysis, you can email the maintainers:
  `pudl@catalyst.coop <mailto:pudl@catalyst.coop>`__

About Catalyst Cooperative
--------------------------

`Catalyst Cooperative <https://catalyst.coop>`__ is a small group of data wranglers
and policy wonks organized as a worker-owned cooperative consultancy. Our goal is a
more just, livable, and sustainable world. We integrate public data and perform
custom analyses to inform public policy
(`Hire us! <https://catalyst.coop/hire-catalyst>`__). Our focus is primarily on
mitigating climate change and improving electric utility regulation in the United
States.
