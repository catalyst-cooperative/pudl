===============================================================================
The Public Utility Data Liberation Project (PUDL)
===============================================================================

.. readme-intro

.. image:: https://www.repostatus.org/badges/latest/wip.svg
   :alt: Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.
   :target: https://www.repostatus.org/#wip

.. image:: https://travis-ci.org/catalyst-cooperative/pudl.svg?branch=master
   :target: https://travis-ci.org/catalyst-cooperative/pudl
   :alt: Build Status

.. image:: https://readthedocs.org/projects/catalystcoop-pudl/badge/?version=latest
   :target: https://catalystcoop-pudl.readthedocs.io/en/latest/
   :alt: Documentation Status

.. image:: https://codecov.io/gh/catalyst-cooperative/pudl/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/catalyst-cooperative/pudl
   :alt: codecov

.. image:: https://api.codacy.com/project/badge/Grade/2fead07adef249c08288d0bafae7cbb5
   :target: https://app.codacy.com/app/zaneselvans/pudl
   :alt: Codacy Badge

`PUDL <https://catalyst.coop/pudl/>`__ makes US energy data easier to access
and work with. Hundreds of gigabytes of supposedly public information published
by government agencies, but in a bunch of different formats that can be hard to
work with and combine. PUDL takes these spreadsheets, CSV files, and databases
and turns them into easy to parse, well-documented
`tabular data packages <https://https://frictionlessdata.io/docs/tabular-data-package/>`__
that can be used to create a database, used directly with Python, R, Microsoft
Access, and lots of other tools.

The project currently contains data from:

* `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`__
* `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`__
* `The EPA Continuous Emissions Monitoring System (CEMS) <https://ampd.epa.gov/ampd/>`__
* `The EPA Integrated Planning Model (IPM) <https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6>`__
* `FERC Form 1 <https://www.ferc.gov/docs-filing/forms/form-1/data.asp>`__

We are especially interested in serving researchers, activists, journalists,
and policy makers that might not otherwise be able to afford access to this
data from commercial data providers.

Getting Started TEMPORARILY OUT OF DATE
---------------------------------------

Just want to play with some example data? Install
`Anaconda <https://www.anaconda.com/distribution/>`__
(or `miniconda <https://docs.conda.io/en/latest/miniconda.html>`__
if you like the command line) with at least Python 3.7. Then run the following
commands in your terminal:

**NOTE: (2019-09-03)** this next code block won't work unless you have the old
PostgreSQL PUDL database set up. We are in the process of deprecating that
database, and using tabular datapackages that feed into SQLite instead.
However, the code is temporarily out of sync with the docs. The last version of
the guide to setting up the PostgreSQL database can be found in
`this commit <https://github.com/catalyst-cooperative/pudl/blob/a8173bd78857d4a09ddf685b19fea0a83f2e5007/docs/getting_started.md#4-install-and-configure-postgresql>`__ if you need to get it set up in the interim.

.. code-block:: console

    $ git clone https://github.com/catalyst-cooperative/pudl.git
    $ conda env create --name pudl --file pudl/environment.yml
    $ conda activate pudl
    $ pip install -e pudl
    $ mkdir pudl-work
    $ pudl_setup --pudl_in=pudl-work --pudl_out=pudl-work
    $ pudl_data --sources eia923 eia860 ferc1 epacems epaipm --years 2017 --states id
    $ pudl_etl pudl-work/settings/pudl_etl_example.yml
    $ jupyter-lab --notebook-dir=pudl_workspace/notebooks

This will install the PUDL Python package, create some local directories
inside a directory called ``pudl-work``, download the most recent year of
data from the public agencies, load it into a local PostgreSQL database,
and open up a folder with some example `Jupyter noteboooks <https://jupyter.org>`__
in your web browser.

We are transitioning to generating CSV/JSON based
`tabular data packages <https://frictionlessdata.io/docs/tabular-data-package/>`__,
which are then loaded into a local SQLite database to make setting up PUDL
easier.

**NOTE:** The example above requires a computer with at least **4 GB of RAM**
and **several GB of free disk space**. You will also need to download about
**500 MB of data**. This could take a while if you have a slow internet
connection.

For more details, see `the full PUDL documentation
<https://catalystcoop-pudl.readthedocs.io/>`__.

Contributing to PUDL
--------------------

Find PUDL useful? Want to help make it better? There are lots of ways to
contribute!

* Please be sure to read our `Code of Conduct <https://catalystcoop-pudl.readthedocs.io/en/latest/CODE_OF_CONDUCT.html>`__
* You can file a bug report, make a feature request, or ask questions in the
  `Github issue tracker
  <https://github.com/catalyst-cooperative/pudl/issues>`__.
* Feel free to fork the project and make a pull request with new code,
  better documentation, or example notebooks.
* `Make a financial contribution <https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PZBZDFNKBJW5E&source=url>`__ to support our work
  liberating public energy data.
* Hire us to do some custom analysis, and let us add the code the project.
* For more information check out our `Contribution Guidelines <https://catalystcoop-pudl.readthedocs.io/en/latest/CONTRIBUTING.html>`__

Licensing
---------

The PUDL software is released under the `MIT License <https://opensource.org/licenses/MIT>`__.
`The PUDL documentation <https://catalystcoop-pudl.readthedocs.io>`__
and the data packages we distribute are released under the `Creative Commons Attribution 4.0 License <https://creativecommons.org/licenses/by/4.0/>`__.

Contact Us
----------

For help with initial setup, usage questions, bug reports, suggestions to make
PUDL better and anything else that could conceivably be of use or interest to
the broader community of users, use the
`PUDL issue tracker <https://github.com/catalyst-cooperative/pudl/issues>`__.
on Github. For private communication about the project, you can email the
team: `pudl@catalyst.coop <mailto:pudl@catalyst.coop>`__

About Catalyst Cooperative
--------------------------

`Catalyst Cooperative <https://catalyst.coop>`__ is a small group of data
scientists and policy wonks. We’re organized as a worker-owned cooperative
consultancy. Our goal is a more just, livable, and sustainable world. We
integrate public data and perform custom analyses to inform public policy
making. Our focus is primarily on mitigating climate change and improving
electric utility regulation in the United States.

Do you work on renewable energy or climate policy? Have you found yourself
scraping data from government PDFs, spreadsheets, websites, and databases,
without getting something reusable? We build tools to pull this kind of
information together reliably and automatically so you can focus on your real
work instead — whether that’s political advocacy, energy journalism, academic
research, or public policy making.

* Web: https://catalyst.coop
* Newsletter: https://catalyst.coop/updates/
* Email: `hello@catalyst.coop <mailto:hello@catalyst.coop>`__
* Twitter: `@CatalystCoop <https://twitter.com/CatalystCoop>`__
