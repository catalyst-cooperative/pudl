===============================================================================
The Public Utility Data Liberation Project (PUDL)
===============================================================================

.. readme-intro

.. image:: https://www.repostatus.org/badges/latest/active.svg
   :target: https://www.repostatus.org/#active
   :alt: Project Status: Active – The project has reached a stable, usable state and is being actively developed.

.. image:: https://github.com/catalyst-cooperative/pudl/workflows/tox-pytest/badge.svg
   :target: https://github.com/catalyst-cooperative/pudl/actions?query=workflow%3Atox-pytest
   :alt: Tox-PyTest Status

.. image:: https://img.shields.io/readthedocs/catalystcoop-pudl
   :target: https://catalystcoop-pudl.readthedocs.io/en/latest/
   :alt: Read the Docs Build Status

.. image:: https://img.shields.io/codecov/c/github/catalyst-cooperative/pudl
   :target: https://codecov.io/gh/catalyst-cooperative/pudl
   :alt: Codecov Test Coverage

.. image:: https://img.shields.io/codacy/grade/2fead07adef249c08288d0bafae7cbb5
   :target: https://app.codacy.com/app/zaneselvans/pudl
   :alt: Codacy Grade

.. image:: https://img.shields.io/pypi/v/catalystcoop.pudl
   :target: https://pypi.org/project/catalystcoop.pudl/
   :alt: PyPI Version

.. image:: https://img.shields.io/conda/vn/conda-forge/catalystcoop.pudl
   :target: https://anaconda.org/conda-forge/catalystcoop.pudl
   :alt: conda-forge Version

.. image:: https://zenodo.org/badge/80646423.svg
   :target: https://zenodo.org/badge/latestdoi/80646423
   :alt: Zenodo DOI

`PUDL <https://catalyst.coop/pudl/>`__ makes US energy data easier to access
and use. Hundreds of gigabytes of information is available from government
agencies, but it's often difficult to work with, and different sources can be
hard to combine. PUDL takes the original spreadsheets, CSV files, and databases
and turns them into unified
`tabular data packages <https://frictionlessdata.io/docs/tabular-data-package/>`__
that can be used to populate a database, or read in directly with Python, R,
Microsoft Access, and many other tools.

The project currently integrates data from:

* `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`__
* `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`__
* `The EPA Continuous Emissions Monitoring System (CEMS) <https://ampd.epa.gov/ampd/>`__
* `The EPA Integrated Planning Model (IPM) <https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6>`__
* `FERC Form 1 <https://www.ferc.gov/docs-filing/forms/form-1/data.asp>`__

The project is focused on serving researchers, activists, journalists, and
policy makers that might not otherwise be able to afford access to this data
from existing commercial data providers. You can sign up for PUDL email updates
`here <https://catalyst.coop/updates/>`__.

Quick Start
-----------

Install
`Anaconda <https://www.anaconda.com/distribution/>`__
or `miniconda <https://docs.conda.io/en/latest/miniconda.html>`__ (see
`this detailed setup guide <https://www.mrdbourke.com/get-your-computer-ready-for-machine-learning-using-anaconda-miniconda-and-conda/>`__
if you need help) and then work through the following commands.

Create and activate a conda environment named ``pudl`` that installs packages
from the community maintained ``conda-forge`` channel. In addition to the
``catalystcoop.pudl`` package, install JupyterLab so we can work with the PUDL
data interactively.

.. code-block:: console

    $ conda create --yes --name pudl --channel conda-forge \
        --strict-channel-priority python=3.7 \
        catalystcoop.pudl jupyter jupyterlab pip
    $ conda activate pudl

Now create a data management workspace called ``pudl-work``. The workspace
has a well defined directory structure that PUDL uses to organize the data it
downloads, processes, and outputs. Run ``pudl_setup --help`` for details.

.. code-block:: console

    $ mkdir pudl-work
    $ pudl_setup pudl-work

Now that we have some raw data, we can run the PUDL ETL (Extract, Transform,
Load) pipeline to clean it up and integrate it together. There are several
steps:

* Cloning the FERC Form 1 database into SQLite
* Extracting data from that database and other sources and cleaning it up
* Outputting the clean data into CSV/JSON based data packages, and finally
* Loading the data packages into a local database or other storage medium.

PUDL provides a script to clone the FERC Form 1 database. The script is called
``ferc1_to_sqlite`` and it is controlled by a YAML file. An example can be
found in the settings folder:

.. code-block:: console

    $ ferc1_to_sqlite pudl-work/settings/ferc1_to_sqlite_example.yml

The main ETL process is controlled by another YAML file defining the data that
will be processed. A well commented ``etl_example.yml`` can also be found
in the ``settings`` directory of the PUDL workspace you set up. The script that
runs the ETL process is called ``pudl_etl``:

.. code-block:: console

    $ pudl_etl pudl-work/settings/etl_example.yml

This generates a bundle of tabular data packages in
``pudl-work/datapkg/pudl-example``

Tabular data packages are made up of CSV and JSON files. They're relatively
easy to parse programmatically, and readable by humans. They are also well
suited to archiving, citation, and bulk distribution, but they are static.

To make the data easier to query and work with interactively, we typically load
it into a local SQLite database using this script, which first combines several
data packages from the same bundle into a single data package,

.. code-block:: console

    $ datapkg_to_sqlite \
        -o pudl-work/datapkg/pudl-example/pudl-merged \
        pudl-work/datapkg/pudl-example/ferc1-example/datapackage.json \
        pudl-work/datapkg/pudl-example/eia-example/datapackage.json \
        pudl-work/datapkg/pudl-example/epaipm-example/datapackage.json

The EPA CEMS data is ~100 times larger than all of the other data we have
integrated thus far, and loading it into SQLite takes a very long time. We've
found the most convenient way to work with it is using
`Apache Parquet <https://parquet.apache.org>`__ files, and have a script that
converts the EPA CEMS Hourly table from the generated datapackage into that
format. To convert the example EPA CEMS data package you can run:

.. code-block:: console

    $ epacems_to_parquet pudl-work/datapkg/pudl-example/epacems-eia-example/datapackage.json

The resulting Apache Parquet dataset will be stored in
``pudl-work/parquet/epacems`` and will be partitioned by year and by state, so
that you can read in only the relevant portions of the dataset. (Though in the
example, you'll only find 2018 data for Idaho)

Now that you have a live database, we can easily work with it using a variety
of tools, including Python, pandas dataframes, and
`Jupyter Notebooks <https://jupyter.org>`__. This command will start up a local
Jupyter notebook server, and open a notebook containing some simple PUDL usage
examples, which is distributed with the Python package, and deployed into your
workspace:

.. code-block:: console

    $ jupyter lab pudl-work/notebook/pudl_intro.ipynb

For more usage and installation details, see
`our more in-depth documentation <https://catalystcoop-pudl.readthedocs.io/>`__
on Read The Docs.

Contributing to PUDL
--------------------

Find PUDL useful? Want to help make it better? There are lots of ways to
contribute!

* Please be sure to read our `Code of Conduct <https://catalystcoop-pudl.readthedocs.io/en/latest/CODE_OF_CONDUCT.html>`__
* You can file a bug report, make a feature request, or ask questions in the
  `Github issue tracker <https://github.com/catalyst-cooperative/pudl/issues>`__.
* Feel free to fork the project and make a pull request with new code,
  better documentation, or example notebooks.
* `Make a recurring financial contribution <https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PZBZDFNKBJW5E&source=url>`__ to support
  our work liberating public energy data.
* `Hire us to do some custom analysis <https://catalyst.coop/hire-catalyst/>`__
  and allow us to integrate the resulting code into PUDL.
* For more information check out our `Contribution Guidelines <https://catalystcoop-pudl.readthedocs.io/en/latest/CONTRIBUTING.html>`__

Licensing
---------

The PUDL software is released under the
`MIT License <https://opensource.org/licenses/MIT>`__.
`The PUDL documentation <https://catalystcoop-pudl.readthedocs.io>`__
and the data packages we distribute are released under the
`CC-BY-4.0 <https://creativecommons.org/licenses/by/4.0/>`__ license.

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
integrate public data and perform custom analyses to inform public policy. Our
focus is primarily on mitigating climate change and improving electric utility
regulation in the United States.

Do you work on renewable energy or climate policy? Have you found yourself
scraping data from government PDFs, spreadsheets, websites, and databases,
without getting something reusable? We build tools to pull this kind of
information together reliably and automatically so you can focus on your real
work instead — whether that’s political advocacy, energy journalism, academic
research, or public policymaking.

* Web: https://catalyst.coop
* Newsletter: https://catalyst.coop/updates/
* Email: `hello@catalyst.coop <mailto:hello@catalyst.coop>`__
* Twitter: `@CatalystCoop <https://twitter.com/CatalystCoop>`__
