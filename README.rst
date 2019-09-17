===============================================================================
The Public Utility Data Liberation Project (PUDL)
===============================================================================

.. readme-intro

.. image:: https://www.repostatus.org/badges/latest/active.svg
   :target: https://www.repostatus.org/#active
   :alt: Project Status: Active – The project has reached a stable, usable state and is being actively developed.

.. image:: https://img.shields.io/travis/catalyst-cooperative/pudl
   :target: https://travis-ci.org/catalyst-cooperative/pudl
   :alt: Travis CI Build Status

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
and work with. Hundreds of gigabytes of public information is published
by government agencies, but in many different formats that make it hard to
work with and combine. PUDL takes these spreadsheets, CSV files, and databases
and turns them into easy use
`tabular data packages <https://https://frictionlessdata.io/docs/tabular-data-package/>`__
that can populate a database, or be used directly with Python, R, Microsoft
Access, and many other tools.

The project currently integrates data from:

* `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`__
* `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`__
* `The EPA Continuous Emissions Monitoring System (CEMS) <https://ampd.epa.gov/ampd/>`__
* `The EPA Integrated Planning Model (IPM) <https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6>`__
* `FERC Form 1 <https://www.ferc.gov/docs-filing/forms/form-1/data.asp>`__

The project is especially meant to serve researchers, activists, journalists,
and policy makers that might not otherwise be able to afford access to this
data from existing commercial data providers.

Getting Started
---------------

Just want to play with some example data? Install
`Anaconda <https://www.anaconda.com/distribution/>`__
(or `miniconda <https://docs.conda.io/en/latest/miniconda.html>`__) with at
least Python 3.7. Then work through the following commands.

First, we create and activate conda environment named ``pudl``. All the
required packages are available from the community maintained ``conda-forge``
channel, and that channel is given priority, to simplify satisfying
dependencies. Note that PUDL currently requires Python 3.7, the most recently
available major version of Python. In addition to the ``catalystcoop.pudl``
package, we'll also install JupyterLab so we can work with the PUDL data
interactively.

.. code-block:: console

    $ conda create -y -n pudl -c conda-forge --strict-channel-priority python=3.7 catalystcoop.pudl jupyter jupyterlab pip
    $ conda activate pudl

Now we create a data management workspace called ``pudl-work`` and download
some data. The workspace is a well defined directory structure that PUDL uses
to organize the data it downloads, processes, and outputs. You can run
``pudl_setup --help`` and ``pudl_data --help`` for more information.

.. code-block:: console

    $ mkdir pudl-work
    $ pudl_setup pudl-work
    $ pudl_data --sources eia923 eia860 ferc1 epacems epaipm --years 2017 --states id

Now that we have the original data as published by the federal agencies, we can
run the ETL (Extract, Transform, Load) pipeline, that turns the raw data into
an well organized, standardized bundle of data packages. This involves a couple
of steps: cloning the FERC Form 1 into an SQLite database, extracting data from
that database and all the other sources and cleaning it up, outputting that
data into well organized CSV/JSON based data packages, and finally loading
those data packages into a local database.

PUDL provides a script to clone the FERC Form 1 database, controlled by a YAML
file which you can find in the settings folder. Run it like this:

.. code-block:: console

    $ ferc1_to_sqlite pudl-work/settings/ferc1_to_sqlite_example.yml

The main ETL process is controlled by the YAML file ``etl_example.yml`` which
defines what data will be processed. It is well commented -- if you want to
understand what the options are, open it in a text editor, and create your own
version.

The data packages will be generated in a sub-directory in
``pudl-work/datapackage`` named ``pudl-example`` (you can change this by
changing the value of ``pkg_bundle_name`` in the ETL settings file you're
using. Run the ETL pipeline with this command:

.. code-block:: console

    $ pudl_etl pudl-work/settings/etl_example.yml

The generated data packages are made up of CSV and JSON files. They're both
easy to parse programmatically, and readable by humans. They are also well
suited to archiving, citation, and bulk distribution. However, to make the
data easier to query and work with interactively, we typically load it into a
local SQLite database using this script, which combines several data packages
from the same bundle into a single unified structure:

.. code-block:: console

    $ datapkg_to_sqlite --pkg_bundle_name pudl-example

Now that we have a live database, we can easily work with it using a variety
of tools, including Python, pandas dataframes, and
`Jupyter notebooks <https://jupyter.org>`__. This command will start up a local
Jupyter notebook server, and open a notebook of PUDL usage examples:

.. code-block:: console

    $ jupyter lab pudl-work/notebook/pudl_intro.ipynb

For more details, see `the full PUDL documentation
<https://catalystcoop-pudl.readthedocs.io/>`__ on Read The Docs.

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
* `Make a recurring financial contribution <https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PZBZDFNKBJW5E&source=url>`__ to support
  our work liberating public energy data.
* Hire us to do some custom analysis, and let us add the code the project.
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
