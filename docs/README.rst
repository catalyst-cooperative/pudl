
The Public Utility Data Liberation Project
============================================
.. image:: https://travis-ci.org/catalyst-cooperative/pudl.svg?branch=master
   :target: https://travis-ci.org/catalyst-cooperative/pudl
   :alt: Build Status

.. image:: https://codecov.io/gh/catalyst-cooperative/pudl/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/catalyst-cooperative/pudl
   :alt: codecov

.. image:: https://api.codacy.com/project/badge/Grade/2fead07adef249c08288d0bafae7cbb5
   :target: https://app.codacy.com/app/zaneselvans/pudl
   :alt: Codacy Badge

.. image:: https://readthedocs.org/projects/catalyst-cooperative-pudl/badge/?version=latest
   :target: https://catalyst-cooperative-pudl.readthedocs.io/en/latest/
   :alt: Documentation Status

.. image:: https://badges.gitter.im/catalyst-cooperative/pudl.svg
   :target: https://gitter.im/catalyst-cooperative/pudl
   :alt: Join the chat at https://gitter.im/catalyst-cooperative/pudl

.. image:: https://mybinder.org/badge.svg
   :target: https://mybinder.org/v2/gh/catalyst-cooperative/pudl/master
   :alt: Binder

.. readme-intro

The `Public Utility Data Liberation project <https://catalyst.coop/pudl/>`__
(PUDL) provides programmatic access to publicly available US energy system
data. Most of this data is published by public agencies, but in formats that
are not immediately usable or interoperable. PUDL combines information from
various spreadsheets, CSV files, and databases into a collection of platform
independent JSON/CSV based `tabular data packages
<https://https://frictionlessdata.io/docs/tabular-data-package/>`__ that can be
used to create a database, or read directly with Python, R, Microsoft Access,
and many other data analysis tools.

PUDL brings together information from the following sources:

* `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`__
* `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`__
* `The EPA Continuous Emissions Monitoring System (CEMS) <https://ampd.epa.gov/ampd/>`__
* `The EPA Integrated Planning Model (IPM) <https://www.epa.gov/airmarkets/national-electric-energy-data-system-needs-v6>`__
* `FERC Form 1 <https://www.ferc.gov/docs-filing/forms/form-1/data.asp>`__

The project aims especially to serve researchers, activists, journalists, and
policy makers that might not otherwise have easy access to this kind of data
via commercial data providers.

Getting Started
^^^^^^^^^^^^^^^

Just want to play with some example data? Install
`Anaconda <https://www.anaconda.com/distribution/>`__
(or `miniconda <https://docs.conda.io/en/latest/miniconda.html>`__
if you like the command line) with at least Python 3.7. Then run the following
commands in your terminal:

.. code-block:: console

    $ pip install catalyst.pudl
    $ pudl_setup --pudl_dir=pudl_workspace
    $ cd pudl_workspace
    $ conda env create --name pudl --file environment.yml
    $ conda activate pudl
    $ pudl_etl settings/pudl_etl_example.yml
    $ jupyter-lab --notebook-dir=notebooks

This will install the PUDL Python packages, create some local directories
inside a directory called ``pudl_workspace``, download the most recent year of
data from the public agencies, process it into `tabular data packages
<https://frictionlessdata.io/docs/tabular-data-package/>`__, and open up a
`Jupyter <https://jupyter.org>`__ notebook in your web browser with some
examples showing how to work with that data.

.. Note::

    To run the example above you'll need a computer with at least **4 GB of
    RAM** and **10 GB of free disk space**. You will also need to download
    about **500 MB of data**. This could take a while if you have a slow
    internet connection.

For more details, see `the full PUDL documentation
<https://catalyst-cooperative-pudl.readthedocs.io/>`__.


Contributing to PUDL
^^^^^^^^^^^^^^^^^^^^

Find PUDL useful? Want to help make it better? There are lots of ways to
contribute!

* Please be sure to read our `Code of Conduct <https://catalyst-cooperative-pudl.readthedocs.io/en/latest/CODE_OF_CONDUCT.html>`__
* You can file a bug report, make a feature request, or ask questions in the
  `Github issue tracker
  <https://github.com/catalyst-cooperative/pudl/issues>`__.
* Feel free to fork the project and make a pull request with new code,
  better documentation, or example notebooks.
* Make a donation to support our work liberating public energy data.
* For more information checkout our `Contribution Guidelines
  <https://catalyst-cooperative-pudl.readthedocs.io/en/latest/CONTRIBUTING.html>`__

Licensing
^^^^^^^^^

The PUDL software is released under the `MIT License
<https://opensource.org/licenses/MIT>`__. `The PUDL documentation
<https://catalyst-cooperative-pudl.readthedocs.io>`__ is released under the
`Creative Commons Attribution 4.0 License
<https://creativecommons.org/licenses/by/4.0/>`__.

Contact Us
^^^^^^^^^^

For usage questions, bug reports, support requests, suggestions to make PUDL
better and anything else that could conceivably be of use to the broader
community of users, we ask you to please use the `Github issue tracker
<https://github.com/catalyst-cooperative/pudl/issues>`__.

* Project Email: `pudl@catalyst.coop <mailto:pudl@catalyst.coop>`__
* Gitter Chat: https://gitter.im/catalyst-cooperative/pudl


About Catalyst Cooperative
^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

* Website: https://catalyst.coop
* Newsletter: https://catalyst.coop/updates/
* Twitter: `@CatalystCoop <https://twitter.com/CatalystCoop>`__
