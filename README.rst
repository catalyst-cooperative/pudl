===============================================================================
The Public Utility Data Liberation Project (PUDL)
===============================================================================

.. readme-intro

.. image:: https://www.repostatus.org/badges/latest/active.svg
   :target: https://www.repostatus.org/#active
   :alt: Project Status: Active

.. image:: https://github.com/catalyst-cooperative/pudl/workflows/pytest/badge.svg
   :target: https://github.com/catalyst-cooperative/pudl/actions?query=workflow%3Apytest
   :alt: PyTest Status

.. image:: https://img.shields.io/codecov/c/github/catalyst-cooperative/pudl?style=flat&logo=codecov
   :target: https://codecov.io/gh/catalyst-cooperative/pudl
   :alt: Codecov Test Coverage

.. image:: https://img.shields.io/readthedocs/catalystcoop-pudl?style=flat&logo=readthedocs
   :target: https://catalystcoop-pudl.readthedocs.io/en/latest/
   :alt: Read the Docs Build Status

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

Who is PUDL for?
----------------

The project is focused on serving researchers, activists, journalists, policy makers,
and small businesses that might not otherwise be able to afford access to this data from
commercial sources and who may not have the time or expertise to do all the data
processing themselves from scratch.

We want to make this data accessible and easy to work with for as wide an audience as
possible: anyone from a grassroots youth climate organizers working with Google sheets
to university researchers with access to scalable cloud computing resources and everyone
in between!

What data is available?
-----------------------

PUDL currently integrates data from:

* `EIA Form 860 <https://www.eia.gov/electricity/data/eia860/>`__: 2001 - 2022
* `EIA Form 860m <https://www.eia.gov/electricity/data/eia860m/>`__: 2023-06
* `EIA Form 861 <https://www.eia.gov/electricity/data/eia861/>`__: 2001 - 2022
* `EIA Form 923 <https://www.eia.gov/electricity/data/eia923/>`__: 2001 - 2023-08
* `EPA Continuous Emissions Monitoring System (CEMS) <https://campd.epa.gov/>`__: 1995 - 2022
* `FERC Form 1 <https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual>`__: 1994-2021
* `FERC Form 714 <https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-no-714-annual-electric/data>`__: 2006-2020
* `US Census Demographic Profile 1 Geodatabase <https://www.census.gov/geographies/mapping-files/2010/geo/tiger-data.html>`__: 2010

Thanks to support from the `Alfred P. Sloan Foundation Energy & Environment
Program <https://sloan.org/programs/research/energy-and-environment>`__, from
2021 to 2024 we will be integrating the following data as well:

* `EIA Form 176 <https://www.eia.gov/dnav/ng/TblDefs/NG_DataSources.html#s176>`__
  (The Annual Report of Natural Gas Supply and Disposition)
* `FERC Electric Quarterly Reports (EQR) <https://www.ferc.gov/industries-data/electric/power-sales-and-markets/electric-quarterly-reports-eqr>`__
* `FERC Form 2 <https://www.ferc.gov/industries-data/natural-gas/overview/general-information/natural-gas-industry-forms/form-22a-data>`__
  (Annual Report of Major Natural Gas Companies)
* `PHMSA Natural Gas Annual Report <https://www.phmsa.dot.gov/data-and-statistics/pipeline/gas-distribution-gas-gathering-gas-transmission-hazardous-liquids>`__
* Machine Readable Specifications of State Clean Energy Standards

How do I access the data?
-------------------------

For details on how to access PUDL data, see the `data access documentation
<https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html>`__. A quick
summary:

* `Datasette <https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#-access-datasette>`__
  provides browsable and queryable data from our nightly builds on the web:
  https://data.catalyst.coop
* `Kaggle <https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#access-kaggle>`__
  provides easy Jupyter notebook access to the PUDL data, updated weekly:
  https://www.kaggle.com/datasets/catalystcooperative/pudl-project
* `Zenodo <https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#access-zenodo>`__
  provides stable long-term access to our versioned data releases with a citeable DOI:
  https://doi.org/10.5281/zenodo.3653158
* `Nightly Data Builds <https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#access-nightly-builds>`__
  push their outputs to the AWS Open Data Registry:
  https://registry.opendata.aws/catalyst-cooperative-pudl/
  See `the nightly build docs <https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#access-nightly-builds>`__
  for direct download links.
* `The PUDL Development Environment <https://catalystcoop-pudl.readthedocs.io/en/latest/dev/dev_setup.html>`__
  lets you run the PUDL data processing pipeline locally.

Contributing to PUDL
--------------------

Find PUDL useful? Want to help make it better? There are lots of ways to help!

* Check out our `contribution guide <https://catalystcoop-pudl.readthedocs.io/en/latest/CONTRIBUTING.html>`__
  including our `Code of Conduct <https://catalystcoop-pudl.readthedocs.io/en/latest/code_of_conduct.html>`__.
* You can file a bug report, make a feature request, or ask questions in the
  `Github issue tracker <https://github.com/catalyst-cooperative/pudl/issues>`__.
* Feel free to fork the project and make a pull request with new code, better
  documentation, or example notebooks.
* `Make a recurring financial contribution <https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PZBZDFNKBJW5E&source=url>`__
  to support our work liberating public energy data.
* `Hire us to do some custom analysis <https://catalyst.coop/hire-catalyst/>`__ and
  allow us to integrate the resulting code into PUDL.

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
* `Follow us here on GitHub <https://github.com/catalyst-cooperative/>`__
* Follow us on Mastodon: `@CatalystCoop@mastodon.energy <https://mastodon.energy/@CatalystCoop>`__
* Follow us on BlueSky:  `@catalyst.coop <https://bsky.app/profile/catalyst.coop>`__
* `Follow us on LinkedIn <https://www.linkedin.com/company/catalyst-cooperative/>`__
* `Follow us on HuggingFace <https://huggingface.co/catalystcooperative>`__
* Follow us on Twitter: `@CatalystCoop <https://twitter.com/CatalystCoop>`__
* `Follow us on Kaggle <https://www.kaggle.com/catalystcooperative/>`__
* More info on our website: https://catalyst.coop
* Email us if you'd like to hire us to provide customized data extraction and analysis:
  `hello@catalyst.coop <mailto:hello@catalyst.coop>`__

About Catalyst Cooperative
--------------------------

`Catalyst Cooperative <https://catalyst.coop>`__ is a small group of data wranglers
and policy wonks organized as a worker-owned cooperative consultancy. Our goal is a
more just, livable, and sustainable world. We integrate public data and perform
custom analyses to inform public policy
(`Hire us! <https://catalyst.coop/hire-catalyst>`__). Our focus is primarily on
mitigating climate change and improving electric utility regulation in the United
States.
