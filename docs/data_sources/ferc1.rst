===============================================================================
FERC Form 1
===============================================================================

=================== ===========================================================
Source URL          https://www.ferc.gov/industries-data/electric/general-information/electric-industry-forms/form-1-electric-utility-annual
Source Description  | Financial and operational information from electric utilities,
                    | licensees and others entities subject to FERC jurisdiction.
Respondents         | Major electric utilities and licensees
Source Format       FoxPro Database (.DBC/.DBF)
Source Years        1994-2018
Size (Download)     1.4 GB
Size (Uncompressed) 10 GB
PUDL Code           ``ferc1``
Years Liberated     1994-2018
Records Liberated   ~12 million (116 raw tables), ~280,000 (7 clean tables)
Issues              `Open FERC Form 1 issues <https://github.com/catalyst-cooperative/pudl/issues?q=is%3Aissue+is%3Aopen+label%3Aferc1>`__
=================== ===========================================================

Background
^^^^^^^^^^

The *Electric Utility Annual Report*, otherwise known as FERC Form 1, consists
of 116 data tables containing ~8GB of financial and operating data for major
utilities and licensees.

**Who is required to fill out the form?**

FERC Form 1 is mandatory under the Federal Power Act, Sections 3, 4(a), 304 and
309, and ﻿18 CFR 141.1 and 141.400.

As outlined in the Commission's Uniform System of Accounts Prescribed for Public
Utilities and Licensees Subject To the Provisions of The Federal Power Act (18 C.F.R.
Part 101), to qualify as a respondent, entities must exceed at least one of the
following criteria for three consecutive years prior to reporting:

  * 1 million MWh of total sales
  * 100MWh of annual sales for resale
  * 500MWh of annual power exchanges delivered
  * 500MWh of annual wheeling for others (deliveries plus losses)

Annual responses are due by April 13th. Failure to submit a report can result in
fines and other civil penalties or sanctions.

**What does the published data look like?**

The data are published as separate annual FoxPro databases for the years
1994-2018.

Further information about Form 1 and its contents can be found by looking at the
`Form 1 Template <https://www.ferc.gov/sites/default/files/2020-04/form-1.pdf>`__.

Notable Irregularities
^^^^^^^^^^^^^^^^^^^^^^

Raw Data
^^^^^^^^

PUDL Data Transformations
^^^^^^^^^^^^^^^^^^^^^^^^^

PUDL can extract all of those tables and load them into a single SQLite database
together (See :doc:`Cloning FERC Form 1 <../dev/clone_ferc1>`). Thus far we have
only integrated 7 of those tables into the full PUDL ETL pipeline. Mostly we
focused on tables pertaining to power plants, their capital & operating
expenses, and fuel consumption. However, we have the tools required to pull
just about any other table in as well.

We continue to improve the integration between the FERC Form 1 plants and the
EIA plants and generators, many of which represent the same utility assets.
Over time we will pull in and clean up additional FERC Form 1 tables. If
there's data you need from Form 1 in bulk you can
`hire us <https://catalyst.coop/hire-catalyst/>`__ to liberate it first.
