=======================================================================================
PUDL Release Notes
=======================================================================================

---------------------------------------------------------------------------------------
v2025.XX.x (2025-MM-DD)
---------------------------------------------------------------------------------------

New Data
^^^^^^^^

Expanded Data Coverage
^^^^^^^^^^^^^^^^^^^^^^

Quality of Life Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Bug Fixes
^^^^^^^^^

.. _release-v2025.7.0:

---------------------------------------------------------------------------------------
v2025.7.0 (2025-07-03)
---------------------------------------------------------------------------------------

This release integrates early release annual 2024 data for the EIA Forms 860 and 923,
as well as fresh EIA 860M monthly data. It also includes a few small bug-fixes, some of
which result in minor changes to the database schema. It also removes the deprecated
``PudlTabl`` output management class.

We are experimenting a new **monthly** release schedule for PUDL, to keep the EIA 860M
data as fresh as possible. This is the first of those monthly releases.

New Data
^^^^^^^^

EIA AEO
~~~~~~~

* Extracted table 2 from the EIA Annual Energy Outlook 2023, which includes future
  projections for energy use through the year 2050 across a variety of scenarios.
  Integrated a subset of available table 2 series as a new core table:

  * ``core_eiaaeo__yearly_projected_energy_use_by_sector_and_type`` contains
    projected energy use for the commercial, electric power, industrial,
    residential, and transportation sectors across different fuels and electricity
    modes. See :issue:`4228` and :pr:`4273`.

Expanded Data Coverage
^^^^^^^^^^^^^^^^^^^^^^

EIA 860
~~~~~~~
* Added EIA 860 early release data from 2024. See :issue:`4323` and PR :pr:`4332`.

EIA 860M
~~~~~~~~
* Added EIA 860M data from April 2025. See :issue:`4324` and PR :pr:`4332`.

EIA 923
~~~~~~~
* Added EIA 923 early release data from 2024 and monthly data from March 2025. See
  :issue:`4325` and PR :pr:`4332`.

Bug Fixes
^^^^^^^^^

* Fixed a number of typos in our documentation and codebase, which resulted in
  renaming ``synchronized_transmission_grid`` in :ref:`core_eia860__scd_generators`,
  :ref:`out_eia__monthly_generators`, and :ref:`out_eia__yearly_generators`.
  See issue :issue:`3783` and :pr:`4355`.

VCE RARE
~~~~~~~~
* Standardized ``place_name`` using data from the latest Census PEP vintage,
  found in ``_core_censuspep__yearly_geocodes``. See issue :issue:`3914` and PR
  :pr:`4319`.

Deprecations
^^^^^^^^^^^^

* After more than a year of deprecation warnings, we've removed the ``PudlTabl``
  output management class, and have stopped distributing a handful of tables that were
  only around to allow the behavior of that class to be maintained. See issues
  :issue:`3215,2911` and PR :pr:`4316`.
* Undeploy superset, given that we are going with Marimo for our usage metrics
  dashboards, and the Eel Hole for publicly facing data access. See PR :pr:`4353`.

Quality of Life Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We've added a new sub-command to ``dbt_helper`` - ``dbt_helper validate``.
  This lets you run validation tests for a selection of DBT models and also
  see what the failing outputs are, instead of doing a bunch of digging after
  the fact.
* We've added a new devtool in ``devtools/materialize_to_parquet.py`` - this
  lets you export and share assets that were previously not persisted to Parquet,
  such as ``raw`` assets that have been extracted but not cleaned. Run
  ``./materialize_to_parquet --help`` from within the ``devtools`` directory for
  details. See :pr:`4320`.

New Tests
^^^^^^^^^
* Added a validation pipeline for our EIA 930 hourly demand imputation. This
  pipeline will perform imputation on a set of values which did not require imputation,
  so there is ground truth data to compare against. It will then compute the percent
  error for all of these imputed values against the reported data. This metric is
  checked during nightly builds and will result in an error if it ever drifts too high.

.. _release-v2025.5.0:

---------------------------------------------------------------------------------------
v2025.5.0 (2025-05-20)
---------------------------------------------------------------------------------------

This is our regular quarterly PUDL data release for 2025Q2. It includes sub-annual
updates to the EIA-860M, EIA-923, EIA-930, EIA bulk electricity API, and EPA CEMS
datasets. It also includes preliminary 2024 data for FERC Form 1 (integrated into PUDL)
and FERC Forms 2, 6, and 60 (as stand-alone SQLite databases). The VCE RARE hourly
county-level renewable energy generation curves have been extended back to cover
2014-2018.

This release also includes new imputed versions of the FERC-714 and EIA-930 hourly
demand curves with missing values filled in and a better organized version of the SEC
10-K company ownership data. Note that work on the demand imputations and SEC 10-K data
is ongoing.

All federal data was archived from the publishing agencies on May 1st, 2025.

Upcoming Deprecations
^^^^^^^^^^^^^^^^^^^^^

* Due to the growing size of PUDL database, we are no longer updating our `Datasette
  deployment <https://data.catalyst.coop>`__ and that URL will soon begin redirecting
  users to the `PUDL Data Viewer <https://viewer.catalyst.coop>`__. You can track our
  progress toward feature parity with the old Datasette deployment in
  `this issue <https://github.com/catalyst-cooperative/eel-hole/issues/36>`__.
* When we complete the migration of our data validation tests to the ``dbt`` framework,
  we will remove the deprecated :class:`pudl.output.pudltabl.PudlTabl` output class.
  This will also happen before our next quarterly release.

New Data
^^^^^^^^

FERC 714
~~~~~~~~
* We refactored our timseries imputation functions to be more generalized and reusable,
  so they can be applied to electricity demand curves from both FERC-714 and EIA-930,
  as well as other time series data in the future. This resulted in some minor changes
  to the imputation results. See issue :issue:`4112` and PR :pr:`4113`.
* Added the table :ref:`out_ferc714__hourly_planning_area_demand`, which contains an
  imputed version of demand. Previously these imputed values were not being distributed
  directly, and fed into the :ref:`out_ferc714__hourly_estimated_state_demand` table.

EIA 930
~~~~~~~
Work on producing EIA 930 demand curves suitable for use in electricity system modeling
is being done in collaboration with :user:`awongel` at
`Carnegie Science <https://carnegiescience.edu>`__, with support from `GridLab
<https://gridlab.org>`__. See issue :issue:`4083` for a list of related issues.

* Added the table :ref:`out_eia930__hourly_subregion_demand`, which
  contains an imputed version of subregion demand. See issues :issue:`4124,4136` and PR
  :pr:`4149`
* Added the table :ref:`out_eia930__hourly_operations`, which
  contains an imputed version of BA level demand. See issue :issue:`4138` and PR
  :pr:`4162`

SEC 10-K
~~~~~~~~
* Reorganized the preliminary SEC 10-K data that was integrated into our last release.
  See issue :issue:`4078` and PR :pr:`4134`. The SEC 10-K tables are now more fully
  normalized and better conform to existing PUDL naming conventions. Overall revision of
  the SEC 10-K data is being tracked in issue :issue:`4085`.

  Note that the SEC 10-K data is still a work in progress, and there are known issues
  that remain to be resolved in the `upstream repository
  <https://github.com/catalyst-cooperative/mozilla-sec-eia>`__ that generates this data.

  The new tables include:

  * :ref:`core_sec10k__quarterly_filings`
  * :ref:`core_sec10k__quarterly_company_information`
  * :ref:`core_sec10k__changelog_company_name`
  * :ref:`core_sec10k__quarterly_exhibit_21_company_ownership`
  * :ref:`core_sec10k__assn_sec10k_filers_and_eia_utilities`
  * :ref:`out_sec10k__quarterly_filings`
  * :ref:`out_sec10k__changelog_company_name`

Expanded Data Coverage
^^^^^^^^^^^^^^^^^^^^^^

FERC Form 1
~~~~~~~~~~~
* Integrated FERC Form 1 data from 2024 into the main PUDL SQLite DB. See issue
  :issue:`4207` and PR :pr:`4215`. FERC Form 1 has a filing deadline of
  `April 18th <https://www.ferc.gov/general-information-0/electric-industry-forms>`__
  for utility respondents, but late filings may come throughout the year. This update
  includes ~95% of the expected utility responses for 2024.

FERC Forms 2, 6, & 60
~~~~~~~~~~~~~~~~~~~~~
* Updated the FERC archive DOIs and ``ferc_to_sqlite`` settings to extract 2024 XBRL
  data for FERC Forms 2, 6, and 60 and add them to their respective SQLite databases.
  Note that this data is not yet being processed beyond the conversion from XBRL to
  SQLite. See PR :pr:`4250`. The reporting deadline for these forms was April 18th, 2025
  so they should include the vast bulk of the expected data, however there may be some
  late filings which will be added in the next quarterly release.

EIA Bulk Electricity
~~~~~~~~~~~~~~~~~~~~
* Updated the EIA Bulk Electricity data to include data published up through
  2025-05-01. Also adapted the extractor to handle changes in formatting for the
  EIA Bulk API archive. See :issue:`4237` and PR :pr:`4246`.

EPA CEMS
~~~~~~~~
* Added 2025 Q1 of CEMS data. See :issue:`4236` and :pr:`4238`.

EIA 930
~~~~~~~~
* Updated EIA 930 to include data published up through the beginning of May 2025.
  See :issue:`4235` and :pr:`4242`. Raw data now includes adjusted and imputed
  values for the ``unknown`` fuel source, making it behave like other fuel sources;
  see :ref:`data-sources-eia930-changes-in-energy-source-granularity-over-time` for
  more information.

EIA 860M
~~~~~~~~
* Added EIA 860M data from January, February, and March 2025. See :issue:`4233` and
  PR :pr:`4242`.

EIA 923
~~~~~~~
* Added EIA 923 from January and February 2025. See :issue:`4234` and PR :pr:`4242`.

VCE RARE
~~~~~~~~
* Integrated 2014-2018 RARE data into PUDL. Also fixed misleading latitude and longitude
  field descriptions, and renamed the field ``county_or_lake_name`` to ``place_name``.
  See issue :issue:`4226` and PR :pr:`4239`.

Bug Fixes
^^^^^^^^^

* Fixed a bug in FERC XBRL extraction that led to quietly skipping tables with names
  that didn't conform to expected format. The only known table affected was in the FERC
  Form 6. See issue :issue:`4203` and PRs :pr:`4224` and
  `catalyst-cooperative/ferc-xbrl-extractor #320 <https://github.com/catalyst-cooperative/ferc-xbrl-extractor/pull/320>`__.
* As part of :pr:`4215` we fixed a bug introduced in the last release that was causing
  most values in the ``out_ferc1__yearly_rate_base`` table to be dropped. See
  `this commit <https://github.com/catalyst-cooperative/pudl/pull/4215/commits/65b36e3121bdfb792ae59c0b94b0ed473307bd78>`__.

Quality of Life Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now publish a `Frictionless data package
  <https://datapackage.org/standard/data-package/>`__ describing our Parquet
  outputs, with the name ``pudl_datapackage.json``. See :issue:`4069` and :pr:`4070`.
* We renamed ``eia_bulk_elec`` to ``eiaapi`` to conform to our dataset naming protocols
  and reflect the expansion of the EIA Bulk API archive to include all datasets
  published through the EIA API, not just the bulk electricity data. See `this PUDL
  archiver issue <https://github.com/catalyst-cooperative/pudl-archiver/issues/628>`__
  and PR :pr:`4212`.
* To improve human readability, we added ``utility_id_pudl`` and ``utility_name_ferc1``
  columns to a number of derived FERC 1 output tables including:

  * :ref:`out_ferc1__yearly_rate_base`
  * :ref:`out_ferc1__yearly_detailed_income_statements`
  * :ref:`out_ferc1__yearly_detailed_balance_sheet_assets`
  * :ref:`out_ferc1__yearly_detailed_balance_sheet_liabilities`

  See PR :pr:`4260`.

New Tests
^^^^^^^^^

We're in the process of migrating hundrds of data validation tests to use the `dbt
<https://docs.getdbt.com/docs/introduction>`__ framework. We have converted at least the
following classes of tests:

* ``check_column_correlation`` – a more generic replacement for the old
  ``test_fbp_ferc1_mmbtu_cost_correlation`` pytest.
  See :issue:`4094`, :pr:`4117`. You can find the implementation in the
  `check_column_correlation.sql
  <../../dbt/tests/data_tests/generic_tests/check_column_correlation.sql>`__ file.
* ``expect_includes_all_value_combinations_from`` - a more generic replacement for the
  old ``ensure_all_ppe_ids_are_in_assn`` pytest. See :issue:`4096`, :pr:`9123`. You
  can find the implementation in the `expect_includes_all_value_combinations_from.sql
  <../../dbt/tests/data_tests/generic_tests/expect_includes_all_value_combinations_from.sql>`__
  file.
* ``expect_quantile_constraints`` - a more generic replacement for the old
  ``vs_bounds`` pytest. See :issue:`4106`, :pr:`4090`, and :pr:`4171`. You can find the
  implementation in the `expect_quantile_constraints.sql
  <../../dbt/tests/data_tests/generic_tests/expect_quantile_constraints.sql>`__ file.
* 19 tests which required special handling; see :issue:`4093`, :pr:`4114`, :pr:`4151`.

.. _release-v2025.2.0:

---------------------------------------------------------------------------------------
v2025.2.0 (2025-02-13)
---------------------------------------------------------------------------------------

This is our regular quarterly release for 2025Q1. It includes updates to all the
datasets that are published with quarterly or higher frequency, plus initial versions
of a few new data sources that have been in the works for a while.

One major change this quarter is that we are now publishing all processed PUDL data as
Apache Parquet files, alongside our existing SQLite databases. See :doc:`data_access`
for more on how to access these outputs.

Some potentially breaking changes to be aware of:

* In the :doc:`data_sources/eia930` a number of new energy sources have been added, and
  some old energy sources have been split into more granular categories. See
  :ref:`data-sources-eia930-changes-in-energy-source-granularity-over-time`.
* We are now running the EPA's CAMD to EIA unit crosswalk code for each individual year
  starting from 2018, rather than just 2018 and 2021, resulting in more connections
  between these two datasets and changes to some sub-plant IDs. See the note below for
  more details.

Many thanks to the organizations who make these regular updates possible! Especially
`GridLab <https://gridlab.org>`__, and `RMI <https://rmi.org>`__. If you rely on PUDL
and would like to help ensure that the data keeps flowing, please consider joining them
as a `PUDL Sustainer <https://opencollective.com/pudl>`__, as we are still fundraising
for 2025.

New Data
^^^^^^^^

EIA 176
~~~~~~~
* Add a couple of semi-transformed interim EIA-176 (natural gas sources and
  dispositions) tables. They aren't yet being written to the database, but are one step
  closer. See :issue:`3555` and PRs :pr:`3590,3978`. Thanks to :user:`davidmudrauskas`
  for moving this dataset forward.
* Extracted these interim tables up through the latest 2023 data release. See
  :issue:`4002` and :pr:`4004`.

EIA 860
~~~~~~~
* Added EIA 860 Multifuel table. See :issue:`3438` and :pr:`3988`. Thanks to
  :user:`jmelot` for working on adding this new table.

FERC 1
~~~~~~
* Added three new output tables containing granular utility accounting data.
  See :pr:`4057`, :issue:`3642` and the table descriptions in the data dictionary:

  * :ref:`out_ferc1__yearly_detailed_income_statements`
  * :ref:`out_ferc1__yearly_detailed_balance_sheet_assets`
  * :ref:`out_ferc1__yearly_detailed_balance_sheet_liabilities`

SEC Form 10-K Parent-Subsidiary Ownership
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* We have added some new tables describing the parent-subsidiary company ownership
  relationships reported in the
  `SEC's Form 10-K <https://en.wikipedia.org/wiki/Form_10-K>`__, Exhibit 21
  "Subsidiaries of the Registrant". Where possible these tables link the SEC filers or
  their subsidiary companies to the corresponding EIA utilities. This work was funded
  by
  `a grant from the Mozilla Foundation <https://catalyst.coop/2024/02/15/beating-utility-ownership-shell-game/>`__.
  Most of the ML models and data preparation took place in the `mozilla-sec-eia
  repository <https://github.com/catalyst-cooperative/mozilla-sec-eia>`__ separate from
  the main PUDL ETL, as it requires processing hundreds of thousands of PDFs and the
  deployment of some ML experiment tracking infrastructure. The new tables are handed
  off as nearly finished products to the PUDL ETL pipeline. **Note that these are
  preliminary, experimental data products and are known to be incomplete and to contain
  errors.** Extracting data tables from unstructured PDFs and the SEC to EIA record
  linkage are necessarily probabilistic processes.
* See PRs :pr:`4026,4031,4035,4046,4048,4050,4079` and check out the table descriptions
  in the PUDL data dictionary:

  * :ref:`core_sec10k__quarterly_filings`
  * :ref:`core_sec10k__quarterly_exhibit_21_company_ownership`
  * :ref:`core_sec10k__quarterly_company_information`
  * :ref:`core_sec10k__changelog_company_name`

Expanded Data Coverage
^^^^^^^^^^^^^^^^^^^^^^

EPA CEMS
~~~~~~~~
* Added 2024 Q4 of CEMS data. See :issue:`4041` and :pr:`4052`.

EPA CAMD EIA Crosswalk
~~~~~~~~~~~~~~~~~~~~~~
* In the past, the crosswalk in PUDL has used the EPA's published crosswalk (run with
  2018 data), and an additional crosswalk we ran with 2021 EIA 860 data. To ensure that
  the crosswalk reflects updates in both EIA and EPA data, we re-ran the EPA R code
  which generates the EPA CAMD EIA crosswalk with 4 new years of data: 2019, 2020, 2022
  and 2023. Re-running the crosswalk pulls the latest data from the CAMD FACT API, which
  results in some changes to the generator and unit IDs reported on the EPA side of the
  crosswalk, which feeds into the creation of :ref:`core_epa__assn_eia_epacamd`.
* The changes only result in the addition of new units and generators in the EPA data,
  with no changes to matches at the plant level. However, the updates to generator and
  unit IDs have resulted in changes to the subplant IDs - some EIA boilers and
  generators which previously had no matches to EPA data have now been matched to EPA
  unit data, resulting in an overall **reduction** in the number of rows in the
  :ref:`core_epa__assn_eia_epacamd_subplant_ids` table. See issues :issue:`4039`
  and PR :pr:`4056` for a discussion of the changes observed in the course of this
  update.

EIA 860M
~~~~~~~~
* Added EIA 860m through December 2024. See :issue:`4038` and :pr:`4047`.

EIA 923
~~~~~~~
* Added EIA 923 monthly data through September 2024. See :issue:`4038` and :pr:`4047`.

EIA Bulk Electricity Data
~~~~~~~~~~~~~~~~~~~~~~~~~
* Updated the EIA Bulk Electricity data to include data published up through
  2024-11-01. See :issue:`4042` and PR :pr:`4051`.

EIA 930
~~~~~~~
* Updated the EIA 930 data to include data published up through the beginning of
  February 2025. See :issue:`4040` and PR :pr:`4054`. 10 new energy sources
  were added and 3 were retired; see
  :ref:`data-sources-eia930-changes-in-energy-source-granularity-over-time` for
  more information.

Bug Fixes
^^^^^^^^^

* Fix an accidentally swapped set of starting balance / ending balance column rename
  parameters in the pre-2021 DBF derived data that feeds into
  :ref:`core_ferc1__yearly_other_regulatory_liabilities_sched278`. See issue
  :issue:`3952` and PRs :pr:`3969,3979`. Thanks to :user:`yolandazzz13` for making
  this fix.
* Added preliminary data validation checks for several FERC 1 tables that were
  missing it :pr:`3860`.
* Fix spelling of Lake Huron and Lake Saint Clair in
  :ref:`out_vcerare__hourly_available_capacity_factor` and related tables. See issue
  :issue:`4007` and PR :pr:`4029`.

Quality of Life Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* We added a ``sources`` parameter to ``pudl.metadata.classes.DataSource.from_id()``
  in order to make it possible to use the `pudl-archiver
  <https://www.github.com/catalyst-cooperative/pudl-archiver>`__ repository to
  archive datasets that won't necessarily be ingested into PUDL. See `this PUDL archiver
  issue <https://github.com/catalyst-cooperative/pudl-archiver/pull/506>`__ and PRs
  :pr:`4003` and :pr:`4013`.

.. _release-v2024.11.0:

---------------------------------------------------------------------------------------
v2024.11.0 (2024-11-14)
---------------------------------------------------------------------------------------

PUDL v2024.11.0 is a regularly scheduled quarterly release, incorporating a few updates
to the following datasets that have come out since the special release we did in
October.

New Data Coverage
^^^^^^^^^^^^^^^^^

EIA 930
~~~~~~~
* Added EIA 930 hourly data through the end of October as part of the Q3 quarterly
  release. See :issue:`3942` and :pr:`3946`.

EIA 923
~~~~~~~
* Added EIA 923 data from August 2024 as part of the Q3 quarterly release.
  See :issue:`3941` and PR :pr:`3950`.

EIA 860M
~~~~~~~~
* Added 2024 EIA 860m data from August, September, and October as part of the Q3
  quarterly release. See :issue:`3940` and PR :pr:`3949`.

EIA 861
~~~~~~~

* Added final release EIA 861 data. See :issue:`3905` and PR :pr:`3911`.

EIA Bulk Electricity Data
~~~~~~~~~~~~~~~~~~~~~~~~~
* Updated the EIA Bulk Electricity data to include data published up through
  2024-08-01. See :issue:`3944` and PR :pr:`3951`.

EPA CEMS
~~~~~~~~
* Added 2024 Q3 of CEMS data. See :issue:`3943` and :pr:`3948`.

Record Linkage
^^^^^^^^^^^^^^^^^^^^^^^^^^
* Updated the ``splink`` FERC to EIA development notebook to be compatible with
  the latest version of ``splink``. This notebook is not run in production but
  is helpful for visualizing model weights and what is happening under the hood.
* Updated ``pudl.analysis.record_linkage.name_cleaner`` company name cleaning
  module to be more efficient by removing all ``.apply`` and instead use
  ``pd.Series.replace`` to make regex replacement rules vectorized. Also removed
  some of the allowed replacement rules to make the cleaner simpler and more
  effective. This module runs approximately 3x faster now when cleaning a
  string Series.

.. _release-v2024.10.0:

---------------------------------------------------------------------------------------
v2024.10.0 (2024-10-20)
---------------------------------------------------------------------------------------

This is a special early release to publish the new VCE Resource Adequacy Renewable
Energy (RARE) dataset. It also includes final releases of EIA 860 and 923 data for 2023
and the FERC Form 714 data for 2021-2023, which had previously been integrated from
the XBRL data published by FERC. See details below

New Data
^^^^^^^^

Vibrant Clean Energy Resource Adequacy Renewable Energy (RARE) Power Dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Integrate the VCE hourly capacity factor data for solar PV, onshore wind, and
  offshore wind from 2019 through 2023. The data in this table were produced by
  Vibrant Clean Energy, and are licensed to the public under the Creative Commons
  Attribution 4.0 International license (CC-BY-4.0). This data complements the
  WECC-wide GridPath RA Toolkit data currently incorporated into PUDL, providing
  capacity factor data nation-wide with a different set of modeling assumptions and
  a different granularity for the aggregation of outputs.
  See :doc:`data_sources/gridpathratoolkit` and :doc:`data_sources/vcerare` for
  more information.  See :issue:`#3872`.

New Data Coverage
^^^^^^^^^^^^^^^^^

EIA 860
~~~~~~~
* Added EIA 860 final release data from 2023. See :issue:`3684` and PR :pr:`3871`.

EIA 861
~~~~~~~
* Added EIA 861 final release data from 2023. See :issue:`3905` and PR :pr:`3911`. This
  includes a new ``energy_capacity_mwh`` field for battery storage in
  :ref:`core_eia861__yearly_net_metering_customer_fuel_class` and
  :ref:`core_eia861__yearly_non_net_metering_customer_fuel_class`.

EIA 923
~~~~~~~
* Added EIA 923 final release data from 2023 and revised data from 2022. See
  :issue:`3902` and PR :pr:`3903`.

FERC Form 714
~~~~~~~~~~~~~
* Integrated 2021-2023 years of the FERC Form 714 data. FERC updated its reporting
  format for 2021 from a CSV files to XBRL files. This update integrates the two
  raw data sources and extends the data coverage through 2023. See :issue:`3809`
  and :pr:`3842`.

Schema Changes
^^^^^^^^^^^^^^
* Added :ref:`out_eia__yearly_assn_plant_parts_plant_gen` table. This table associates
  records from the :ref:`out_eia__yearly_plant_parts` with ``plant_gen`` records from
  that same plant parts table. See issue :issue:`3773` and PR :pr:`3774`.

Bug Fixes
^^^^^^^^^
* Included more retiring generators in the net generation and fuel consumption
  allocation. Thanks to :user:`grgmiller` for this contribution :pr:`3690`.
* Fixed a bug found in the rolling averages used to impute missing values in
  ``fuel_cost_per_mmbtu`` and to calculate ``capex_annual_addition_rolling``. Thanks
  to RMI for identifying this bug! See issue :issue:`3889` and PR :pr:`3892`.

Major Dependency Updates
^^^^^^^^^^^^^^^^^^^^^^^^
* Updated to use `Numpy v2.0 <https://numpy.org/doc/stable/release/2.0.0-notes.html>`__
  and `Splink v4.0 <https://moj-analytical-services.github.io/splink/blog/2024/07/24/splink-400-released.html>`__.
  See issues :issue:`3736,3735` and PRs :pr:`3547,3834`.

Quality of Life Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* We now use an asset factory to generate Dagster assets for near-identical FERC1 output
  tables. See :issue:`3147` and :pr:`3883`. Thanks to :user:`hfireborn` and
  :user:`denimalpaca` for their work on this one!

.. _release-v2024.8.0:

---------------------------------------------------------------------------------------
v2024.8.0 (2024-08-19)
---------------------------------------------------------------------------------------

This is our regular quarterly release for 2024Q3. It includes quarterly updates to all
datasets that are updated with quarterly or higher frequency by their publishers,
including EIA-860M, EIA-923 (YTD data), EIA-930, the EIA's bulk electricity API data
(used to fill in missing fuel prices), and the EPA CEMS hourly emissions data.

Annual datasets which have been published since our last quarterly release have also
been integrated. These include FERC Forms 1, 2, 6, 60, and 714, and the NREL ATB.

This release also includes provisional versions of the annual 2023 EIA-860 and EIA-923
datasets, whose final release will not happen until the fall.

New Data Coverage
^^^^^^^^^^^^^^^^^

FERC Form 1
~~~~~~~~~~~
* Integrated FERC Form 1 data from 2023 into the main PUDL SQLite DB. See issue
  :issue:`3700` and PR :pr:`3701`. This required updating to a new version of the
  ``catalystcoop.ferc_xbrl_extractor`` package because there are now multiple XBRL
  taxonomies in use by FERC in different years, or even within the same year. See `this
  PR <https://github.com/catalyst-cooperative/ferc-xbrl-extractor/pull/242>`__ for more
  details, as well as issue :issue:`3544` and PR :pr:`3710`.

FERC Forms 2, 6, 60, & 714
~~~~~~~~~~~~~~~~~~~~~~~~~~
* Updated the ``ferc_to_sqlite`` settings to extract 2023 XBRL data for FERC Forms 2, 6
  60, and 714 and add them to their respective SQLite databases. Note that this data
  is not yet being processed beyond the conversion from XBRL to SQLite. See PR
  :pr:`3710`

EIA AEO
~~~~~~~
* Added new tables from EIA AEO table 54:

  * :ref:`core_eiaaeo__yearly_projected_fuel_cost_in_electric_sector_by_type`
    contains fuel costs for the electric power sector. These are broken out by
    fuel type, and include both nominal USD per MMBtu as well as real 2022 USD
    per MMBtu. See issue :issue:`3649` and PR :pr:`3656`.

EIA 860
~~~~~~~
* Added EIA 860 early release data from 2023. This included adding a new tab with
  proposed energy storage generators as well as adding a number of new columns
  regarding energy storage and solar generators. See issue :issue:`3676` and PR
  :pr:`3681`.
* Added EIA 860m data through June 2024. See issue :issue:`3759` and PR :pr:`3767`.

EIA 923
~~~~~~~
* Added EIA 923 early release data from 2023. See :issue:`3719` and PR :pr:`3721`.
* Added EIA 923 monthly data through May as part of the Q2 quarterly release. See
  :issue:`3760` and :pr:`3768`.

EIA 930
~~~~~~~
* Added EIA 930 hourly data through the end of July as part of the Q2 quarterly release.
  See :issue:`3761` and :pr:`3789`.

EPA CEMS
~~~~~~~~
* Added 2024 Q2 of CEMS data. See :issue:`3762` and :pr:`3769`.

EIA Bulk Electricity Data
~~~~~~~~~~~~~~~~~~~~~~~~~

* Updated the EIA Bulk Electricity data archive to include data that was available as of
  2024-08-01, which covers up through 2024-05-01 (3 months more than the previously
  used archive). See :issue:`3763` and PR :pr:`3785`.

FERC 714
~~~~~~~~
* Added :ref:`core_ferc714__yearly_planning_area_demand_forecast` based on FERC
  Form 714, Part III, Schedule 2b. Data includes forecasted demand and net energy load.
  See issue :issue:`3519` and PR :pr:`3670`.
* WIP: Adding XBRL(2021+) data for FERC 714 tables. Track progress in :issue:`3822`.

NREL ATB
~~~~~~~~
* Added 2024 NREL ATB data. This includes adding a new tax credit case,
  ``model_tax_credit_case_nrelatb``, a breakout of ``capex_grid_connection_per_kw`` for
  all technologies, and more detailed nuclear breakdowns of ``fuel_cost_per_mwh``.
  Simultaneously, updated the :mod:`docs.dev.existing_data_updates` documentation to
  make it easier to add future years of data. See :issue:`3706` and :pr:`3719`.
* Updated NREL ATB data to include `error corrections in the 2024 data <https://atb.nrel.gov/electricity/2024/errata>`__.
  See :issue:`3777` and PR :pr:`3778`.

Data Cleaning
^^^^^^^^^^^^^
* When ``generator_operating_date`` values are too inconsistent to be harvested
  successfully, we now take the last reported date in EIA 860 and 860M. See :issue:`423`
  and PR :pr:`3967`.
* Added the ``generator_operating_date`` field into
  :ref:`core_eia860m__changelog_generators`, adding 860M reported generator operating
  dates into the changelog table. This table is not harvested, and thus does not affect
  the ``generator_operating_date`` values reported in other core EIA tables. See
  :issue:`3722` and PR :pr:`3751.`

Bug Fixes
^^^^^^^^^
* Disabled filling of missing values using rolling averages for the
  ``fuel_cost_per_mmbtu`` column in the :ref:`out_eia923__fuel_receipts_costs` table, as
  it was resulting in some anomlously high fuel prices. See :pr:`3716`. This results in
  about 2% more records in the table being left ``NA`` after filling with the average
  prices for that fuel type for the state and month found in the bulk EIA API data.

Quality of Life Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* The full ETL settings are now read directly from ``etl_full.yml`` instead of using
  default values defined in the settings classes.  This also results in the settings
  showing up in the Dagster UI Launchpad, which previously they didn't, leading to
  confusion when trying to re-run the FERC to SQLite conversions. See :pr:`3710`.
* ``mlflow`` experiment tracking has been disabled by default when running the DAG,
  since it is only really helpful during development of new record linkage or other ML
  workflows. See :pr:`3710`.

.. _release-v2024.5.0:

---------------------------------------------------------------------------------------
v2024.5.0 (2024-05-24)
---------------------------------------------------------------------------------------

We've just completed our quarterly integration of EIA data sources for 2024Q2
(in support of RMI's Utility Transition Hub) and have also added a bunch of new
tables over the last few months in an effort to better support energy system
modelers (with support from GridLab). Details below.


New Data Coverage
^^^^^^^^^^^^^^^^^

EIA-860 & EIA-923
~~~~~~~~~~~~~~~~~

* Added cleaned EIA860 Schedule 8E FGD Equipment and EIA923 Schedule 8C FGD Operation
  and Maintenance data to the PUDL database as
  :ref:`i_core_eia923__fgd_operation_maintenance` and
  :ref:`i_core_eia860__fgd_equipment`. Once harvested, these tables will eventually be
  removed from the database, but they are being published until then. See :issue:`3394`
  and :issue:`3392`, and :pr:`3403`.
* Added new :ref:`core_eia860__scd_generators_wind` table from EIA860 Schedule 3.2
  which contains wind generator attributes. See :pr:`3522` and :pr:`3494`.
* Added new :ref:`core_eia860__scd_generators_solar` table from EIA860 Schedule 3.3
  which contains solar generator attributes. See :pr:`3524` and :pr:`3482`.
* Added new :ref:`core_eia860__scd_generators_energy_storage` table from EIA860 Schedule
  3.4 which contains energy storage generator attributes. See :pr:`3488` and :pr:`3526`.
  which contains solar generator attributes. See :pr:`3524` and :pr:`3482`
* Added new :ref:`core_eia923__monthly_energy_storage` table from EIA923 which contains
  monthly energy and fuel consumption metrics. See :pr:`3516` and :pr:`3546`.
* Added 2024 Q1 EIA923 and EIA860m data. See issues :issue:`3617,3618`, and PR
  :pr:`3625`.

GridPath RA Toolkit
~~~~~~~~~~~~~~~~~~~

* Added a new ``gridpathratoolkit`` data source containing hourly wind and solar
  generation profiles from the `GridPath Resource Adequacy Toolkit
  <https://gridlab.org/gridpathratoolkit>`__. See :doc:`data_sources/gridpathratoolkit`
  and the `new Zenodo archive <https://zenodo.org/records/10844662>`__, PR :pr:`3489`
  and `this PUDL archiver issue
  <https://github.com/catalyst-cooperative/pudl-archiver/issues/296>`__.
* Integrated the most processed version of the GridPath RA Toolkit wind and solar
  generation profiles, as well as the tables describing how individual generators were
  aggregated together to create the profiles. See issues :issue:`3509,3510,3511,3515`
  and PR :pr:`3514`. The new tables include:
  :ref:`out_gridpathratoolkit__hourly_available_capacity_factor` and
  :ref:`core_gridpathratoolkit__assn_generator_aggregation_group`.

EIA AEO
~~~~~~~

* Extracted tables 13, 15, 20, and 54 from the `EIA Annual Energy Outlook 2023
  <https://www.eia.gov/outlooks/aeo/tables_ref.php>`__, which include future
  projections related to electric power and renewable energy through the year
  2050, across a variety of scenarios. See :issue:`3368` and :pr:`3538`.
* Added new :ref:`core_eia861__yearly_short_form` table from EIA861 which contains
  the shorter version of EIA861. See issues :issue:`3540` and PR :pr:`3565`.
* Added new tables from EIA AEO table 54:

  * :ref:`core_eiaaeo__yearly_projected_generation_in_electric_sector_by_technology`
    contains generation capacity & generation projections for the electric
    sector, broken out by technology type. See :issue:`3581` and :pr:`3582`.
  * :ref:`core_eiaaeo__yearly_projected_generation_in_end_use_sectors_by_fuel_type`
    contains generation capacity & generation projections for the electric
    sector, broken out by technology type. See :issue:`3581` and :pr:`3598`.
  * :ref:`core_eiaaeo__yearly_projected_electric_sales` contains electric sales
    projections until 2050, broken out by customer type. See :issue:`3581` and
    :pr:`3617`.

NREL ATB
~~~~~~~~

* Added new NREL ATB tables with annual technology cost and performance projections. See
  issue :issue:`3465` and PRs :pr:`3498,3570`.

EIA-930
~~~~~~~

* Added hourly generation, demand, and interchange tables from the EIA-930. See issues
  :issue:`3486,3505` PR :pr:`3584` and `this issue in the PUDL archiver repo
  <https://github.com/catalyst-cooperative/pudl-archiver/issues/295>`__. See the
  data source documentation :doc:`data_sources/eia930` for more information.

EPA CEMS
~~~~~~~~

* Added 2024 Q1 of CEMS data. See :issue:`3620` and :pr:`3624`

EIA Bulk Electricity Data
~~~~~~~~~~~~~~~~~~~~~~~~~

* Updated the EIA Bulk Electricity data archive to include data that was available as of
  2024-05-01, which covers up through 2024-02-01 (3 months more than the previously
  used archive). See PR :pr:`3615`.

FERC Form 1
~~~~~~~~~~~
* Added new :ref:`out_ferc1__yearly_rate_base` table which includes granular financial
  data regarding what utilities include in their rate bases. See epic :issue:`2016`.

Data Cleaning
^^^^^^^^^^^^^
* When ``generator_operating_date`` values are too inconsistent to be harvested
  successfully, we now take the max date within a year and attempt to harvest again, to
  rescue records lost because of inconsistent month reporting in EIA 860 and 860M. See
  :issue:`3340` and PR :pr:`3419`. This change also fixed a bug that was preventing
  other columns harvested with a special process from being saved.
* When ingesting FERC 1 XBRL filings, we now take the most recent non-null
  value instead of the value from the latest filing that applies for a specific
  row. This means that we no longer lose data if a utility posts a FERC filing
  with only a small number of updated values.

EIA - FERC1 Record Linkage Model Update
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We merged in a refactor of the EIA plant parts to FERC1 plants record linkage
model, which was generously supported by a `CCAI Innovation Grant
<https://www.climatechange.ai/calls/innovation_grants>`__. This replaced the linear
regression model with a model built with the Python package `Splink
<https://moj-analytical-services.github.io/splink/index.html>`__. Splink provides helpful
visualizations to understand model performance and parameter tuning, which can be
generated with :mod:`devtools/splink-ferc1-eia-match.ipynb`. We measured model
performance with precision - a measure of accuracy when the model makes a prediction,
recall - a measure of coverage of FERC records model predicted a match for, and
accuracy - a measure of overall correctness of the predictions. Model performance
improved and now has a precision of .94, recall of .9, and overall accuracy of .85.

Schema Changes
^^^^^^^^^^^^^^
* Added ``balancing_authority_code_eia`` and ``sector_id_eia`` into the
  :ref:`core_eia860m__changelog_generators` table. The BA codes reported in the raw data
  contained a lot of non-standard values, which have now been standardized. See issue
  :issue:`3437` and PR :pr:`3442`.
* Renamed the ``utc_datetime`` column found in the FERC-714 tables to ``datetime_utc``
  in order to be consistent with ``operating_datetime_utc`` in the EPA CEMS data, and
  the new hourly renewable generation profiles in the GridPath RA Toolkit. See PR
  :pr:`3514`.
* Renamed the utility and balancing authority service territory tables to better conform
  to our naming conventions: ``out_eia861__compiled_geometry_utilities`` is now
  :ref:`out_eia861__yearly_utility_service_territory` and
  ``out_eia861__compiled_geometry_balancing_authorities`` is now
  :ref:`out_eia861__yearly_balancing_authority_service_territory`. See PR :pr:`3552`.
* All hourly tables are now published only as Apache Parquet files, rather than being
  written to the main PUDL SQLite database. This reduces the size of the PUDL DB, and
  also makes accessing these large table much faster both during data processing and for
  end users. See PR :pr:`3584`.  Affected tables include:

  * :ref:`core_eia930__hourly_interchange`
  * :ref:`core_eia930__hourly_net_generation_by_energy_source`
  * :ref:`core_eia930__hourly_operations`
  * :ref:`core_eia930__hourly_subregion_demand`
  * :ref:`core_epacems__hourly_emissions`
  * :ref:`out_ferc714__hourly_estimated_state_demand`
  * :ref:`out_ferc714__hourly_planning_area_demand`
  * :ref:`out_gridpathratoolkit__hourly_available_capacity_factor`

  The FERC-714 hourly demand tables have been removed from the
  :class:`pudl.output.pudltabl.PudlTabl` class, which has been deprecated.
* The long derelict ``core_ferc__codes_accounts`` table has been removed from the PUDL
  database. This table contained descriptions of the FERC accounts that were found in
  the Electric Plant in Service table, but only pertained to a single year, and was not
  being referenced or maintained elsewhere. See PR :pr:`3584`.
* Additional columns were added to the :ref:`core_eia__codes_balancing_authorities`
  table, indicating the timezone associated with each BA's reporting, whether it is a
  generation only BA, and its date of retirement, and what region it is part of. See PR
  :pr:`3584`.
* A new :ref:`core_eia__codes_balancing_authority_subregions` table was added to
  describe the relationships between BAs and their subregions. See PR :pr:`3584`.

Bug Fixes
^^^^^^^^^
* Ensure that all columns fed into the harvesting / reconciliation process are encoded
  before harvesting takes place, improving the consistency of harvested fields. See
  issue :issue:`3542` and PR :pr:`3558`. This change also simplifies the encoding
  process in the vast majority of cases, since the same global set of encoders can be
  used on any dataframe, with every column encoded based on the field definitions and
  FK constraints associated with the column name.

CLI Changes
^^^^^^^^^^^
* Removed the ``--clobber`` option from the ``ferc_to_sqlite`` command and associated
  assets. We rebuild these databases infrequently, and needing to either edit the
  runtime parameters in Dagster's Launchpad or remove the existing databases from the
  filesystem manually are brittle. Partly in response to issue :issue:`3612`; see PR
  :pr:`3622`.

.. _release-v2024.2.6:

---------------------------------------------------------------------------------------
v2024.2.6 (2024-02-25)
---------------------------------------------------------------------------------------
The main impetus behind this release is the quarterly update of some of our
core datasets with preliminary data for 2023Q4. The :doc:`data_sources/eia860`,
:doc:`data_sources/epacems`, and bulk EIA API data are all up to date through the end of
2023, while the :doc:`data_sources/eia923` lags a month behind and is currently only
available through November, 2023. We also addressed several issues we found in our
initial release automation process that will make it easier for us to do more frequent
releases, like this one!

We're also for the first time publishing the full historical time series of of generator
data available in the EIA860M, rather than just using the most recent release to update
the EIA860 outputs. This enables tracking of how planned fossil plant retirement dates
have evolved over time.

There are also updates to our data validation system, a new version of Pandas, and
experimental Parquet outputs. See below for the details.

New Data Coverage
^^^^^^^^^^^^^^^^^
* Add EIA860M data through December 2023 :issue:`3313`, :pr:`3367`.
* Add 2023 Q4 of CEMS data. See :issue:`3315`, :pr:`3379`.
* Add EIA923 monthly data through November 2023 :issue:`3314`, :pr:`3398,3422`.
* Create a new table :ref:`core_eia860m__changelog_generators` which tracks the
  evolution of all generator data reported in the EIA860M, in particular the stated
  retirement dates. see issue :issue:`3330` and PR :pr:`3331`. Previously only the most
  recent month of reported EIA860M data was available within the PUDL DB.

Release Infrastructure
^^^^^^^^^^^^^^^^^^^^^^
* Use the same logic to merge version tags into the ``stable`` branch as we are using
  to merge the nightly build tags into the ``nightly`` branch. See PR :pr:`3347`
* Automatically place a `temporary object hold <https://cloud.google.com/storage/docs/holding-objects#use-object-holds>`__
  on all versioned data releases that we publish to GCS, to ensure that they can't be
  accidentally deleted. See issue :issue:`3400` and PR :pr:`3421`.

Schema Changes
^^^^^^^^^^^^^^
* Restored the individual FERC Form 1 plant output tables, providing direct access to
  denormalized versions of the specific plant types via:

  * :ref:`out_ferc1__yearly_steam_plants_sched402`
  * :ref:`out_ferc1__yearly_small_plants_sched410`
  * :ref:`out_ferc1__yearly_hydroelectric_plants_sched406`
  * :ref:`out_ferc1__yearly_pumped_storage_plants_sched408`

  See issue :issue:`3416` & PR :pr:`3417`

Data Validation with Pandera
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We've started integrating :mod:`pandera` dataframe schemas and checks with
:mod:`dagster` `asset checks <https://docs.dagster.io/concepts/assets/asset-checks>`__
to validate data while our ETL pipeline is running instead of only after all the data
has been produced. Initially we are using the various database schema checks that are
generated by our metadata, but the goal is to migrate all of our data validation tests
into this framework over time, and to start using it to encode any new data validations
immediately. See issues :issue:`941,1572,3318,3412` and PR :pr:`3282`.

Pandas 2.2
^^^^^^^^^^
We've updated to Pandas 2.2, which has a number of changes and deprecations.  See PRs
:pr:`3272,3410`.

* Changes in
  `how merge results are sorted
  <https://pandas.pydata.org/pandas-docs/stable/whatsnew/v2.2.0.html#merge-and-dataframe-join-now-consistently-follow-documented-sort-behavior>`__
  impacted the assignment of ``unit_id_pudl`` values, so any hard-coded values that
  dependent on the previous assignments will likely be incorrect now. We had to update a
  number of tests and FERC1-EIA record linkage training data to account for this change.
* Pandas is also deprecating the use of the ``AS`` frequency alias, in favor of ``YS``,
  so many references to the old alias have been updated.
* We've switched to using the ``calamine`` engine for reading Excel files, which is
  much faster than the old ``openpyxl`` library.

Parquet Outputs
^^^^^^^^^^^^^^^
The ETL now outputs PyArrow Parquet files for all tables that are written to the PUDL
DB. The Parquet outputs are used as the interim storage for the ETL, rather than reading
all tables out of the SQLite DB. We aren't publicly distributing the Parquet outputs
yet, but are giving them a test run with some existing users. See :issue:`3102`
:pr:`3296,3399`.

Dependencies
^^^^^^^^^^^^
* Update PUDL to use Python 3.12. See issue :issue:`3327` and PR :pr:`3413`.

.. _release-v2024.02.05:

---------------------------------------------------------------------------------------
v2024.02.05
---------------------------------------------------------------------------------------

This release contains only minor data updates compared to what we put out in December,
however the database naming conventions and release process has changed pretty
dramatically. We are confident these changes will make the data we publish more
accessible, and allow us to push out updates much more frequently going forward.

We also finally merged in improvements and generalizations to our record linkage
processes, which were generously supported by a `CCAI Innovation Grant
<https://www.climatechange.ai/calls/innovation_grants>`__. Connecting disparate public
datasets that describe the same physical infrastructure and corporate entities is one
of the most valuable improvements we make to the data, and we are excited to be able to
be able to do it in a more general, reproducible way so we can easily apply it to other
datasets. We've already started work on a Mozilla Foundation grant to link SEC data to
the FERC and EIA data we already have, allowing us to track ownership relationships
between utility holding companies and their many subsidiaries. We expect the same kind
of process will be useful for linking the PHMSA gas pipeline data to natural gas
utilities that report to EIA and FERC.

Database Naming Conventions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Our main focus with this release was to overhaul the naming system for our nearly 200
database tables. This will hopefully make it easier to find what you're looking for,
especially if you are a new PUDL user. We think it will also make it easier for us to
keep the database organized as we continue to expand its scope.  For an explanation of
the new naming conventions, see :doc:`dev/naming_conventions`, and to see the full list
of all available tables, see the :doc:`data_dictionaries/pudl_db`.

This is a major breaking change for anybody is accessing the database directly. Stick
with the :ref:`release-v2023.12.01` release until you're ready to update your references
to the old database table names. For the time being we have patched the old
:class:`pudl.output.pudltabl.PudlTabl` class so that it behaves as similarly as possible
to before. However, we plan to remove this output class in the near future, and no new
database tables will be made accessible through it. Going forward we expect users to use
the database directly, freeing them from the need to install all of the software and
dependencies which we use to produce it, hopefully improving the data's technical
accessibility and platform independence.

For more development details see :issue:`2765` which was the main epic tracking this
process (with many sub-issues: :issue:`2777,2788,2812,2868,2992,3030,3173,3174,3223`)
and PR :pr:`2818`.

Changes to CLI Tools
^^^^^^^^^^^^^^^^^^^^

* The ``epacems_to_parquet`` and ``state_demand`` scripts have been retired in favor of
  using the Dagster UI. See :issue:`3107` and :pr:`3086`. Visualizations of hourly
  state-level electricity demand have been moved into our example notebooks which can
  be found both `on Kaggle <https://www.kaggle.com/code/catalystcooperative/02-state-hourly-electricity-demand>`__
  and `on GitHub <https://github.com/catalyst-cooperative/pudl-examples/>`__
* The ``pudl_setup`` script has been retired. All input/output locations are now set
  using the ``$PUDL_INPUT`` and ``$PUDL_OUTPUT`` environment variables.  See
  :issue:`3107` and :pr:`3086`.
* The :func:`pudl.analysis.service_territory.pudl_service_territories` script has been
  fixed, and can be used to generate `GeoParquet <https://geoparquet.org/>`__
  outputs describing historical utility and balancing authority service territories. See
  :issue:`1174` and :pr:`3086`.

Development Infrastructure
^^^^^^^^^^^^^^^^^^^^^^^^^^

* Automate the process of doing software and data releases when a new version tag is
  pushed to facilitate continuous deployment. See :pr:`3127,3158`
* To make development more convenient given our long-running integration tests, the PUDL
  repository now uses a `merge queue <https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue>`__.
* Switch to using Google Batch for our data builds. See :pr:`3211`.
* Deprecated the ``dev`` branch and updated our nightly builds and GitHub workflow to
  use three persistent branches: ``main`` for bleeding edge changes, ``nightly`` for the
  most recent commit to have a successful nightly build output, and ``stable`` for the
  most recently released version of PUDL. The ``nightly`` and ``stable`` branches are
  protected and automatically updated. Build outputs are now written to
  ``gs://builds.catalyst.coop`` and retained for 30 days. See issues :issue:`3140,3179`
  and PRs :pr:`3195,3206,3212,3188,3164`

Record Linkage Improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* The :mod:`pudl.analysis.record_linkage.eia_ferc1_record_linkage` module has been
  refactored substantially to make use of more generic PUDL record linkage
  infrastructure and include extra cleaning steps. This resulted in around 500 or 2% of
  matches changing. See `catalyst-cooperative/ccai-entity-matching#108 <http://github.com/catalyst-cooperative/ccai-entity-matching/issues/108>`__
  and :pr:`3184`.
* Update the FERC Form 1 plant ID assignment (Identifying related plant records from
  different years within the FERC Form 1 data) to use the new record linkage
  infrastructure. See :pr:`3007,3137`

New Data Coverage
^^^^^^^^^^^^^^^^^

* Updated :doc:`data_sources/epacems` to switch to pulling the quarterly updates of
  CEMS instead of the annual files. Integrates CEMS through 2023Q3. See issue
  :issue:`2973` & PR :pr:`3096,3139`.
* Began integration of PHMSA gas distribution and transmission tables into PUDL,
  extracting raw data from 1990-present. Note that these tables are not yet being
  written to the database as they are still raw. See epic :issue:`2848`, and constituent
  PRs: :pr:`2932,3242,3254,3260,3262, 3266,3267,3269,3270,3279,3280`.
* We began integration of data from EIA Forms 176, 191, and 757, describing natural gas
  sources, storage, transportation, and disposition. Note this data is still in its raw
  extracted form and is not yet being written to the PUDL DB. See :pr:`3304,3227`
* Updated the EIA Bulk Electricity data archive so that the available data now to runs
  through 2023-10-01. See :pr:`3252`.  Also added this dataset to the set of data that
  will automatically generate archives each month. See `This PUDL Archiver PR
  <https://github.com/catalyst-cooperative/pudl-archiver/pull/257>`__ and `this Zenodo
  archive <https://doi.org/10.5281/zenodo.10525348>`__

Data Cleaning
^^^^^^^^^^^^^

* Filled in null annual balances with fourth-quarter quarterly balances in
  :ref:`core_ferc1__yearly_balance_sheet_liabilities_sched110`. :issue:`3233` and
  :pr:`3234`.
* Added a notebook :mod:`devtools/debug-column-mapping.ipynb` to make debugging manual
  column maps for new datasets simpler and faster.

Metadata Cleaning
^^^^^^^^^^^^^^^^^

* Fix metadata structures and pyarrow schema generation process so that all tables can
  now be output as Parquet files. See issue :issue:`3102` and PR :pr:`3222`.
* Made a description field mandatory for all instances of ``Field`` and ``Resource``.
  Updated the :py:const:`pudl.metadata.fields.FIELD_METADATA`` and
  :py:const:`pudl.metadata.resources.RESOURCE_METADATA`` so that all of them have a
  description. This primarily affected :doc:`data_sources/eia861` tables. See
  :issue:`3224`, :pr:`3283`.
* Removed fields that are not used in any tables and removed the xfail from the
  ``test_defined_fields_are_used`` test. :issue:`3224`, :pr:`3283`.

.. _release-v2023.12.01:

---------------------------------------------------------------------------------------
v2023.12.01
---------------------------------------------------------------------------------------

Dagster Adoption
^^^^^^^^^^^^^^^^
* After comparing comparing python orchestration tools :issue:`1487`, we decided to
  adopt `Dagster <https://dagster.io/>`__. Dagster will allow us to parallelize the ETL,
  persist datafarmes at any step in the data cleaning process, visualize data
  dependencies and run subsets of the ETL from upstream caches.
* We are converting PUDL code to use dagster concepts in two phases. The first phase
  converts the ETL portion of the code base to use
  `software defined assets <https://docs.dagster.io/concepts/assets/software-defined-assets>`__
  :issue:`1570`. The second phase converts the output and analysis tables in the
  :mod:`pudl.output.pudltabl.PudlTabl` class to use software defined assets, replacing
  the existing ``pudl_out`` output functions.
* General changes:

  * :mod:`pudl.etl` is now a subpackage that collects all pudl assets into a dagster
    `Definition <https://docs.dagster.io/concepts/code-locations>`__.
  * The ``pudl_settings``, ``Datastore`` and ``DatasetSettings`` are now dagster
    resources. See :mod:`pudl.resources`.
  * The ``pudl_etl``  and ``ferc_to_sqlite`` commands no longer support loading
    specific tables. The commands run all of the tables. Use dagster assets to
    run subsets of the tables.
  * The ``--clobber`` argument has been removed from the ``pudl_etl`` command.
  * New static method :mod:`pudl.metadata.classes.Package.get_etl_group_tables`
    returns the resources ids for a given etl group.
  * :mod:`pudl.settings.FercToSqliteSettings` class now loads all FERC
    datasources if no datasets are specified.
  * The Excel extractor in ``pudl.extract.excel`` has been updated to parallelize
    Excel spreadsheet extraction using Dagster ``@multi_asset`` functionality, thanks to
    :user:`dstansby`. This is currently being used for EIA 860, 861 and 923 data. See
    :issue:`2385` and PRs :pr:`2644`, :pr:`2943`.

* EIA ETL changes:

  * The EIA table level cleaning functions are now
    dagster assets. The table level cleaning assets now have a "clean\_" prefix
    and a "_{datasource}" suffix to distinguish them from the final harvested tables.
  * ``pudl.transform.eia.transform()`` is now a ``@multi_asset`` that depends
    on all of the EIA table level cleaning functions / assets.

* EPA CEMS ETL changes:

  * :func:`pudl.transform.epacems.transform()` now loads the ``epacamd_eia`` and
    ``plants_entity_eia`` tables as dataframes using the
    :mod:`pudl.io_manager.pudl_sqlite_io_manager` instead of reading the tables
    using a ``pudl_engine``.
  * Adds a Ohio plant that is in 2021 CEMS but missing from EIA since 2018 to
    the ``additional_epacems_plants.csv`` sheet.

* FERC ETL changes:

  * :mod:`pudl.extract.ferc1.dbf2sqlite()` and :mod:`pudl.extract.xbrl.xbrl2sqlite()`
    are now configurable dagster ops. These ops make up the
    ``ferc_to_sqlite`` dagster graph in :mod:`pudl.ferc_to_sqlite.defs`.
  * FERC 714 extraction methods are now subsettable by year, with 2019 and 2020 data
    included in the ``etl_fast.yml`` by default. See :issue:`2628` and PR :pr:`2649`.

* Census DP1 ETL changes:

  * :mod:`pudl.convert.censusdp1tract_to_sqlite` and :mod:`pudl.output.censusdp1tract`
    are now integrated into dagster. See :issue:`1973` and :pr:`2621`.

New Asset Naming Convention
^^^^^^^^^^^^^^^^^^^^^^^^^^^
There are hundreds of new tables in ``pudl.sqlite`` now that the methods in ``PudlTabl``
have been converted to Dagster assets. This significant increase in tables and diversity
of table types prompted us to create a new naming convention to make the table names
more descriptive and organized. You can read about the new naming convention in the
:ref:`docs <asset-naming>`.

To help users migrate away from using ``PudlTabl`` and our temporary table names,
we've created a `google sheet <https://docs.google.com/spreadsheets/d/1RBuKl_xKzRSLgRM7GIZbc5zUYieWFE20cXumWuv5njo/edit?usp=sharing>`__
that maps the old table names and ``PudlTabl`` methods to the new table names.

We've added deprecation warnings to the ``PudlTabl`` class. We plan to remove
``PudlTabl`` from the ``pudl`` package once our known users have
successfully migrated to pulling data directly from ``pudl.sqlite``.

Data Coverage
^^^^^^^^^^^^^

* Updated :doc:`data_sources/eia860` to include final release data from 2022, see
  :issue:`3008` & PR :pr:`3040`.
* Updated :doc:`data_sources/eia861` to include final release data from 2022, see
  :issue:`3034` & PR :pr:`3048`.
* Updated :doc:`data_sources/eia923` to include final release data from 2022 and
  monthly YTD data as of October 2023, see :issue:`3009` & PR :pr:`#3073`.
* Extracted the raw ``raw_eia923__emissions_control`` table, see PR :pr:`3100`.
* Updated :doc:`data_sources/epacems` to switch from the old FTP server to the new
  CAMPD API, and to include 2022 data. Due to changes in the ETL, Alaska, Puerto Rico
  and Hawaii are now included in CEMS processing. See issue :issue:`1264` & PRs
  :pr:`2779`, :pr:` 2816`.
* New :ref:`core_epa__assn_eia_epacamd` crosswalk version v0.3, see issue :issue:`2317`
  and PR :pr:`2316`. EPA's updates add manual matches and exclusions focusing on
  operating units with a generator ID as of 2018.
* New PUDL tables from :doc:`data_sources/ferc1`, integrating older DBF and newer XBRL
  data. See :issue:`1574` for an overview of our progress integrating FERC's XBRL data.
  To see which DBF and XBRL tables the following PUDL tables are derived from, refer to
  :py:const:`pudl.extract.ferc1.TABLE_NAME_MAP`

  * :ref:`core_ferc1__yearly_energy_sources_sched401`, see issue :issue:`1819` & PR
    :pr:`2094`.
  * :ref:`core_ferc1__yearly_energy_dispositions_sched401`, see issue :issue:`1819` &
    PR :pr:`2100`.
  * :ref:`core_ferc1__yearly_transmission_lines_sched422`, see issue :issue:`1822` & PR
    :pr:`2103`
  * :ref:`core_ferc1__yearly_utility_plant_summary_sched200`, see issue
    :issue:`1806` & PR :pr:`2105`.
  * :ref:`core_ferc1__yearly_balance_sheet_assets_sched110`, see issue :issue:`1805` &
    PRs :pr:`2112,2127`.
  * :ref:`core_ferc1__yearly_balance_sheet_liabilities_sched110`, see issue
    :issue:`1810` & PR :pr:`2134`.
  * :ref:`core_ferc1__yearly_depreciation_summary_sched336`, see issue :issue:`1816`
    & PR :pr:`2143`.
  * :ref:`core_ferc1__yearly_income_statements_sched114`, see issue :issue:`1813` & PR
    :pr:`2147`.
  * :ref:`core_ferc1__yearly_depreciation_changes_sched219` see issue
    :issue:`1808` & :pr:`2119`.
  * :ref:`core_ferc1__yearly_depreciation_by_function_sched219` see issue
    :issue:`1808` & PR :pr:`2183`.
  * :ref:`core_ferc1__yearly_operating_expenses_sched320`, see issue :issue:`1817` & PR
    :pr:`2162`.
  * :ref:`core_ferc1__yearly_retained_earnings_sched118`, see issue :issue:`1811` & PR
    :pr:`2155`.
  * :ref:`core_ferc1__yearly_cash_flows_sched120`, see issue :issue:`1821` & PR
    :pr:`2184`.
  * :ref:`core_ferc1__yearly_sales_by_rate_schedules_sched304`, see issue
    :issue:`1823` & PR :pr:`2205`.

* Harvested owner utilities from the EIA 860 ownership table which are now included in
  the :ref:`core_eia__entity_utilities` and :ref:`core_pudl__assn_eia_pudl_utilities`
  tables. See :pr:`2714`. Renamed columns with owner or operator suffix to differentiate
  between owner and operator utility columns in :ref:`core_eia860__scd_ownership` and
  :ref:`out_eia860__yearly_ownership`. See :pr:`2903`.

* New PUDL tables from :doc:`data_sources/eia860`:

  * :ref:`core_eia860__scd_emissions_control_equipment`, see issue :issue:`2338` & PR
    :pr:`2561`.
  * :ref:`out_eia860__yearly_emissions_control_equipment`, see issue :issue:`2338` & PR
    :pr:`2561`.
  * :ref:`core_eia860__assn_yearly_boiler_emissions_control_equipment`, see
    :issue:`2338` & PR :pr:`2561`.
  * :ref:`core_eia860__assn_boiler_cooling`, see :issue:`2586` & PR :pr:`2587`
  * :ref:`core_eia860__assn_boiler_stack_flue`, see :issue:`2586` & PR :pr:`2587`

* The :ref:`core_eia860__scd_boilers` table now includes annual boiler attributes from
  :doc:`data_sources/eia860` Schedule 6.2 Environmental Equipment data, and the new
  :ref:`core_eia__entity_boilers` table now includes static boiler attributes. See issue
  :issue:`1162` & PR :pr:`2319`.
* All :doc:`data_sources/eia861` tables are now being loaded into the PUDL DB, rather
  than only being available via an ad-hoc ETL process that was only accessible through
  the :class:`pudl.output.pudltabl.PudlTabl` class. Note that most of these tables have
  not been normalized, and the ``utility_id_eia`` and ``balancing_authority_id_eia``
  values in them haven't been harvested, so these tables have very few valid foreign key
  relationships with the rest of the database right now -- but at least the data is
  available in the database! Existing methods for accessing these tables have been
  preserved. The ``PudlTabl`` methods just read directly from the DB and apply uniform
  data types, rather than actually doing the ETL. See :issue:`2265` & :pr:`2403`. The
  newly accessible tables contain data from 2001-2021 and include:

  * :ref:`core_eia861__yearly_advanced_metering_infrastructure`
  * :ref:`core_eia861__yearly_balancing_authority`
  * :ref:`core_eia861__assn_balancing_authority`
  * :ref:`core_eia861__yearly_demand_response`
  * :ref:`core_eia861__yearly_demand_response_water_heater`
  * :ref:`core_eia861__yearly_demand_side_management_sales`
  * :ref:`core_eia861__yearly_demand_side_management_ee_dr`
  * :ref:`core_eia861__yearly_demand_side_management_misc`
  * :ref:`core_eia861__yearly_distributed_generation_tech`
  * :ref:`core_eia861__yearly_distributed_generation_fuel`
  * :ref:`core_eia861__yearly_distributed_generation_misc`
  * :ref:`core_eia861__yearly_distribution_systems`
  * :ref:`core_eia861__yearly_dynamic_pricing`
  * :ref:`core_eia861__yearly_energy_efficiency`
  * :ref:`core_eia861__yearly_green_pricing`
  * :ref:`core_eia861__yearly_mergers`
  * :ref:`core_eia861__yearly_net_metering_customer_fuel_class`
  * :ref:`core_eia861__yearly_net_metering_misc`
  * :ref:`core_eia861__yearly_non_net_metering_customer_fuel_class`
  * :ref:`core_eia861__yearly_non_net_metering_misc`
  * :ref:`core_eia861__yearly_operational_data_revenue`
  * :ref:`core_eia861__yearly_operational_data_misc`
  * :ref:`core_eia861__yearly_reliability`
  * :ref:`core_eia861__yearly_sales`
  * :ref:`core_eia861__yearly_service_territory`
  * :ref:`core_eia861__assn_utility`
  * :ref:`core_eia861__yearly_utility_data_nerc`
  * :ref:`core_eia861__yearly_utility_data_rto`
  * :ref:`core_eia861__yearly_utility_data_misc`

* A couple of tables from :doc:`data_sources/ferc714` have been added to the PUDL DB.
  These tables contain data from 2006-2020 (2021 is distributed by FERC in XBRL format
  and we have not yet integrated it). See :issue:`2266`, :pr:`2421` and :pr:`2550`.
  The newly accessible tables include:

  * :ref:`core_ferc714__respondent_id` (linking FERC-714 respondents to EIA utilities)
  * :ref:`out_ferc714__hourly_planning_area_demand` (hourly electricity demand by
    planning area)
  * :ref:`out_ferc714__respondents_with_fips` (annual respondents with county FIPS IDs)
  * :ref:`out_ferc714__summarized_demand` (annual demand for FERC-714 respondents)

* Added new table :ref:`core_epa__assn_eia_epacamd_subplant_ids`, which arguments the
  :ref:`core_epa__assn_eia_epacamd` glue table. This table incorporates all
  :ref:`core_eia__entity_generators` and all :ref:`core_epacems__hourly_emissions` ID's
  and uses these complete IDs to develop a full-coverage ``subplant_id`` column which
  granularly connects EPA CAMD with EIA. Thanks to :user:`grgmiller` for his
  contribution to this process. See :issue:`2456` & :pr:`2491`.

* Added new table :ref:`out_pudl__yearly_assn_eia_ferc1_plant_parts` which links FERC1
  records from :ref:`out_ferc1__yearly_all_plants` and
  :ref:`out_eia__yearly_plant_parts`.

* Thanks to contributions from :user:`rousik` we've generalized the code we use to
  convert FERC's old annual Visual FoxPro databases into multi-year SQLite databases.

  * We have started extracting the FERC Form 2 (natual gas utility financial reports).
    See issues :issue:`1984,2642` and PRs :pr:`2536,2564,2652`. We haven't yet done any
    integration of the Form 2 into the cleaned and normalized PUDL DB, but the converted
    `FERC Form 2 is available on Datasette <https://data.catalyst.coop/ferc2>`__
    covering 1996-2020. Earlier years (1991-1995) were distributed using a different
    binary format and we don't currently have plans to extract them. From 2021 onward we
    are extracting the `FERC 2 from XBRL <https://data.catalyst.coop/ferc2_xbrl>`__.
  * Similarly :pr:`2595` converts the earlier years of FERC Form 6 (2000-2020) from DBF
    to SQLite, describing the finances of oil pipeline companies. When the nightly
    builds succeed, `FERC Form 6 will be available on Datasette <https://data.catalyst.coop/ferc6>`__
    as well.
  * :pr:`2734` converts the earlier years of FERC Form 60 (2006-2020) from DBF to
    SQLite. Form 60 is a comprehensive financial and operating report submitted for
    centralized service companies. `FERC Form 60 will also be available on Datasette
    <https://data.catalyst.coop/ferc6>`__.

Data Cleaning
^^^^^^^^^^^^^

* Removed inconsistently reported leading zeroes from numeric ``boiler_id`` values. This
  affected a small number of records in any table referring to boilers, including
  :ref:`core_eia__entity_boilers`, :ref:`core_eia860__scd_boilers`,
  :ref:`core_eia923__monthly_boiler_fuel`, :ref:`core_eia860__assn_boiler_generator`
  and the :ref:`core_epa__assn_eia_epacamd` crosswalk. It also had some minor downstream
  effects on the MCOE outputs. See :issue:`2366` and :pr:`2367`.
* The :ref:`core_eia923__monthly_boiler_fuel` table now includes the
  ``prime_mover_code`` column. This column was previously incorrectly being associated
  with boilers in the :ref:`core_eia__entity_boilers` table. See issue :issue:`2349` &
  PR :pr:`2362`.
* Fixed column naming issues in the
  :ref:`core_ferc1__yearly_operating_revenues_sched300` table.
* Made minor calculation fixes in the metadata for
  :ref:`core_ferc1__yearly_income_statements_sched114`,
  :ref:`core_ferc1__yearly_utility_plant_summary_sched200`,
  :ref:`core_ferc1__yearly_operating_revenues_sched300`,
  :ref:`core_ferc1__yearly_balance_sheet_assets_sched110`,
  :ref:`core_ferc1__yearly_balance_sheet_liabilities_sched110`, and
  :ref:`core_ferc1__yearly_operating_expenses_sched320`,
  :ref:`core_ferc1__yearly_depreciation_changes_sched219` and
  :ref:`core_ferc1__yearly_depreciation_by_function_sched219`. See :issue:`2016`,
  :pr:`2563`, :pr:`2662` and :pr:`2687`.
* Changed the :ref:`core_ferc1__yearly_retained_earnings_sched118` table transform to
  restore factoids for previous year balances, and added calculation metadata. See
  :issue:`1811`, :issue:`2016`, and :pr:`2645`.
* Added "correction" records to many FERC Form 1 tables where the reported totals do not
  match the outcomes of calculations specified in XBRL metadata (even after cleaning up
  the often incorrect calculation specifications!). See :issue:`2957` and :pr:`2620`.
* Flip the sign of some erroneous negative values in the
  :ref:`core_ferc1__yearly_plant_in_service_sched204` and
  :ref:`core_ferc1__yearly_utility_plant_summary_sched200` tables. See
  :issue:`2599`, and :pr:`2647`.

Analysis
^^^^^^^^

* Added a method for attributing fuel consumption reported on the basis of boiler ID and
  fuel to individual generators, analogous to the existing method for attributing net
  generation reported on the basis of prime mover & fuel. This should allow much more
  complete estimates of generator heat rates and thus fuel costs and emissions. Thanks
  to :user:`grgmiller` for his contribution, which was integrated by :user:`cmgosnell`!
  See PRs :pr:`1096,1608` and issues :issue:`1468,1478`.
* Integrated :mod:`pudl.analysis.eia_ferc1_record_linkage` from our RMI collaboration
  repo, which uses logistic regression to match FERC1 plants data to EIA 860 records.
  While far from perfect, this baseline model utilizes the manually created training
  data and plant IDs to perform record linkage on the FERC1 data and EIA plant parts
  list created in :mod:`pudl.analysis.plant_parts_eia`. See issue :issue:`1064` & PR
  :pr:`2224`. To account for 1:m matches in the manual data, we added
  ``plant_match_ferc1`` as a plant part in :mod:`pudl.analysis.plant_parts_eia`.
* Refined how we are associating generation and fuel data in
  :mod:`pudl.analysis.allocate_gen_fuel`, which was renamed from ``allocate_net_gen``.
  Energy source codes that show up in the :ref:`core_eia923__monthly_generation_fuel` or
  the :ref:`core_eia923__monthly_boiler_fuel` are now added into the
  :ref:`core_eia860__scd_generators` table so associating those gf and bf records are
  more cleanly associated with generators. Thanks to :user:`grgmiller` for his
  contribution, which was integrated by :user:`cmgosnell`! See PRs :pr:`2235,2446`.
* The :mod:`pudl.analysis.mcoe` table now uses the allocated estimates for per-generator
  net generation and fuel consumption. See PR :pr:`2553`.
* Additionally, the :mod:`pudl.analysis.mcoe` table now only includes attributes
  pertaining to the generator capacity, heat rate, and fuel cost. No additional
  generator attributes are included in this table. The full table with generator
  attributes merged on is now provided by :mod:`pudl.analysis.mcoe_generators`. See PR
  :pr:`2553`.
* Added outputs from :mod:`pudl.analysis.service_territory` and
  :mod:`pudl.analysis.state_demand` into PUDL. These outputs include the US Census
  geometries associated with balancing authority and utility data from EIA 861
  (:ref:`out_eia861__yearly_balancing_authority_service_territory` and
  :ref:`out_eia861__yearly_utility_service_territory`), and the estimated total hourly
  electricity demand for each US state in
  :ref:`out_ferc714__hourly_estimated_state_demand`. See :issue:`1973`
  and :pr:`2550`.

Deprecations
^^^^^^^^^^^^

* Replace references to deprecated ``pudl-scrapers`` and
  ``pudl-zenodo-datastore`` repositories with references to `pudl-archiver
  <https://www.github.com/catalyst-cooperative/pudl-archiver>`__ repository in
  :doc:`dev/datastore`, and :doc:`dev/existing_data_updates`. See
  :pr:`2190`.
* :mod:`pudl.etl` is now a subpackage that collects all pudl assets into a dagster
  `Definition <https://docs.dagster.io/concepts/code-locations>`__. All
  ``pudl.etl._etl_{datasource}`` functions have been deprecated. The coordination
  of ETL steps is being handled by dagster.
* The ``pudl.load`` module has been removed in favor of using the
  :mod:`pudl.io_managers.pudl_sqlite_io_manager`.
* The ``pudl_etl``  and ``ferc_to_sqlite`` commands no longer support loading
  specific tables. The commands run all of the tables. Use dagster assets to
  run subsets of the tables.
* The ``--clobber`` argument has been removed from the ``pudl_etl`` command.
* ``pudl.transform.eia860.transform()`` and ``pudl.transform.eia923.transform()``
  functions have been deprecated. The table level EIA cleaning functions are now
  coordinated using dagster.
* ``pudl.transform.ferc1.transform()`` has been removed. The ferc1 table
    transformations are now being orchestrated with Dagster.
* ``pudl.transform.ferc1.transform`` can no longer be executed as a script.
  Use dagster-webserver to execute just the FERC Form 1 pipeline.
* ``pudl.extract.ferc1.extract_dbf``, ``pudl.extract.ferc1.extract_xbrl``
  ``pudl.extract.ferc1.extract_xbrl_single``,
  ``pudl.extract.ferc1.extract_dbf_single``,
  ``pudl.extract.ferc1.extract_xbrl_generic``,
  ``pudl.extract.ferc1.extract_dbf_generic`` have all been deprecated. The extraction
  logic is now covered by the :mod:`pudl.io_managers.ferc1_xbrl_sqlite_io_manager` and
  :mod:`pudl.io_managers.ferc1_dbf_sqlite_io_manager` IO Managers.
* ``pudl.extract.ferc1.extract_xbrl_metadata`` has been replaced by the
  :func:`pudl.extract.ferc1.xbrl_metadata_json` asset.
* All sub classes of :func:`pudl.settings.GenericDatasetSettings` in
  :mod:`pudl.settings` no longer have table attributes because the ETL no longer
  supports loading specific tables via settings. Use dagster to select subsets of
  tables to process.

Miscellaneous
^^^^^^^^^^^^^

* Updated PUDL to use Python 3.11. See :pr:`2408` & :issue:`2383`
* Apply start and end dates to ferc1 data in :class:`pudl.output.pudltabl.PudlTabl`.
  See :pr:`2238` & :issue:`274`.
* Add generic spot fix method to transform process, to manually rescue FERC1 records.
  See :pr:`2254` & :issue:`1980`.
* Reverted a fix made in :pr:`1909`, which mapped all plants located in NY state that
  reported a balancing authority code of "ISONE" to "NYISO". These plants now retain
  their original EIA codes. Plants with manual re-mapping of BA codes have also been
  fixed to have correctly updated BA names. See :pr:`2312` and :issue:`2255`.
* Fixed a column naming bug that was causing EIA860 monthly retirement dates to get
  nulled out. See :issue:`2834` and :pr:`2835`
* Switched to using ``conda-lock`` and ``Makefile`` to manage testing and python
  environment. Moved away from packaging PUDL for distribution via PyPI and
  ``conda-forge`` and toward treating it as an application.  See :pr:`2968`
* The two-point-ohening: We now require Pandas v2 (see :pr:`2320`), SQLAlchemy v2 (see
  :pr:`2267`) and Pydantic v2 (see :pr:`3051`).
* Update the names of our FERC SQLite DBs to indicate what source data they come from.
  See issue :issue:`3079` and` :pr:`3094`.

.. _release-v2022.11.30:

---------------------------------------------------------------------------------------
v2022.11.30
---------------------------------------------------------------------------------------

Data Coverage
^^^^^^^^^^^^^

* Added archives of the bulk EIA electricity API data to our datastore, since the API
  itself is too unreliable for production use. This is part of :issue:`1763`. The code
  for this new data is ``eia_bulk_elec`` and the data comes as a single 200MB zipped
  JSON file. :pr:`1922` updates the datastore to include
  `this archive on Zenodo <https://zenodo.org/record/7067367>`__ but most of the work
  happened in the
  `pudl-scrapers <https://github.com/catalyst-cooperative/pudl-scrapers>`__ and
  `pudl-zenodo-storage <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
  repositories. See issue :issue:`catalyst-cooperative/pudl-zenodo-storage#29`.
* Incorporated 2021 data from the :doc:`data_sources/epacems` dataset. See :pr:`1778`
* Incorporated Final Release 2021 data from the :doc:`data_sources/eia860`,
  :doc:`data_sources/eia861`, and :doc:`data_sources/eia923`. We also integrated a
  ``data_maturity`` column and related ``data_maturities`` table into most of the EIA
  data tables in order to alter users to the level of finality of the data. See
  :pr:`1834,1855,1915,1921`.
* Incorporated 2022 data from the :doc:`data_sources/eia860` monthly update from
  September 2022. See :pr:`2079`. A June 2022 eia860m update included adding new
  ``energy_storage_capacity_mwh`` (for batteries) and ``net_capacity_mwdc`` (for
  behind-the-meter solar PV) attributes to the ``generators_eia860`` table, as they
  appear in the :doc:`data_sources/eia860` monthly updates for 2022.  See :pr:`1834`.
* Added new ``datasources`` table, which includes partitions used to generate the
  database. See :pr:`2079`.
* Integrated several new columns into the EIA 860 and EIA 923 including several
  codes with coding tables (See :doc:`data_dictionaries/codes_and_labels`). :pr:`1836`
* Added the `EPACAMD-EIA Crosswalk <https://github.com/USEPA/camd-eia-crosswalk>`__ to
  the database. Previously, the crosswalk was a csv stored in ``package_data/glue``,
  but now it has its own scraper
  :pr:`https://github.com/catalyst-cooperative/pudl-scrapers/pull/20`, archiver,
  :pr:`https://github.com/catalyst-cooperative/pudl-zenodo-storage/pull/20`
  and place in the PUDL db. For now there's a ``epacamd_eia`` output table you can use
  to merge CEMS and EIA data yourself :pr:`1692`. Eventually we'll work these crosswalk
  values into an output table combining CEMS and EIA.
* Integrated 2021 from the :doc:`data_sources/ferc1` data. FERC updated its reporting
  format for 2021 from a DBF file to a XBRL files. This required a major overhaul of
  the extract and transform step. The updates were accumulated in :pr:`1665`. The raw
  XBRL data is being extracted through a
  `FERC XBRL Extractor <https://github.com/catalyst-cooperative/ferc-xbrl-extractor>`__.
  This work is ongoing with additional tasks being tracked in :issue:`1574`. Specific
  updates in this release include:

  * Convert XBRL into raw sqlite database :pr:`1831`
  * Build transformer infrastructure & Add ``fuel_ferc1`` table :pr:`1721`
  * Map utility XBRL and DBF utility IDs :pr:`1931`
  * Add ``plants_steam_ferc1`` table :pr:`1881`
  * Add ``plants_hydro_ferc1`` :pr:`1992`
  * Add ``plants_pumped_storage_ferc1`` :pr:`2005`
  * Add ``purchased_power_ferc1`` :pr:`2011`
  * Add ``plants_small_ferc1`` table :pr:`2035`
  * Add ``plant_in_service_ferc1`` table :pr:`2025` & :pr:`2058`

* Added all of the SQLite databases which we build from FERC's raw XBRL filings to our
  Datasette deployment. See :pr:`2095` & :issue:`2080`. Browse the published data here:

  * `FERC Form 1 <https://data.catalyst.coop/ferc1_xbrl>`__
  * `FERC Form 2 <https://data.catalyst.coop/ferc2_xbrl>`__
  * `FERC Form 6 <https://data.catalyst.coop/ferc6_xbrl>`__
  * `FERC Form 60 <https://data.catalyst.coop/ferc60_xbrl>`__
  * `FERC Form 714 <https://data.catalyst.coop/ferc714_xbrl>`__

Data Analysis
^^^^^^^^^^^^^
* Instead of relying on the EIA API to fill in redacted fuel prices with aggregate
  values for individual states and plants, use the archived ``eia_bulk_elec`` data. This
  means we no longer have any reliance on the API, which should make the fuel price
  filling faster and more reliable. Coverage is still only about 90%. See :issue:`1764`
  and :pr:`1998`. Additional filling with aggregate and/or imputed values is still on
  the workplan. You can follow the progress in :issue:`1708`.

Nightly Data Builds
^^^^^^^^^^^^^^^^^^^
* We added infrastructure to run the entire ETL and all tests nightly
  so we can catch data errors when they are merged into ``dev``. This allows us
  to automatically update the `PUDL Intake data catalogs <https://github.com/catalyst-cooperative/pudl-catalog>`__
  when there are new code releases. See :issue:`1177` for more details.
* Created a `docker image <https://hub.docker.com/r/catalystcoop/pudl-etl>`__
  that installs PUDL and its dependencies. The ``build-deploy-pudl.yaml`` GitHub
  Action builds and pushes the image to Docker Hub and deploys the image on
  a Google Compute Engine instance. The ETL outputs are then loaded to Google
  Cloud buckets for the data catalogs to access.
* Added ``GoogleCloudStorageCache`` support to ``ferc1_to_sqlite`` and
  ``censusdp1tract_to_sqlite`` commands and pytest.
* Allow users to create monolithic and partitioned EPA CEMS outputs without having
  to clobber or move any existing CEMS outputs.
* ``GoogleCloudStorageCache`` now supports accessing requester pays buckets.
* Added a ``--loglevel`` arg to the package entrypoint commands.

Database Schema Changes
^^^^^^^^^^^^^^^^^^^^^^^
* After learning that generators' prime movers do very occasionally change over
  time, we recategorized the ``prime_mover_code`` column in our entity resolution
  process to enable the rare but real variability over time. We moved the
  ``prime_mover_code`` column from the statically harvested/normalized data
  column to an annually harvested data column (i.e. from ``generators_entity_eia``
  to ``generators_eia860``) :pr:`1600`. See :issue:`1585` for more details.
* Created ``operational_status_eia`` into our static metadata tables (See
  :doc:`data_dictionaries/codes_and_labels`). Used these standard codes and code
  fixes to clean ``operational_status_code`` in the ``generators_entity_eia``
  table. :pr:`1624`
* Moved a number of slowly changing plant attributes from the ``plants_entity_eia``
  table to the annual ``plants_eia860`` table. See :issue:`1748` and :pr:`1749`.
  This was initially inspired by the desire to more accurately reproduce the aggregated
  fuel prices which are available in the EIA's API. Along with state, census region,
  month, year, and fuel type, those prices are broken down by industrial sector.
  Previously ``sector_id_eia`` (an aggregation of several ``primary_purpose_naics_id``
  values) had been assumed to be static over a plant's lifetime, when in fact it can
  change if e.g. a plant is sold to an IPP by a regulated utility. Other plant
  attributes which are now allowed to vary annually include:

  * ``balancing_authority_code_eia``
  * ``balancing_authority_name_eia``
  * ``ferc_cogen_status``
  * ``ferc_exempt_wholesale_generator``
  * ``ferc_small_power_producer``
  * ``grid_voltage_1_kv``
  * ``grid_voltage_2_kv``
  * ``grid_voltage_3_kv``
  * ``iso_rto_code``
  * ``primary_purpose_id_naics``

* Renamed ``grid_voltage_kv`` to ``grid_voltage_1_kv`` in the ``plants_eia860``
  table, to follow the pattern of many other multiply reported values.
* Added a ``balancing_authorities_eia`` coding table mapping BA codes found in the
  :doc:`data_sources/eia860` and :doc:`data_sources/eia923` to their names, cleaning up
  non-standard codes, and fixing some reporting errors for ``PACW`` vs. ``PACE``
  (PacifiCorp West vs. East) based on the state associated with the plant reporting the
  code. Also added backfilling for codes in years before 2013 when BA Codes first
  started being reported, but only in the output tables. See: :pr:`1906,1911`
* Renamed and removed some columns in the :doc:`data_sources/epacems` dataset.
  ``unitid`` was changed to ``emissions_unit_id_epa`` to clarify the type of unit it
  represents. ``unit_id_epa`` was removed because it is a unique identifyer for
  ``emissions_unit_id_epa`` and not otherwise useful or transferable to other datasets.
  ``facility_id`` was removed because it is specific to EPA's internal database and does
  not aid in connection with other data. :pr:`1692`
* Added a new table ``political_subdivisions`` which consolidated various bits of
  information about states, territories, provinces etc. that had previously been
  scattered across constants stored in the codebase. The ``ownership_eia860`` table
  had a mix of state and country information stored in the same column, and to retain
  all of it we added a new ``owner_country_code`` column. :pr:`1966`

Data Accuracy
^^^^^^^^^^^^^
* Retain NA values for :doc:`data_sources/epacems` fields ``gross_load_mw`` and
  ``heat_content_mmbtu``. Previously, these fields converted NA to 0, but this is not
  accurate, so we removed this step.
* Update the ``plant_id_eia`` field from :doc:`data_sources/epacems` with values from
  the newly integrated ``epacamd_eia`` crosswalk as not all EPA's ORISPL codes are
  correct.

Helper Function Updates
^^^^^^^^^^^^^^^^^^^^^^^
* Replaced the PUDL helper function ``clean_merge_asof`` that merged two dataframes
  reported on different temporal granularities, for example monthly vs yearly data.
  The reworked function, :mod:`pudl.helpers.date_merge`, is more encapsulating and
  faster and replaces ``clean_merge_asof`` in the MCOE table and EIA 923 tables. See
  :pr:`1103,1550`
* The helper function :mod:`pudl.helpers.expand_timeseries` was also added, which
  expands a dataframe to include a full timeseries of data at a certain frequency.
  The coordinating function :mod:`pudl.helpers.full_timeseries_date_merge` first calls
  :mod:`pudl.helpers.date_merge` to merge two dataframes of different temporal
  granularities, and then calls :mod:`pudl.helpers.expand_timeseries` to expand the
  merged dataframe to a full timeseries. The added ``timeseries_filling`` argument,
  makes this function optionally used to generate the MCOE table that includes a full
  monthly timeseries even in years when annually reported generators don't have
  matching monthly data. See :pr:`1550`
* Updated the ``fix_leading_zero_gen_ids`` function by changing the name to
  ``remove_leading_zeros_from_numeric_strings`` because it's used to fix more than just
  the ``generator_id`` column. Included a new argument to specify which column you'd
  like to fix.

Plant Parts List Module Changes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* We refactored a couple components of the Plant Parts List module in preparation
  for the next round of entity matching of EIA and FERC Form 1 records with the
  Panda model developed by the
  `Chu Data Lab at Georgia Tech <https://chu-data-lab.cc.gatech.edu/>`__, through work
  funded by a
  `CCAI Innovation Grant <https://www.climatechange.ai/calls/innovation_grants>`__.
  The labeling of different aggregations of EIA generators as the true granularity was
  sped up, resulting in faster generation of the final plant parts list. In addition,
  the generation of the ``installation_year`` column in the plant parts list was fixed
  and a ``construction_year`` column was also added. Finally, ``operating_year`` was
  added as a level that the EIA generators are now aggregated to.
* The mega generators table and in turn the plant parts list requires the MCOE table
  to generate. The MCOE table is now created with the new :mod:`pudl.helpers.date_merge`
  helper function (described above). As a result, now by default only columns from the
  EIA 860 generators table that are necessary for the creation of the plant parts list
  will be included in the MCOE table. This list of columns is defined by the global
  :mod:`pudl.analysis.mcoe.DEFAULT_GENS_COLS`. If additional columns that are not part
  of the default list are needed from the EIA 860 generators table, these columns can be
  passed in with the ``gens_cols`` argument.  See :pr:`1550`
* For memory efficiency, appropriate columns are now cast to string and
  categorical types when the full plant parts list is created. The resource and field
  metadata is now included in the PUDL metadata. See :pr:`1865`
* For clarity and specificity, the ``plant_name_new`` column was renamed
  ``plant_name_ppe`` and the ``ownership`` column was renamed ``ownership_record_type``.
  See :pr:`1865`
* The ``PLANT_PARTS_ORDERED`` list was removed and ``PLANT_PARTS`` is now an
  ``OrderedDict`` that establishes the plant parts hierarchy in its keys. All references
  to ``PLANT_PARTS_ORDERED`` were replaced with the ``PLANT_PARTS`` keys. See :pr:`1865`

Metadata
^^^^^^^^
* Used the data source metadata class added in release 0.6.0 to dynamically generate
  the data source documentation (See :doc:`data_sources/index`). :pr:`1532`
* The EIA plant parts list was added to the resource and field metadata. This is the
  first output table to be included in the metadata. See :pr:`1865`

Documentation
^^^^^^^^^^^^^
* Fixed broken links in the documentation since the Air Markets Program Data (AMPD)
  changed to Clean Air Markets Data (CAMD).
* Added graphics and clearer descriptions of EPA data and reporting requirements to the
  :doc:`data_sources/epacems` page. Also included information about the ``epacamd_eia``
  crosswalk.

Bug Fixes
^^^^^^^^^
* `Dask v2022.4.2 <https://docs.dask.org/en/stable/changelog.html#v2022-04-2>`__
  introduced breaking changes into :meth:`dask.dataframe.read_parquet`.  However, we
  didn't catch this when it happened because it's only a problem when there's more than
  one row-group. Now we're processing 2019-2020 data for both ID and ME (two of the
  smallest states) in the tests. Also restricted the allowed Dask versions in our
  ``setup.py`` so that we get notified by the dependabot any time even a minor update.
  happens to any of the packages we depend on that use calendar versioning. See
  :pr:`1618`.
* Fixed a testing bug where the partitioned EPA CEMS outputs generated using parallel
  processing were getting output in the same output directory as the real ETL, which
  should never happen. See :pr:`1618`.
* Changed the way fixes to the EIA-861 balancing authority names and IDs are applied,
  so that they still work when only some years of data are being processed. See
  :pr:`1671` and :issue:`828`.

Dependencies / Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^
* In conjunction with getting the :user:`dependabot` set up to merge its own PRs if CI
  passes, we tightened the version constraints on a lot of our dependencies. This should
  reduce the frequency with which we get surprised by changes breaking things after
  release. See :pr:`1655`
* We've switched to using `mambaforge <https://github.com/conda-forge/miniforge>`__ to
  manage our environments internally, and are recommending that users use it as well.
* We're moving toward treating PUDL like an application rather than a library, and part
  of that is no longer trying to be compatible with a wide range of versions of our
  dependencies, instead focusing on a single reproducible environment that is associated
  with each release, using lockfiles, etc. See :issue:`1669`
* As an "application" PUDL is now only supporting the most recent major version of
  Python (currently 3.10). We used
  `pyupgrade <https://github.com/asottile/pyupgrade>`__ and
  `pep585-upgrade <https://github.com/snok/pep585-upgrade>`__ to update the syntax of
  to use Python 3.10 norms, and are now using those packages as pre-commit hooks as
  well. See :pr:`1685`

.. _release-v0-6-0:

---------------------------------------------------------------------------------------
0.6.0 (2022-03-11)
---------------------------------------------------------------------------------------

Data Coverage
^^^^^^^^^^^^^
* :doc:`data_sources/eia860` monthly updates (``eia860m``) up to the end of 2021.
  :pr:`1510`

New Analyses
^^^^^^^^^^^^
* For the purposes of linking EIA and FERC Form 1 records, we (mostly :user:`cmgosnell`)
  have created a new output called the Plant Parts List in
  :mod:`pudl.analysis.plant_parts_eia` which combines many different sub-parts of the
  EIA generators based on their fuel type, prime movers, ownership, etc. This allows a
  huge range of hypothiecally possible FERC Form 1 plant records to be synthesized, so
  that we can identify exactly what data in EIA should be associated with what data in
  FERC using a variety of record linkage & entity matching techniques. This is still a
  work in progress, both with our partners at RMI, and in collaboration with the
  `Chu Data Lab at Georgia Tech <https://chu-data-lab.cc.gatech.edu/>`__, through work
  funded by a
  `CCAI Innovation Grant <https://www.climatechange.ai/calls/innovation_grants>`__.
  :pr:`1157`

Metadata
^^^^^^^^
* Column data types for our database and Apache Parquet outputs, as well as pandas
  dataframes are all based on the same underlying schemas, and should be much more
  consistent. :pr:`1370,1377,1408`
* Defined a data source metadata class :class:`pudl.metadata.classes.DataSource` using
  Pydantic to store information and procedures specific to each data source (e.g.
  :doc:`data_sources/ferc1`, :doc:`data_sources/eia923`). :pr:`1446`
* Use the data source metadata classes to automatically export rich metadata for use
  with our Datasette deployment. :pr:`1479`
* Use the data source metadata classes to store rich metadata for use with our
  `Zenodo raw data archives <https://github.com/catalyst-cooperative/pudl-zenodo-storage/>`__
  so that information is no longer duplicated and liable to get out of sync.
  :pr:`1475`
* Added static tables and metadata structures that store definitions and additional
  information related to the many coded categorical columns in the database. These
  tables are exported directly into the documentation (See
  :doc:`data_dictionaries/codes_and_labels`). The metadata structures also document all
  of the non-standard values that we've identified in the raw data, and the standard
  codes that they are mapped to. :pr:`1388`
* As a result of all these metadata improvements we were finally able to close
  :issue:`52` and delete the ``pudl.constants`` junk-drawer module... after 5 years.

Data Cleaning
^^^^^^^^^^^^^
* Fixed a few inaccurately hand-mapped PUDL Plant & Utility IDs. :pr:`1458,1480`
* We are now using the coding table metadata mentioned above and the foreign key
  relationships that are part of the database schema to automatically recode any column
  that refers to the codes defined in the coding table. This results in much more
  uniformity across the whole database, especially in the EIA ``energy_source_code``
  columns. :pr:`1416`
* In the raw input data, often NULL values will be represented by the empty string or
  other not really NULL values. We went through and cleaned these up in all of the
  categorical / coded columns so that their values can be validated based on either an
  ENUM constraint in the database, or a foreign key constraint linking them to the
  static coding tables. Now they should primarily use the pandas NA value, or numpy.nan
  in the case of floats. :pr:`1376`
* Many FIPS and ZIP codes that appear in the raw data are stored as integers rather than
  strings, meaning that they lose their leading zeros, rendering them invalid in many
  contexts. We use the same method to clean them all up now, and enforce a uniform
  field width with leading zero padding. This also allows us to enforce a regex pattern
  constraint on these fields in the database outputs. :pr:`1405,1476`
* We're now able to fill in missing values in the very useful ``generators_eia860``
  ``technology_description`` field. Currently this is optionally available in the output
  layer, but we want to put more of this kind of data repair into the core database
  gong forward. :pr:`1075`

Miscellaneous
^^^^^^^^^^^^^
* Created a simple script that allows our SQLite DB to be loaded into Google's CloudSQL
  hosted PostgreSQL service `pgloader <https://pgloader.io/>`__ and
  `pg_dump <https://www.postgresql.org/docs/14/app-pgdump.html>`__. :pr:`1361`
* Made better use of our
  `Pydantic settings classes <https://pydantic-docs.helpmanual.io/usage/settings/>`__ to
  validate and manage the ETL settings that are read in from YAML files and passed
  around throughout the functions that orchestrate the ETL process. :pr:`1506`
* PUDL now works with pandas 1.4 (:pr:`1421`) and Python 3.10 (:pr:`1373`).
* Addressed a bunch of deprecation warnings being raised by :mod:`geopandas`. :pr:`1444`
* Integrated the `pre-commit.ci <https://pre-commit.ci>`__ service into our GitHub CI
  in order to automatically apply a variety of code formatting & checks to all commits.
  :pr:`1482`
* Fixed random seeds to avoid stochastic test coverage changes in the
  :mod:`pudl.analysis.timeseries_cleaning` module. :pr:`1483`
* Silenced a bunch of 3rd party module warnings in the tests. See :pr:`1476`

Bug Fixes
^^^^^^^^^
* In addressing :issue:`851,1296,1325` the ``generation_fuel_eia923`` table was split
  to create a ``generation_fuel_nuclear_eia923`` table since they have different
  primary keys. This meant that the :meth:`pudl.output.pudltabl.PudlTabl.gf_eia923`
  method no longer included nuclear generation. This impacted the net generation
  allocation process and MCOE calculations downstream, which were expecting to have all
  the reported nuclear generation. This has now been fixed, and the generation fuel
  output includes both the nuclear and non-nuclear generation, with nuclear generation
  aggregated across nuclear unit IDs so that it has the same primary key as the rest
  of the generation fuel table. :pr:`1518`
* EIA changed the URL of their API to only accept connections over HTTPS, but we had
  a hard-coded HTTP URL, meaning the historical fuel price filling that uses the API
  broke. This has been fixed.

Known Issues
^^^^^^^^^^^^
* Everything is fiiiiiine.

.. _release-v0-5-0:

---------------------------------------------------------------------------------------
0.5.0 (2021-11-11)
---------------------------------------------------------------------------------------

Data Coverage Changes
^^^^^^^^^^^^^^^^^^^^^
* Integration of 2020 data for all our core datasets (See :issue:`1255`):

  * :doc:`data_sources/eia860` for 2020 as well as 2001-2003 (see :issue:`1122`).
  * EIA Form 860m through 2021-08.
  * :doc:`data_sources/eia923` for 2020.
  * :doc:`data_sources/ferc1` for 2020.
  * :doc:`data_sources/eia861` data for 2020.
  * :doc:`data_sources/ferc714` for 2020.
  * Note: the 2020 :doc:`data_sources/epacems` data was already available in v0.4.0.

* **EPA IPM / NEEDS** data has been removed from PUDL as we didn't have the internal
  resources to maintain it, and it was no longer working. Apologies to
  :user:`gschivley`!

SQLite and Parquet Outputs
^^^^^^^^^^^^^^^^^^^^^^^^^^
* The ETL pipeline now outputs SQLite databases and Apache Parquet datasets
  directly, rather than generating tabular data packages. This is much faster
  and simpler, and also takes up less space on disk. Running the full ETL
  including all EPA CEMS data should now take around 2 hours if you have all the
  data downloaded.
* The new :mod:`pudl.load.sqlite` and :mod:`pudl.load.parquet` modules contain
  this logic. The :mod:`pudl.load.csv` and :mod:`pudl.load.metadata` modules have been
  removed along with other remaining datapackage infrastructure. See :issue:`1211`
* Many more tables now have natural primary keys explicitly specified within the
  database schema.
* The ``datapkg_to_sqlite`` script has been removed and the ``epacems_to_parquet``
  script can now be used to process the original EPA CEMS CSV data directly to
  Parquet using an existing PUDL database to source plant timezones.  See
  :issue:`1176,806`.
* Data types, specified value constraints, and the uniqueness / non-null
  constraints on primary keys are validated during insertion into the SQLite DB.
* The PUDL ETL CLI :mod:`pudl.etl.cli` now has flags to toggle various constraint
  checks including ``--ignore-foreign-key-constraints``
  ``--ignore-type-constraints`` and ``--ignore-value-constraints``.

New Metadata System
^^^^^^^^^^^^^^^^^^^
With the deprecation of tabular data package outputs, we've adopted a more
modular metadata management system that uses `Pydantic
<https://pydantic-docs.helpmanual.io/>`__.  This setup will allow us to easily
validate the metadata schema and export to a variety of formats to support data
distribution via `Datasette <https://datasette.io>`__ and `Intake catalogs
<https://intake.readthedocs.io>`__, and automatic generation of data
dictionaries and documentation. See :issue:`806,1271,1272` and the :mod:`pudl.metadata`
subpackage. Many thanks to :user:`ezwelty` for most of this work.

ETL Settings File Format Changed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We are also using `Pydantic <https://pydantic-docs.helpmanual.io/>`__ to parse and
validate the YAML settings files that tell PUDL what data to include in an ETL run. If
you have any old settings files of your own lying around they'll need to be updated.
Examples of the new format will be deployed to your system if you re-run the
``pudl_setup`` script. Or you can make a copy of the ``etl_full.yml`` or
``etl_fast.yml`` files that are stored under ``src/pudl/package_data/settings`` and
edit them to reflect your needs.

Database Schema Changes
^^^^^^^^^^^^^^^^^^^^^^^
With the direct database output and the new metadata system, it's much easier for us
to create foreign key relationships automatically. Updates that are in progress to
the database normalization and entity resolution process also benefit from using
natural primary keys when possible. As a result we've made some changes to the PUDL
database schema, which will probably affect some users.

* We have split out a new ``generation_fuel_nuclear_eia923`` table from the existing
  ``generation_fuel_eia923`` table, as nuclear generation and fuel consumption are
  reported at the generation unit level, rather than the plant level, requiring a
  different natural primary key. See :issue:`851,1296,1325`.
* Implementing a natural primary key for the ``boiler_fuel_eia923`` table required
  the aggregation of a small number of records that didn't have well-defined
  ``prime_mover_code`` values. See :issue:`852,1306,1311`.
* We repaired, aggregated, or dropped a small number of records in the
  ``generation_eia923`` (See :issue:`1208,1248`) and
  ``ownership_eia860`` (See :issue:`1207,1258`) tables due to null values in their
  primary key columns.
* Many new foreign key constraints are being enforced between the EIA data tables,
  entity tables, and coding tables. See :issue:`1196`.
* Fuel types and energy sources reported to EIA are now defined in / constrained by
  the static ``energy_sources_eia`` table.
* The columns that indicate the mode of transport for various fuels now contain short
  codes rather than longer labels, and are defined in / constrained by the static
  ``fuel_transportation_modes_eia`` table.
* In the simplified FERC 1 fuel type categories, we're now using ``other`` instead of
  ``unknown``.
* Several columns have been renamed to harmonize meanings between different tables and
  datasets, including:

  * In ``generation_fuel_eia923`` and ``boiler_fuel_eia923`` the ``fuel_type`` and
    ``fuel_type_code`` columns have been replaced with ``energy_source_code``, which
    appears in various forms in ``generators_eia860`` and
    ``fuel_receipts_costs_eia923``.
  * ``fuel_qty_burned`` is now ``fuel_consumed_units``
  * ``fuel_qty_units`` is now ``fuel_received_units``
  * ``heat_content_mmbtu_per_unit`` is now ``fuel_mmbtu_per_unit``
  * ``sector_name`` and ``sector_id`` are now ``sector_name_eia`` and ``sector_id_eia``
  * ``primary_purpose_naics_id`` is now ``primary_purpose_id_naics``
  * ``mine_type_code`` is now ``mine_type`` (a human readable label, not a code).

New Analyses
^^^^^^^^^^^^
* Added a deployed console script for running the state-level hourly electricity
  demand allocation, using FERC 714 and EIA 861 data, simply called
  ``state_demand`` and implemented in :mod:`pudl.analysis.state_demand`. This
  script existed in the v0.4.0 release, but was not deployed on the user's
  system.

Known Issues
^^^^^^^^^^^^
* The ``pudl_territories`` script has been disabled temporarily due to a memory
  issue. See :issue:`1174`
* Utility and Balancing Authority service territories for 2020 have not been vetted,
  and may contain errors or omissions. In particular there seems to be some missing
  demand in ND, SD, NE, KS, and OK. See :issue:`1310`

Updated Dependencies
^^^^^^^^^^^^^^^^^^^^
* **SQLAlchemy 1.4.x:** Addressed all deprecation warnings associated with API changes
  coming in SQLAlchemy 2.0, and bumped current requirement to 1.4.x
* **Pandas 1.3.x:** Addressed many data type issues resulting from changes in how Pandas
  preserves and propagates ExtensionArray / nullable data types.
* **PyArrow v5.0.0** Updated to the most recent version
* **PyGEOS v0.10.x** Updated to the most recent version
* **contextily** has been removed, since we only used it optionally for making a single
  visualization and it has substantial dependencies itself.
* **goodtables-pandas-py** has been removed since we're no longer producing or
  validating datapackages.
* **SQLite 3.32.0** The type checks that we've implemented currently only work with
  SQLite version 3.32.0 or later, as we discovered in debugging build failures on PR
  :issue:`1228`. Unfortunately Ubuntu 20.04 LTS shipped with SQLite 3.31.1. Using
  ``conda`` to manage your Python environment avoids this issue.

.. _release-v0-4-0:

---------------------------------------------------------------------------------------
0.4.0 (2021-08-16)
---------------------------------------------------------------------------------------
This is a ridiculously large update including more than a year and a half's
worth of work.

New Data Coverage
^^^^^^^^^^^^^^^^^

* :doc:`data_sources/eia860` for 2004-2008 + 2019, plus eia860m through 2020.
* :doc:`data_sources/eia923` for 2001-2008 + 2019
* :doc:`data_sources/epacems` for 2019-2020
* :doc:`data_sources/ferc1` for 2019
* :ref:`US Census Demographic Profile (DP1) <data-censusdp1tract>` for 2010
* :doc:`data_sources/ferc714` for 2006-2019 (experimental)
* :doc:`data_sources/eia861` for 2001-2019 (experimental)

Documentation & Data Accessibility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We've updated and (hopefully) clarified the documentation, and no longer expect
most users to perform the data processing on their own. Instead, we are offering
several methods of directly accessing already processed data:

* Processed data archives on Zenodo that include a Docker container preserving
  the required software environment for working with the data.
* `A repository of PUDL example notebooks <https://github.com/catalyst-cooperative/pudl-examples>`__
* `A JupyterHub instance <https://catalyst-cooperative.pilot.2i2c.cloud/>`__
  hosted in collaboration with `2i2c <https://2i2c.org>`__
* Browsable database access via `Datasette <https://datasette.io>`__ at
  https://data.catalyst.coop

Users who still want to run the ETL themselves will need to set up the
:doc:`set up the PUDL development environment <dev/dev_setup>`

Data Cleaning & Integration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now inject placeholder utilities in the cloned FERC Form 1 database when
  respondent IDs appear in the data tables, but not in the respondent table.
  This addresses a bunch of unsatisfied foreign key constraints in the original
  databases published by FERC.
* We're doing much more software testing and data validation, and so hopefully
  we're catching more issues early on.

Hourly Electricity Demand and Historical Utility Territories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With support from `GridLab <https://gridlab.org>`__ and in collaboration with
researchers at Berkeley's `Center for Environmental Public Policy
<https://gspp.berkeley.edu/faculty-and-impact/centers/cepp>`__, we did a bunch
of work on spatially attributing hourly historical electricity demand. This work
was largely done by :user:`ezwelty` and :user:`yashkumar1803` and included:

* Semi-programmatic compilation of historical utility and balancing authority
  service territory geometries based on the counties associated with utilities,
  and the utilities associated with balancing authorities in the EIA 861
  (2001-2019). See e.g. :pr:`670` but also many others.
* A method for spatially allocating hourly electricity demand from FERC 714 to
  US states based on the overlapping historical utility service territories
  described above. See :pr:`741`
* A fast timeseries outlier detection routine for cleaning up the FERC 714
  hourly data using correlations between the time series reported by all of the
  different entities. See :pr:`871`

Net Generation and Fuel Consumption for All Generators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
We have developed an experimental methodology to produce net generation and
fuel consumption for all generators. The process has known issues and is being
actively developed. See :pr:`989`

Net electricity generation and fuel consumption are reported in multiple ways in
the EIA 923. The ``generation_fuel_eia923`` table reports both generation and
fuel consumption, and breaks them down by plant, prime mover, and fuel. In
parallel, the ``generation_eia923`` table reports generation by generator,
and the ``boiler_fuel_eia923`` table reports fuel consumption by boiler.

The ``generation_fuel_eia923`` table is more complete, but the
``generation_eia923`` + ``boiler_fuel_eia923`` tables are more granular.
The ``generation_eia923`` table includes only ~55% of the total MWhs reported
in the ``generation_fuel_eia923`` table.

The :mod:`pudl.analysis.allocate_gen_fuel` module estimates the net electricity
generation and fuel consumption attributable to individual generators based on
the more expansive reporting of the data in the ``generation_fuel_eia923``
table.

Data Management and Archiving
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* We now use a series of web scrapers to collect snapshots of the raw input data
  that is processed by PUDL. These original data are archived as
  `Frictionless Data Packages <https://specs.frictionlessdata.io/data-package/>`__
  on `Zenodo <https://zenodo.org>`__, so that they can be accessed reproducibly
  and programmatically via a REST API. This addresses the problems we were
  having with the v0.3.x releases, in which the original data on the agency
  websites was liable to be modified long after its "final" release, rendering
  it incompatible with our software. These scrapers and the Zenodo archiving
  scripts can be found in our
  `pudl-scrapers <https://github.com/catalyst-cooperative/pudl-scrapers>`__ and
  `pudl-zenodo-storage <https://github.com/catalyst-cooperative/pudl-zenodo-storage>`__
  repositories. The archives themselves can be found within the
  `Catalyst Cooperative community on Zenodo <https://zenodo.org/communities/catalyst-cooperative/>`__
* There's an experimental caching system that allows these Zenodo archives to
  work as long-term "cold storage" for citation and reproducibility, with
  cloud object storage acting as a much faster way to access the same data for
  day to day non-local use, implemented by :user:`rousik`
* We've decided to shift to producing a combination of relational databases
  (SQLite files) and columnar data stores (Apache Parquet files) as the primary
  outputs of PUDL. `Tabular Data Packages <https://specs.frictionlessdata.io/tabular-data-package/>`__
  didn't end up serving either database or spreadsheet users very well. The CSV
  file were often too large to access via spreadsheets, and users missed out on
  the relationships between data tables. Needing to separately load the data
  packages into SQLite and Parquet was a hassle and generated a lot of overly
  complicated and fragile code.

Known Issues
^^^^^^^^^^^^

* The EIA 861 and FERC 714 data are not yet integrated into the SQLite database
  outputs, because we need to overhaul our entity resolution process to
  accommodate them in the database structure. That work is ongoing, see
  :issue:`639`
* The EIA 860 and EIA 923 data don't cover exactly the same rage of years. EIA
  860 only goes back to 2004, while EIA 923 goes back to 2001. This is because
  the pre-2004 EIA 860 data is stored in the DBF file format, and we need to
  update our extraction code to deal with the different format. This means some
  analyses that require both EIA 860 and EIA 923 data (like the calculation of
  heat rates) can only be performed as far back as 2004 at the moment. See
  :issue:`848`
* There are 387 EIA utilities and 228 EIA palnts which appear in the EIA 923,
  but which haven't yet been assigned PUDL IDs and associated with the
  corresponding utilities and plants reported in the FERC Form 1. These entities
  show up in the 2001-2008 EIA 923 data that was just integrated. These older
  plants and utilities can't yet be used in conjunction with FERC data. When the
  EIA 860 data for 2001-2003 has been integrated, we will finish this manual
  ID assignment process. See :issue:`848,1069`
* 52 of the algorithmically assigned ``plant_id_ferc1`` values found in the
  ``plants_steam_ferc1`` table are currently associated with more than one
  ``plant_id_pudl`` value (99 PUDL plant IDs are involved), indicating either
  that the algorithm is making poor assignments, or that the manually assigned
  ``plant_id_pudl`` values are incorrect. This is out of several thousand
  distinct ``plant_id_ferc1`` values. See :issue:`954`
* The county FIPS codes associated with coal mines reported in the Fuel Receipts and
  Costs table are being treated inconsistently in terms of their data types, especially
  in the output functions, so they are currently being output as floating point numbers
  that have been cast to strings, rather than zero-padded integers that are strings. See
  :issue:`1119`

.. _release-v0-3-2:

---------------------------------------------------------------------------------------
0.3.2 (2020-02-17)
---------------------------------------------------------------------------------------
The primary changes in this release:

* The 2009-2010 data for EIA 860 have been integrated, including updates
  to the data validation test cases.
* Output tables are more uniform and less restrictive in what they
  include, no longer requiring PUDL Plant & Utility IDs in some tables.  This
  release was used to compile v1.1.0 of the PUDL Data Release, which is archived
  at Zenodo under this DOI: https://doi.org/10.5281/zenodo.3672068

  With this release, the EIA 860 & 923 data now (finally!) cover the same span
  of time. We do not anticipate integrating any older EIA 860 or 923 data at
  this time.

.. _release-v0-3-1:

---------------------------------------------------------------------------------------
0.3.1 (2020-02-05)
---------------------------------------------------------------------------------------
A couple of minor bugs were found in the preparation of the first PUDL data
release:

* No maximum version of Python was being specified in setup.py. PUDL currently
  only works on Python 3.7, not 3.8.

* ``epacems_to_parquet`` conversion script was erroneously attempting to
  verify the availability of raw input data files, despite the fact that it now
  relies on the packaged post-ETL epacems data. Didn't catch this before since
  it was always being run in a context where the original data was lying
  around... but that's not the case when someone just downloads the released
  data packages and tries to load them.

.. _release-v0-3-0:

---------------------------------------------------------------------------------------
0.3.0 (2020-01-30)
---------------------------------------------------------------------------------------
This release is mostly about getting the infrastructure in place to do regular
data releases via Zenodo, and updating ETL with 2018 data.

Added lots of data validation / quality assurance test cases in anticipation of
archiving data. See the pudl.validate module for more details.

New data since v0.2.0 of PUDL:

* EIA Form 860 for 2018
* EIA Form 923 for 2018
* FERC Form 1 for 1994-2003 and 2018 (select tables)

We removed the FERC Form 1 accumulated depreciation table from PUDL because it
requires detailed row-mapping in order to be accurate across all the years. It
and many other FERC tables will be integrated soon, using new row-mapping
methods.

Lots of new plants and utilities integrated into the PUDL ID mapping process,
for the earlier years (1994-2003).  All years of FERC 1 data should be
integrated for all future ferc1 tables.

Command line interfaces of some of the ETL scripts have changed, see their help
messages for details.

.. _release-v0-2-0:

---------------------------------------------------------------------------------------
0.2.0 (2019-09-17)
---------------------------------------------------------------------------------------
This is the first release of PUDL to generate data packages as the canonical
output, rather than loading data into a local PostgreSQL database. The data
packages can then be used to generate a local SQLite database, without relying
on any software being installed outside of the Python requirements specified for
the catalyst.coop package.

This change will enable easier installation of PUDL, as well as archiving and
bulk distribution of the data products in a platform independent format.

.. _release-v0-1-0:

---------------------------------------------------------------------------------------
0.1.0 (2019-09-12)
---------------------------------------------------------------------------------------

This is the only release of PUDL that will be made that makes use of
PostgreSQL as the primary data product. It is provided for reference, in case
there are users relying on this setup who need access to a well defined release.
