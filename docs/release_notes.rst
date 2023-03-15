=======================================================================================
PUDL Release Notes
=======================================================================================

.. _release-v2023.XX.XX:

---------------------------------------------------------------------------------------
v2023.XX.XX
---------------------------------------------------------------------------------------

Data Coverage
^^^^^^^^^^^^^

* Updated :doc:`data_sources/eia860` to include data as of 2022-09.
* New :ref:`epacamd_eia` crosswalk version v0.3, see issue :issue:`2317` and PR
  :pr:`2316`. EPA's updates add manual matches and exclusions focusing on operating
  units with a generator ID as of 2018.
* New PUDL tables from :doc:`data_sources/ferc1`, integrating older DBF and newer XBRL
  data. See :issue:`1574` for an overview of our progress integrating FERC's XBRL data.
  To see which DBF and XBRL tables the following PUDL tables are derived from, refer to
  :py:const:`pudl.extract.ferc1.TABLE_NAME_MAP`

  * :ref:`electric_energy_sources_ferc1`, see issue :issue:`1819` & PR :pr:`2094`.
  * :ref:`electric_energy_dispositions_ferc1`, see issue :issue:`1819` & PR :pr:`2100`.
  * :ref:`transmission_statistics_ferc1`, see issue :issue:`1822` & PR :pr:`2103`
  * :ref:`utility_plant_summary_ferc1`, see issue :issue:`1806` & PR :pr:`2105`.
  * :ref:`balance_sheet_assets_ferc1`, see issue :issue:`1805` & PRs :pr:`2112,2127`.
  * :ref:`balance_sheet_liabilities_ferc1`, see issue :issue:`1810` & PR :pr:`2134`.
  * :ref:`depreciation_amortization_summary_ferc1`, see issue :issue:`1816` & PR
    :pr:`2143`.
  * :ref:`income_statement_ferc1`, see issue :issue:`1813` & PR :pr:`2147`.
  * :ref:`electric_plant_depreciation_changes_ferc1` see issue :issue:`1808` &
    :pr:`2119`.
  * :ref:`electric_plant_depreciation_functional_ferc1` see issue :issue:`1808` & PR
    :pr:`2183`
  * :ref:`electric_operating_expenses_ferc1`, see issue :issue:`1817` & PR :pr:`2162`.
  * :ref:`retained_earnings_ferc1`, see issue :issue:`1811` & PR :pr:`2155`.
  * :ref:`cash_flow_ferc1`, see issue :issue:`1821` & PR :pr:`2184`
  * :ref:`electricity_sales_by_rate_schedule_ferc1`, see issue :issue:`1823` & PR
    :pr:`2205`
* The :ref:`boilers_eia860` table now includes annual boiler attributes from
  :doc:`data_sources/eia860` Schedule 6.2 Environmental Equipment data, and the new
  :ref:`boilers_entity_eia` table now includes static boiler attributes. See issue
  :issue:`1162` & PR :pr:`2319`.

Data Cleaning
^^^^^^^^^^^^^

* Removed inconsistently reported leading zeroes from numeric ``boiler_id`` values. This
  affected a small number of records in any table referring to boilers, including
  :ref:`boilers_entity_eia`, :ref:`boilers_eia860`, :ref:`boiler_fuel_eia923`,
  :ref:`boiler_generator_assn_eia860` and the :ref:`epacamd_eia` crosswalk. It also had
  some minor downstream effects on the MCOE outputs. See :issue:`2366` and :pr:`2367`.
* The :ref:`boiler_fuel_eia923` table now includes the ``prime_mover_code`` column. This
  column was previously incorrectly being associated with boilers in the
  :ref:`boilers_entity_eia` table. See issue :issue:`2349` & PR :pr:`2362`.

Analysis
^^^^^^^^

* Added a method for attributing fuel consumption reported on the basis of boiler ID and
  fuel to individual generators, analogous to the existing method for attributing net
  generation reported on the basis of prime mover & fuel. This should allow much more
  complete estimates of generator heat rates and thus fuel costs and emissions. Thanks
  to :user:`grgmiller` for his contribution, which was integrated by :user:`cmgosnell`!
  See PRs :pr:`1096,1608` and issues :issue:`1468,1478`.
* Integrated :mod:`pudl.analysis.ferc1_eia` from our RMI collaboration repo, which uses
  logistic regression to match FERC1 plants data to EIA 860 records. While far from
  perfect, this baseline model utilizes the manually created training data and plant IDs
  to perform record linkage on the FERC1 data and EIA plant parts list created in
  :mod:`pudl.analysis.plant_parts_eia`. See issue :issue:`1064` & PR :pr:`2224`.

Deprecations
^^^^^^^^^^^^

* Replace references to deprecated ``pudl-scrapers`` and
  ``pudl-zenodo-datastore`` repositories with references to `pudl-archiver
  <https://www.github.com/catalyst-cooperative/pudl-archiver>`__ repository in
  :doc:`intro`, :doc:`dev/datastore`, and :doc:`dev/annual_updates`. See :pr:`2190`.

Miscellaneous
^^^^^^^^^^^^^

* Apply start and end dates to ferc1 data in :class:`pudl.output.pudltabl.PudlTabl`.
  See :pr:`2238` & :issue:`274`.
* Added the ability to serialize :class:`pudl.output.pudltabl.PudlTabl` using
  :mod:`pickle`. To implement this functionality new ``__getstate__`` and
  ``__setstate__`` methods have been added to :class:`pudl.output.pudltabl.PudlTabl` and
  :class:`pudl.workspace.resource_cache.GoogleCloudStorageCache` to accommodate elements
  of their internals that could not otherwise be serialized.
* Add generic spot fix method to transform process, to manually rescue FERC1 records.
  See :pr:`2254` & :issue:`1980`.
* Reverted a fix made in :pr:`1909`, which mapped all plants located in NY state that
  reported a balancing authority code of "ISONE" to "NYISO". These plants now retain
  their original EIA codes. Plants with manual re-mapping of BA codes have also been
  fixed to have correctly updated BA names. See :pr:`2312` and :issue:`2255`.

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
  :ref:`data-eia861`, and :doc:`data_sources/eia923`. We also integrated a
  ``data_maturity`` column and related ``data_maturities`` table into most of the EIA
  data tables in order to alter users to the level of finality of the data. See
  :pr:`1834,1855,1915,1921`.
* Incorporated 2022 data from the :doc:`data_sources/eia860` monthly update from
  September 2022. See :pr:`2079`. A June 2022 eia860m update included adding new
  ``energy_storage_capacity_mwh`` (for batteries) and ``net_capacity_mwdc`` (for
  behind-the-meter solar PV) attributes to the :ref:`generators_eia860` table, as they
  appear in the :doc:`data_sources/eia860` monthly updates for 2022.  See :pr:`1834`.
* Added new :ref:`datasources` table, which includes partitions used to generate the
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
  * Build transformer infrastructure & Add :ref:`fuel_ferc1` table :pr:`1721`
  * Map utility XBRL and DBF utility IDs :pr:`1931`
  * Add :ref:`plants_steam_ferc1` table :pr:`1881`
  * Add :ref:`plants_hydro_ferc1` :pr:`1992`
  * Add :ref:`plants_pumped_storage_ferc1` :pr:`2005`
  * Add :ref:`purchased_power_ferc1` :pr:`2011`
  * Add :ref:`plants_small_ferc1` table :pr:`2035`
  * Add :ref:`plant_in_service_ferc1` table :pr:`2025` & :pr:`2058`

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
  that installs PUDL and it's depedencies. The ``build-deploy-pudl.yaml`` GitHub
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
  column to an annually harvested data column (i.e. from :ref:`generators_entity_eia`
  to :ref:`generators_eia860`) :pr:`1600`. See :issue:`1585` for more details.
* Created :ref:`operational_status_eia` into our static metadata tables (See
  :doc:`data_dictionaries/codes_and_labels`). Used these standard codes and code
  fixes to clean ``operational_status_code`` in the :ref:`generators_entity_eia`
  table. :pr:`1624`
* Moved a number of slowly changing plant attributes from the :ref:`plants_entity_eia`
  table to the annual :ref:`plants_eia860` table. See :issue:`1748` and :pr:`1749`.
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

* Renamed ``grid_voltage_kv`` to ``grid_voltage_1_kv`` in the :ref:`plants_eia860`
  table, to follow the pattern of many other multiply reported values.
* Added a :ref:`balancing_authorities_eia` coding table mapping BA codes found in the
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
* Added a new table :ref:`political_subdivisions` which consolidated various bits of
  information about states, territories, provinces etc. that had previously been
  scattered across constants stored in the codebase. The :ref:`ownership_eia860` table
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
  merged dataframe to a full timeseries. The added ``timeseries_fillin`` argument,
  makes this function optionally used to generate the MCOE table that includes a full
  monthly timeseries even in years when annually reported generators don't have
  matching monthly data. See :pr:`1550`
* Updated the ``fix_leading_zero_gen_ids`` fuction by changing the name to
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
  Python (curently 3.10). We used
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
  with our Datasette deployement. :pr:`1479`
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
* We're now able to fill in missing values in the very useful :ref:`generators_eia860`
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
* In addressing :issue:`851,1296,1325` the :ref:`generation_fuel_eia923` table was split
  to create a :ref:`generation_fuel_nuclear_eia923` table since they have different
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
  * :ref:`data-eia861` data for 2020.
  * :ref:`data-ferc714` data for 2020.
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
* The PUDL ETL CLI :mod:`pudl.cli` now has flags to toggle various constraint
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
With the direct database output and the new metadata system, it's much eaiser for us
to create foreign key relationships automatically. Updates that are in progress to
the database normalization and entity resolution process also benefit from using
natural primary keys when possible. As a result we've made some changes to the PUDL
database schema, which will probably affect some users.

* We have split out a new :ref:`generation_fuel_nuclear_eia923` table from the existing
  :ref:`generation_fuel_eia923` table, as nuclear generation and fuel consumption are
  reported at the generation unit level, rather than the plant level, requiring a
  different natural primary key. See :issue:`851,1296,1325`.
* Implementing a natural primary key for the :ref:`boiler_fuel_eia923` table required
  the aggregation of a small number of records that didn't have well-defined
  ``prime_mover_code`` values. See :issue:`852,1306,1311`.
* We repaired, aggregated, or dropped a small number of records in the
  :ref:`generation_eia923` (See :issue:`1208,1248`) and
  :ref:`ownership_eia860` (See :issue:`1207,1258`) tables due to null values in their
  primary key columns.
* Many new foreign key constraints are being enforced between the EIA data tables,
  entity tables, and coding tables. See :issue:`1196`.
* Fuel types and energy sources reported to EIA are now defined in / constrained by
  the static :ref:`energy_sources_eia` table.
* The columns that indicate the mode of transport for various fuels now contain short
  codes rather than longer labels, and are defined in / constrained by the static
  :ref:`fuel_transportation_modes_eia` table.
* In the simplified FERC 1 fuel type categories, we're now using ``other`` instead of
  ``unknown``.
* Several columns have been renamed to harmonize meanings between different tables and
  datasets, including:

  * In :ref:`generation_fuel_eia923` and :ref:`boiler_fuel_eia923` the ``fuel_type`` and
    ``fuel_type_code`` columns have been replaced with ``energy_source_code``, which
    appears in various forms in :ref:`generators_eia860` and
    :ref:`fuel_receipts_costs_eia923`.
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
* :ref:`data-ferc714` for 2006-2019 (experimental)
* :ref:`data-eia861` for 2001-2019 (experimental)

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
the EIA 923. The :ref:`generation_fuel_eia923` table reports both generation and
fuel consumption, and breaks them down by plant, prime mover, and fuel. In
parallel, the :ref:`generation_eia923` table reports generation by generator,
and the :ref:`boiler_fuel_eia923` table reports fuel consumption by boiler.

The :ref:`generation_fuel_eia923` table is more complete, but the
:ref:`generation_eia923` + :ref:`boiler_fuel_eia923` tables are more granular.
The :ref:`generation_eia923` table includes only ~55% of the total MWhs reported
in the :ref:`generation_fuel_eia923` table.

The :mod:`pudl.analysis.allocate_net_gen` module estimates the net electricity
generation and fuel consumption attributable to individual generators based on
the more expansive reporting of the data in the :ref:`generation_fuel_eia923`
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
  plants and utilities can't yet be used in conjuction with FERC data. When the
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
