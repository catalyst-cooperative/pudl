===============================================================================
Naming Conventions
===============================================================================
    *There are only two hard problems in computer science: caching,
    naming things, and off-by-one errors.*

We try to use consistent naming conventions for the data tables, data assets,
columns, data sources, and functions.

.. _asset-naming:

Asset Naming Conventions
---------------------------------------------------

PUDL's data processing is divided into three layers of Dagster assets: Raw, Core
and Output. Dagster assets are the core unit of computation in PUDL. The outputs
of assets can be persisted to any type of storage though PUDL outputs are typically
tables in a SQLite database, parquet files or pickle files (read more about this here:
:doc:`../index`). The asset name is used for the table or parquet file name. Asset
names should generally follow this naming convention:

.. code-block::

    {layer}_{source}__{asset_type}_{asset_name}

* ``layer`` is the processing layer of the asset. Acceptable values are:
  ``raw``, ``core`` and ``out``. ``layer`` is required for all assets in all layers.
* ``source`` is an abbreviation of the original source of the data. For example,
  ``eia860``, ``ferc1`` and ``epacems``.
* ``asset_type`` describes how the asset is modeled.
* ``asset_name`` should describe the entity, categorical code type, or measurement of
  the asset. Note: FERC Form 1 assets typically include the schedule number in the
  ``asset_name`` so users and contributors know which schedule the cleaned asset
  refers to.

Raw layer
^^^^^^^^^
This layer contains assets that extract data from spreadsheets and databases
and are persisted as pickle files.

Naming convention: ``raw_{source}__{asset_name}``

* ``asset_name`` is typically copied from the source data.
* ``asset_type`` is not included in this layer because the data modeling does not
  yet conform to PUDL standards. Raw assets are typically just copies of the
  source data.

Core layer
^^^^^^^^^^
This layer contains assets that typically break denormalized raw assets into
well-modeled tables that serve as building blocks for downstream wide tables
and analyses. Well-modeled means tables in the database have logical
primary keys, foreign keys, datatypes and generally follow
:ref:`Tidy Data standards <tidy-data>`. Assets in this layer create
consistent categorical variables, deduplicate and impute data.
These assets are typically stored in parquet files or tables in a database.

Naming convention: ``core_{source}__{asset_type}_{asset_name}``

* ``source`` is sometimes ``pudl``. This means the asset
  is a derived connection the contributors of PUDL created to connect multiple
  datasets via manual or machine learning methods.

* ``asset_type`` describes how the asset is modeled and its role in PUDL’s
  collection of core assets. There are a handful of table types in this layer:

  * ``assn``: Association assets provide connections between entities. They should
    follow this naming convention:

    ``{layer}_{source of association asset}__assn_{datasets being linked}_{entity
    being linked}``

    Association assets can be manually compiled or extracted from data sources. If
    the asset associates data from two sources, the source names should be included
    in the ``asset_name`` in alphabetical order. Examples:

    * ``core_pudl__assn_plants_eia`` associates EIA Plant IDs and manually assigned
      PUDL Plant IDs.
    * ``core_epa__assn_epacamd_eia`` associates  EPA units with EIA plants, boilers,
      and generators.
  * ``codes``: Code tables contain more verbose descriptions of categorical codes
    typically manually compiled from source data dictionaries. Examples:

    * ``core_eia__codes_averaging_periods``
    * ``core_eia__codes_balancing_authorities``
  * ``entity``: Entity tables contain static information about entities. For example,
    the state a plant is located in or the plant a boiler is a part of. Examples:

    * ``core_eia__entity_boilers``
    * ``core_eia923__entity_coalmine``.
  * ``scd``: Slowly changing dimension tables describe attributes of entities that
    rarely change. For example, the ownership or the capacity of a plant. Examples:

    * ``core_eia860__scd_generators``
    * ``core_eia860__scd_plants``.
  * ``yearly/monthly/hourly``: Time series tables contain attributes about entities
    that are expected to change for each reported timestamp. Time series tables
    typically contain measurements of processes like net generation or co2 emissions.
    Examples:

    * ``out_ferc714__hourly_planning_area_demand``,
    * ``core_ferc1__yearly_plant_in_service``.

Output layer
^^^^^^^^^^^^
This layer uses assets in the Core layer to construct wide and complete tables
suitable for users to perform analysis on. This layer can contain intermediate
tables that bridge the core and user-facing tables.

Naming convention: ``out_{source}__{asset_type}_{asset_name}``

* ``source`` is optional in this layer because there can be assets that join data from
  multiple sources.
* ``asset_type`` is also optional. It will likely describe the frequency at which
  the data is reported (annual/monthly/hourly).

Intermediate Assets
^^^^^^^^^^^^^^^^^^^
Intermediate assets are logical steps towards a final well-modeled core or
user-facing output asset. These assets are not intended to be persisted in the
database or accessible to the user. These assets are denoted by a preceding
underscore, like a private python method. For example, the intermediate asset
``_core_eia860__plants`` is a logical step towards the
``core_eia860__entity_plants`` and ``core_eia860__scd_plants`` assets.
``_core_eia860__plants`` does some basic cleaning of the ``raw_eia860__plant``
asset but still contains duplicate plant entities. The computation intensive
harvesting process deduplicates ``_core_eia860__plants`` and outputs the
``core_eia860__entity_plants`` and ``core_eia860__scd_plants`` assets which
follow Tiny Data standards.

Limit the number of intermediate assets to avoid an extremely
cluttered DAG. It is appropriate to create an intermediate asset when:

  * there is a short and long running portion of a process. It is convenient to separate
    the long and short-running processing portions into separate assets so debugging the
    short-running process doesn’t take forever.
  * there is a logical step in a process that is frequently inspected for debugging. For
    example, the pre harvest assets in the ``_core_eia860`` and ``_core_eia923`` groups
    are frequently inspected when new years of data are added.


Columns and Field Names
-----------------------
If two columns in different tables record the same quantity in the same units,
give them the same name. That way if they end up in the same dataframe for
comparison it's easy to automatically rename them with suffixes indicating
where they came from. For example, net electricity generation is reported to
both :doc:`FERC Form 1 <../data_sources/ferc1>` and
:doc:`EIA 923<../data_sources/eia923>`, so we've named columns ``net_generation_mwh``
in each of those data sources. Similarly, give non-comparable quantities reported in
different data sources **different** column names. This helps make it clear that the
quantities are actually different.

* ``total`` should come at the beginning of the name (e.g.
  ``total_expns_production``)
* Identifiers should be structured ``type`` + ``_id_`` + ``source`` where
  ``source`` is the agency or organization that has assigned the ID. (e.g.
  ``plant_id_eia``)
* The data source or label (e.g. ``plant_id_pudl``) should follow the thing it
  is describing
* Append units to field names where applicable (e.g.
  ``net_generation_mwh``). This includes "per unit" signifiers (e.g. ``_pct``
  for percent, ``_ppm`` for parts per million, or a generic ``_per_unit`` when
  the type of unit varies, as in columns containing a heterogeneous collection
  of fuels)
* Financial values are assumed to be in nominal US dollars (I.e., the suffix
  _usd is implied.)If they are not reported in USD, convert them to USD. If
  they must be kept in their original form for some reason, append a suffix
  that lets the user know they are not USD.
* ``_id`` indicates the field contains a usually numerical reference to
  another table, which will not be intelligible without looking up the value in
  that other table.
* The suffix ``_code`` indicates the field contains a short abbreviation from
  a well defined list of values, that probably needs to be looked up if you
  want to understand what it means.
* The suffix ``_type`` (e.g. ``fuel_type``) indicates a human readable category
  from a well defined list of values. Whenever possible we try to use these
  longer descriptive names rather than codes.
* ``_name`` indicates a longer human readable name, that is likely not well
  categorized into a small set of acceptable values.
* ``_date`` indicates the field contains a :class:`Date` object.
* ``_datetime`` indicates the field contains a full :class:`Datetime` object.
* ``_year`` indicates the field contains an :class:`integer` 4-digit year.
* ``capacity`` refers to nameplate capacity (e.g. ``capacity_mw``)-- other
  specific types of capacity are annotated.
* Regardless of what label utilities are given in the original data source
  (e.g. ``operator`` in EIA or ``respondent`` in FERC) we refer to them as
  ``utilities`` in PUDL.
* Include verb prefixes (e.g.: ``is_{x}``, ``has_{x}``, or ``served_{x}``)
  to boolean columns to highlight their binary nature. (Not all columns in
  the PUDL database follow this standard, but we'd like them to moving
  forward).

Naming Conventions in Code
--------------------------

In the PUDL codebase, we aspire to follow the naming and other conventions
detailed in :pep:`8`.

Admittedly we have a lot of... named things in here, and we haven't been
perfect about following conventions everywhere. We're trying to clean things up
as we come across them again in maintaining the code.

* Imperative verbs (e.g. connect) should precede the object being acted upon
  (e.g. connect_db), unless the function returns a simple value (e.g. datadir).
* No duplication of information (e.g. form names).
* lowercase, underscores separate words (i.e. ``snake_case``).
* Add a preceding underscore to semi-private helper functions (functions used
  within a single module only and not exposed via the public API).
* When the object is a table, use the full table name (e.g. ingest_fuel_ferc1).
* When dataframe outputs are built from multiple tables, identify the type of
  information being pulled (e.g. "plants") and the source of the tables (e.g.
  ``eia`` or ``ferc1``). When outputs are built from a single table, simply use
  the table name (e.g. ``core_eia923__monthly_boiler_fuel``).

General Abbreviations
^^^^^^^^^^^^^^^^^^^^^

======================= ======================================================
Abbreviation            Definition
======================= ======================================================
``abbr``                abbreviation
``assn``                association
``avg``                 average (mean)
``bbl``                 barrel (quantity of liquid fuel)
``capex``               capital expense
``corr``                correlation
``db``                  database
``deg``                 degree
``df`` & ``dfs``        dataframe & dataframes
``dir``                 directory
``epxns``               expenses
``equip``               equipment
``info``                information
``mcf``                 thousand cubic feet (volume of gas)
``mmbtu``               million British Thermal Units
``mw``                  Megawatt
``mwh``                 Megawatt Hours
``num``                 number
``opex``                operating expense
``pct``                 percent
``ppm``                 parts per million
``ppb``                 parts per billion
``q``                   (fiscal) quarter
``qty``                 quantity
``util`` & ``utils``    utility & utilities
``us``                  United States
``usd``                 US Dollars
``wacc``                Weighted average cost of capital
======================= ======================================================

Data Source Specific Abbreviations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

======================= ======================================================
Abbreviation            Definition
======================= ======================================================
``frc_eia923``          Fuel Receipts and Costs (:doc:`../data_sources/eia923`)
``gen_eia923``          Generation (:doc:`../data_sources/eia923`)
``gf_eia923``           Generation Fuel (:doc:`../data_sources/eia923`)
``gens_eia923``         Generators (:doc:`../data_sources/eia923`)
``utils_eia860``        Utilities (:doc:`../data_sources/eia860`)
``own_eia860``          Ownership (:doc:`../data_sources/eia860`)
======================= ======================================================


Data Extraction Functions
^^^^^^^^^^^^^^^^^^^^^^^^^

The lower level namespace uses an imperative verb to identify the action the
function performs followed by the object of extraction (e.g.
``get_eia860_file``). The upper level namespace identifies the dataset where
extraction is occurring.
