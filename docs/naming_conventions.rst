===============================================================================
Naming Conventions
===============================================================================

In the PUDL codebase, we aspire to follow the naming and other conventions
detailed in :pep:`8`.

Admittedly we have a lot of... named things in here, and we haven't been
perfect about following conventions everywhere. We're trying to clean things up
as we come across them again in maintaining the code.

* Imperative verbs (e.g. connect) should precede the object being acted upon
  (e.g. connect_db), unless the function returns a simple value (e.g. datadir).
* No duplication of information (e.g. form names).
* lowercase, underscores separate words (i.e. ``snake_case``).
* Semi-private helper functions (functions used within a single module only
  and not exposed via the public API) should be preceded by an underscore.
* When the object is a table, use the full table name (e.g. ingest_fuel_ferc1).
* When dataframe outputs are built from multiple tables, identify the type of
  information being pulled (e.g. "plants") and the source of the tables (e.g.
  ``eia`` or ``ferc1``). When outputs are built from a single table, simply use
  the table name (e.g. ``boiler_fuel_eia923``).

.. _glossary:

Glossary of Abbreviations
-------------------------

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
======================= ======================================================

Data Source Specific Abbreviations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

======================= ======================================================
Abbreviation            Definition
======================= ======================================================
``frc_eia923``          Fuel Receipts and Costs (:ref:`data-eia923`)
``gen_eia923``          Generation (:ref:`data-eia923`)
``gf_eia923``           Generation Fuel (:ref:`data-eia923`)
``gens_eia923``         Generators (:ref:`data-eia923`)
``utils_eia860``        Utilities (:ref:`data-eia860`)
``own_eia860``          Ownership (:ref:`data-eia860`)
======================= ======================================================


Data Extraction Functions
-------------------------

The lower level namespace uses an imperative verb to identify the action the
function performs followed by the object of extraction (e.g.
``get_eia860_file``). The upper level namespace identifies the dataset where
extraction is occurring.

Output Functions
-----------------

When dataframe outputs are built from multiple tables, identify the type of
information being pulled (e.g. ``plants``) and the source of the tables (e.g.
``eia`` or ``ferc1``). When outputs are built from a single table, simply use
the table name (e.g. ``boiler_fuel_eia923``).

Table Names
-----------

See `this article <http://www.vertabelo.com/blog/technical-articles/naming-conventions-in-database-modeling>`__ on database naming conventions.

* Table names in snake_case
* The data source should follow the thing it applies to e.g. ``plant_id_ferc1``

Columns and Field Names
-----------------------

* ``total`` should come at the beginning of the name (e.g.
  ``total_expns_production``)
* Identifiers should be structured ``type`` + ``_id_`` + ``source`` where
  ``source`` is the agency or organization that has assigned the ID. (e.g.
  ``plant_id_eia``)
* The data source or label (e.g. ``plant_id_pudl``) should follow the thing it
  is describing
* Units should be appended to field names where applicable (e.g.
  ``net_generation_mwh``). This includes "per unit" signifiers (e.g. ``_pct``
  for percent, ``_ppm`` for parts per million, or a generic ``_per_unit`` when
  the type of unit varies, as in columns containing a heterogeneous collection
  of fuels)
* Financial values are assumed to be in nominal US dollars.
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
