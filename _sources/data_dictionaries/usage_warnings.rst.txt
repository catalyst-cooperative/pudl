=====================================================================================================
PUDL Usage Warnings
=====================================================================================================

The following warnings apply to one or more tables in PUDL. This page tabulates
general information about how each warning is applied. Specific information
about how a warning applies to a particular table is recorded in the Additional
Details section for the table in the PUDL Data Dictionary.

Aggregation hazard:
  Some columns contain subtotals; use caution when choosing columns to aggregate.

Derived values:
  Contains columns derived from inputs and not originally present in sources.

Discontinued by the source:
  The data is no longer being collected or reported in this way.

Discontinued by us:
  PUDL does not currently update its copy of this data.

  If you would be interested in funding additional updates to this data, get in touch!

Early release:
  May contain early release data. EIA releases some of it's annual data early for
  immediate access to individual plant and generator level data. The data has not
  been fully edited and is inappropriate for use in aggregation. Data for certain
  plants may be excluding pending validation.

Estimated values:
  Contains estimated values.

Experimental Work-In-Progress:
  This table is experimental and/or a work in progress and may change in the future.

FERC:
  FERC data is notoriously difficult to extract cleanly, and often contains
  free-form strings, non-labeled total rows and lack of IDs. See `Notable
  Irregularities
  <https://catalystcoop-pudl.readthedocs.io/en/latest/data_sources/ferc1.html#notable-irregularities>`__
  for details.

Free text:
  Contains columns which may appear categorical, but are actually free text.

.. _harvested:

Harvested:
  Data has been drawn from several EIA sources which are not always consistent
  with each other, and PUDL has overridden unusual entries in this table with a
  more consistent value.

  When there are multiple values reported for the same entity, in most cases,
  PUDL chooses the most consistent value reported which is found in at least 70%
  of available entries, and if no value occurs more than 70% of the time, PUDL
  fills in a null value. Internally, we refer to this process as **harvesting**.

  The 70% threshold is the default, and we use different rules for columns with
  additional requirements:

  * Latitude and longitude are particularly noisy, and 70% consistency is not
    attainable very often. We use the 70% threshold when possible, but for
    records that don't meet the threshold, we do a second pass after rounding
    latitude and longitude to the nearest tenth of a degree.
  * Generator operating date has an unusual pattern of missingness that permits
    the most recently reported operating date to be reliable when 70%
    consistency cannot otherwise be reached.
  * We set the consistency threshold to 0% for a few columns so that we always
    get a value: Plant name, utility name and prime mover code.

Imputed values:
  Contains rows where missing values were imputed.

Incomplete ID coverage:
  Not all IDs are present.

Irregular years:
  Some years use a slightly different data definition.

Known discrepancies:
  Contains known calculation discrepancies.

Low coverage:
  Table has known low coverage - either geographic or temporal or otherwise.

Missing years:
  Some years are missing from the data record.

Mixed aggregations:
  Some entries contain aggregates that do not match the table type.

Month as date:
  Date column arbitrarily uses the first of the month.

Multiple inputs:
  Contains information from multiple raw inputs.

No leap year:
  Date column disregards leap years to comply with Actual/365 (Fixed) standard.

Outliers
  Outliers present.

Redacted Values
  Some values have been redacted.

Scale Hazard
  Large table; do not attempt to open with Excel.
