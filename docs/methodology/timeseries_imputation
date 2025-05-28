===============================================================================
Timeseries Imputation
===============================================================================

-------------------------------------------------------------------------------
Overview
-------------------------------------------------------------------------------

The module ``src/pudl.analysis/timeseries_cleaning.py`` provides infrastructure for
imputing values in hourly timeseries data where there may be missing or anomolous
values. The implementation is adapted from work originally in `this repo
<https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code>`__, which was
developed by:

- `Tyler Ruggles <https://github.com/truggles>`__
- `Alicia Wongel <https://github.com/awongel>`__
- `Greg Schivley <https://github.com/gschivley>`__

So far, we've applied these imputation methods to FERC 714 and EIA 930 hourly demand
data, but theoretically they could be applied to other instances of correlated hourly
data. For example, we've also considered applying these methods to EIA 930 hourly net
generation.

-------------------------------------------------------------------------------
How it Works
-------------------------------------------------------------------------------
At a high level, our imputation methods will first identify any values which are missing
or deemed anomolous, then impute those "flagged" values. There are many reasons a value
may have been deemed anomolous. For a full list of these reasons, including
descriptions, see the table ``core_pudl__codes_imputation_reasons``. After we've
identified all of these anomolous values, we will pass one year of data through actual
imputation at a time. The imputation works by aligning all of the timeseries' in the
input table, and using correlated curves as a reference to impute the missing values.
`This paper <https://arxiv.org/abs/2008.03194>`__ contains a detailed explanation
of the methods we've adopted here.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Interface
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We've developed an `asset factory
<https://docs.dagster.io/guides/build/assets/creating-asset-factories>`__ called
``impute_timeseries_asset_factory``, which can generate a set of assets to apply
imputation to an upstream asset containing timeseries data. These generated assets
expect the upstream input asset to contain an hourly ``datetime`` column, an ID column,
and a column with values to impute. For example:

============================ =================== ===================
balancing_authority_code_eia datetime_utc        demand_reported_mwh
============================ =================== ===================
AEC                          2019-01-01 00:00:00 1000.14
AEC                          2019-01-01 01:00:00 1001.23
                                    ...
YAD                          2024-12-31 22:00:00 983.12
YAD                          2024-12-31 23:00:00 982.94
============================ =================== ===================

In this instance, the final asset produced from the imputation would contain two new
columns, ``demand_imputed_mwh`` and ``demand_imputed_pudl_mwh_imputation_code``
(and any other columns which were in the input table). The ``imputation_code`` will
contain a code for each imputed value, which corresponds to one of those described in
``core_pudl__codes_imputation_reasons``.

To configure the asset factory, there are a number of parameters to the function, which
are used to specify the names of columns, and there is a settings object called
``ImputeTimeseriesSettings``, which configures the actual imputation methods.

-------------------------------------------------------------------------------
Validation
-------------------------------------------------------------------------------
To validate the performance of our imputation, we've developed a parallel validation
pipeline, which can be used to compute a quantitative metric scoring the results.
This works by flagging a set of values for imputation, which would otherwise be deemed
"good". This gives us imputed values, which we can compare to ground-truth data.
Specifically, we compute Mean Absolute Percentage Error (MAPE). This validation
pipeline can be enabled in production to make sure it runs every night, or it can be
used as a one off way to validate imputation or compare methods. Currently, validation
is enabled in production for EIA 930 hourly demand, and we will continually check that
MAPE is above 5%. By default, the validation pipeline will not be enabled, but can be
by setting the ``simulate_flags_settings`` parameter in ``ImputeTimeseriesSettings``.
See the class ``SimulateFlagsSettings`` for detailed descriptions of validation
settings.

The validation pipeline works as follows:

1. Identify months where many values were imputed (a month in this case is specific to
   a single entity).
2. Identify months where many values were imputed.
3. Match one "good" month with one "bad" month.
4. Use the pattern of flagged values from the "bad" month to flag values in the "good"
   months. When we flag values in the "good" data, we call these "simulated" flags.
5. Impute data with simulated flags.
6. Compare imputed values vs reported and compute MAPE.
7. Compare MAPE to a configurable threshold.
