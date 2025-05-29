Timeseries Imputation
===============================================================================

Overview
~~~~~~~~

In energy systems there are many instances of correlated timeseries data that have some
seasonality or periodicity, such as hourly electricity demand or net generation over a
large region. Often these data as they are reported to and published by public agencies
will have missing or anomalous values, which can make them unsuitable for use in some
modeling or analysis tasks. To better support these applications PUDL does outlier
detection and missing value imputation on some of the timeseries data we publish, while
also providing access to the originally reported values. The infrastructure for this
work lives in the :mod:`pudl.analysis.timeseries_cleaning` module. Thus far we have only
applied these methods to the :doc:`/data_sources/eia930` and
:doc:`/data_sources/ferc714` hourly electricity demand data.

For both of these demand timeseries, we flag anomalous values for imputation using
heuristics developed and maintained by:

- `Tyler Ruggles <https://github.com/truggles>`__
- `Alicia Wongel <https://github.com/awongel>`__
- `Greg Schivley <https://github.com/gschivley>`__
- `David Farnham <https://github.com/d-farnham>`__

Their anomaly detection heuristics were originally implemented in
`the truggles GitHub repo, "EIA Cleaned Hourly Electricity Demand Code,"
<https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code>`__ (also
`archived on Zenodo <http://doi.org/10.5281/zenodo.3737085>`__) and published in
`Developing reliable hourly electricity demand data through screening and imputation
<https://doi.org/10.1038/s41597-020-0483-x>`__.

This work was originally applied only to the EIA-930 hourly electricity demand data
reported by balancing authorities (BAs). We re-implemented the heuristics in PUDL and
applied them to the very similar FERC-714 demand data reported by electricity planning
areas, which has a much longer history (going back to 2006, vs. 2015 for the EIA-930).

We also adopted a different imputation method designed for correlated timeseries that
display periodicity. It is very computationally efficient, allowing these timeseries
with tens of millions of values to be imputed in a few minutes on a laptop. The
algorithm was designed for multivariate time series forecasting and we adapted it
from code published by: `Xinyu Chen <https://xinychen.github.io/>`__ in their `Tensor
decomposition for machine learning repository
<https://github.com/xinychen/tensor-learning>`__. The method is described in more detail
in the following papers:

- `Low-Rank Autoregressive Tensor Completion for Multivariate Time Series Forecasting <https://arxiv.org/abs/2006.10436>`__
- `Scalable Low-Rank Tensor Learning for Spatiotemporal Traffic Data Imputation <https://arxiv.org/abs/2008.03194>`__

This method could potentially be applied to other instances of correlated periodic data.
For example, we've considered applying these methods to EIA 930 hourly net generation.

How it Works
~~~~~~~~~~~~

First we identify any values which are missing or deemed anomolous using the heuristics
developed by Ruggels et al., and then we impute those "flagged" values. You can see the
list of reasons why a value might be flagged for imputation in the
:ref:`core_pudl__codes_imputation_reasons` table. After we've flagged the missing and
anomolous values, we impute each year of data independently. The imputation algorithm
aligns all of the timeseries in the input table based on their offset from UTC. Because
the primary electricity demand periodicity is diurnal, the data is reshaped to allow all
the individual days of data to be compared with each other. Then it identifies which
sets of timeseries serve as the best references for each other to impute missing values
using information from those best referenece timeseries.

This means we have to choose what collection of timeseries make sense to impute together
in a single run. For example, the EIA-930 includes both BA-level data and more granular
data from BA sub-regions for some of the larger BAs. We could choose to use only
BA-level data to impute the BA timeseries, and only subregion data to impute the
subregion data, or we could bundle all of the timeseries together and impute them in a
single run, with the BAs informing the subregion results and vice-versa.

It turns out that because the algorithm is good at selecting which of the individual
timeseries within the input data are most appropriate for imputing a given set of
missing values, these two approaches provide almost identical results. We've chosen to
bundle all of the EIA-930 timeseries together in a single imputation run because it
simplifies resource management in the PUDL ETL -- the linear algebra operations that it
relies on are heavily parallelized internally, and doing all of the imputation in a
single job means that we don't have to worry about more than one imputation running
simultaneously resulting in contention for CPU resources.

For simplicity, the FERC-714 data is imputed in a separate run, but in the future we
might consider combining the FERC-714 and EIA-930 data into a single imputation run,
since they both report the same underlying variable and should have very similar curves.

After the imputation is complete, we split out the imputed BA and subregion data into
distinct tables, since they are each associated with different additional columns.

Evaluating Imputation Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To validate the performance of our imputation, we have a separate validation pipeline
that can be enabled in the PUDL ETL and used to compute a quantitative metric scoring
the imputation results.

In this pipeline we compile a collection of simulated data-years in which we've nulled
a set of values that are actually reported in the original data, using masks derived
from observed anomolous or missing values. The masks are selected from months with
particularly high rates of imputation, and applied to months in which there are no
missing or anomolous values.

After imputation we can compare the results to the originally reported values by
computing the Mean Absolute Percentage Error (MAPE) with
:mod:`sklearn.metrics.mean_absolute_percentage_error`. So, in outline the validation
pipeline works as follows:

1. Identify "bad" months where many values were imputed (a month in this case is
   specific to a single reporting entity).
2. Identify "good" months where no values were imputed.
3. Match one "good" month with one "bad" month.
4. Use the pattern of flagged values from the "bad" month to null values in the "good"
   month and flag them as "simulated".
5. Impute any "simulated" null values using all the other time series available to
   inform the imputation.
6. Compare the imputed and reported values and compute the MAPE.
7. (optionally, in production) Check that the MAPE is less than a configurable threshold
   (currently set to 5%) and raise an error if it is not.

This validation pipeline can be enabled in production to make sure it runs every night,
or it can be used as a one off way to validate imputation or compare methods. Currently
it is only enabled manually for development and testing purposes as it is fairly
resource intensive and causes issues in our GitHub CI.

The validation process is stochastic, since it selects different reference months and
imputation masks for each run. As a result, the MAPE values will vary slightly between
different runs. However, across many runs we've seen the following results consistently:

- EIA-930 BAs: MAPE of 2-3%
- EIA-930 BA subregions: MAPE of 1%
- FERC-714: MAPE of 3-4%

Visual inspections of heavily imputed months don't show any obvious individual outliers.

Programming Interface (for developers)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use an `asset factory
<https://docs.dagster.io/guides/build/assets/creating-asset-factories>`__ called
:func:`pudl.analysis.timeseries_cleaning.impute_timeseries_asset_factory`, to generate
a set of assets that impute an upstream timeseries. These generated assets expect the
input to contain an hourly ``datetime`` column, an ID column, and a column with values
to impute. For example:

============================ =================== ===================
balancing_authority_code_eia datetime_utc        demand_reported_mwh
============================ =================== ===================
AEC                          2019-01-01 00:00:00 1000.14
AEC                          2019-01-01 01:00:00 1001.23
...                          ...                 ...
YAD                          2024-12-31 22:00:00 983.12
YAD                          2024-12-31 23:00:00 982.94
============================ =================== ===================

In this instance, the final asset produced from the imputation would contain two new
columns, ``demand_imputed_pudl_mwh`` and ``demand_imputed_pudl_mwh_imputation_code``
(and any other columns which were in the input table). The ``imputation_code`` column
will contain a code for each imputed value, which corresponds to one of those described
in :ref:`core_pudl__codes_imputation_reasons`.

To configure the asset factory, there are a number of parameters to the function, which
are used to specify the names of columns, and there is a settings object called
:class:`pudl.analysis.timeseries_cleaning.ImputeTimeseriesSettings`, which configures
the actual imputation methods.
