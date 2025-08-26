Timeseries Imputation
===============================================================================

Overview
~~~~~~~~

In energy systems there are many instances of correlated timeseries data that have some
seasonality or periodicity, such as hourly electricity demand or net generation over a
large region. Often these data as they are reported to and published by public agencies
will have missing or anomalous values, which can make them unsuitable for use in some
modeling or analysis tasks. To better support these applications PUDL detects and
imputes anomalous and missing values in some of the timeseries data we publish, while
also providing access to the originally reported values. The infrastructure for this
work lives in the :mod:`pudl.analysis.timeseries_cleaning` module. Thus far we have only
applied these methods to the :doc:`/data_sources/eia930` and
:doc:`/data_sources/ferc714` hourly electricity demand data.

Anomaly Detection Heuristics
----------------------------

For both of these hourly electricity demand timeseries, we flag anomalous values for
imputation using heuristics developed and maintained by:

- `Tyler Ruggles <https://github.com/truggles>`__
- `Alicia Wongel <https://github.com/awongel>`__
- `Greg Schivley <https://github.com/gschivley>`__
- `David Farnham <https://github.com/d-farnham>`__

Their anomaly detection heuristics for electricity demand were originally implemented in
the `EIA Cleaned Hourly Electricity Demand Code GitHub repository
<https://github.com/truggles/EIA_Cleaned_Hourly_Electricity_Demand_Code>`__ (also
`archived on Zenodo <http://doi.org/10.5281/zenodo.3737085>`__) and published in
`Developing reliable hourly electricity demand data through screening and imputation
<https://doi.org/10.1038/s41597-020-0483-x>`__.

The above authors applied their methods to the EIA-930 hourly electricity demand data
reported by balancing authorities (BAs). We re-implemented the anomaly detection
heuristics in PUDL and now apply them to both EIA-930 and the very similar FERC-714
demand data reported by electricity planning areas. The FERC-714 longer history (going
back to 2006, vs.  2015 for the EIA-930).

Imputation Algorithm
--------------------

PUDL adopts a different imputation method than that of Ruggles et al., developed by
`Xinyu Chen <https://xinychen.github.io/>`__. Chen's algorithm was designed for
multivariate time series forecasting and we adapted it from their reference
implementation, which can be found in their `Tensor decomposition for machine learning
repository <https://github.com/xinychen/tensor-learning>`__ on GitHub. The method is
described in more detail in the following papers:

- `Low-Rank Autoregressive Tensor Completion for Multivariate Time Series Forecasting <https://arxiv.org/abs/2006.10436>`__
- `Scalable Low-Rank Tensor Learning for Spatiotemporal Traffic Data Imputation <https://arxiv.org/abs/2008.03194>`__

It works particularly well on collections of correlated periodic timeseries, and is very
computationally efficient, allowing timeseries with tens of millions of values to be
imputed in a few minutes on a laptop.

This method could potentially be applied to other instances of correlated periodic
electricity data. For example, we've considered applying these methods to EIA 930 hourly
net generation.

How it Works
~~~~~~~~~~~~

First we identify any values which are missing or deemed anomalous using the heuristics
developed by Ruggels et al., and then we impute those "flagged" values. You can see the
list of reasons why a value might be flagged for imputation in the
:ref:`core_pudl__codes_imputation_reasons` table. After we've flagged the missing and
anomalous values, each year is imputed independently, meaning the imputation within a
given year does not depend on the values in any other year. This is done to limit the
memory usage of the process.

The imputation algorithm aligns all of the timeseries in the input table based on their
offset from UTC. Because the primary electricity demand periodicity is diurnal, the data
is reshaped to allow all the individual days of data to be compared with each other.
Then it identifies which sets of timeseries serve as the best references for each other
to impute missing values using information from those best reference timeseries.

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

We use a form of hold-out or simulation-based validation to measure the performance of
our imputation. We artificially remove reported values from 30 randomly selected months
of data that have no values flagged for imputation, using observed patterns of missing
or anomalous values from 30 randomly selected months that have a significant fraction
(10-50%) of such values.  Then we impute the removed values in a validation pipeline
that is separate from the main ETL pipeline, and calculate an error metric comparing the
originally reported values to the imputed values. The error metric we're using is the
Mean Absolute Percentage Error (MAPE) via
:func:`sklearn.metrics.mean_absolute_percentage_error`.

In summary we:

1. Randomly select 30 "bad" months where 10-50% of all values were imputed for a single
   reporting organization (e.g., a balancing authority or electricity planning area).
2. Randomly select 30 "good" months with no imputed values, which could be from any
   reporting organization.
3. Associate each "good" month with one "bad" month.
4. Use the pattern of values flagged for imputation in the "bad" month to remove values
   from the "good" month, flagging them as "simulated".
5. Impute any "simulated" null values using all the other available time series to
   inform the imputation.
6. Compare the imputed and reported values and compute the MAPE.
7. (optionally, in production) Check that the MAPE is less than a configurable threshold
   (currently set to 5%) and raise an error if it is not.

This validation pipeline can be enabled in production so it runs every night, or it can
be used as a one-off way to validate imputation or compare methods. Currently it is only
enabled manually for development and testing purposes as it is resource intensive.

The validation process is stochastic, since it selects different reference months and
imputation masks for each run. As a result, the MAPE values will vary slightly between
different runs. However, across many runs we've seen the following results consistently:

- EIA-930 Balancing Authorities: 2-3% average error
- EIA-930 Balancing Authority Subregions: 1% average error
- FERC-714: Electricity Planning Areas 3-4% average error

Visual inspections of heavily imputed months don't show any obvious individual outliers.

Programming Interface (for developers)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use an `asset factory
<https://docs.dagster.io/guides/build/assets/creating-asset-factories>`__ called
:func:`pudl.analysis.timeseries_cleaning.impute_timeseries_asset_factory`, to generate
a set of assets that impute an upstream timeseries. These generated assets expect the
input to contain an ID column, an hourly ``datetime`` column, and a column with values
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
