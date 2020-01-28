===============================================================================
Data and ETL Design Guidelines
===============================================================================

Here we list some technical norms and expectations that we strive to adhere to,
and hope that contributors can also follow.

We're all learning as we go -- if you have suggestions for best practices we
might want to adopt, let us know!

-------------------------------------------------------------------------------
Input vs. Output Data
-------------------------------------------------------------------------------
It's important to differentiate between the original data we're attempting
to provide easy access to, and analyses or data products that are derived from
that original data. The original data is meant to be archived and re-used as an
alternative to other users re-processing the raw data from various public
agencies. For the sake of reproducibility, it's important that we archive the
inputs alongside the ouputs -- since the reporting agencies often go back and
update the data they have published without warning, and without version
control.

-------------------------------------------------------------------------------
Minimize Data Alteration
-------------------------------------------------------------------------------
We are trying to provide a uniform, easy-to-use interface to existing public
data. We want to provide access to the original data, insofar as that is
possible, while still having it be uniform and easy-to-use. Some alteration is
unavoidable and other changes make the data much more usable, but these should
be made with care and documentation.

* **Make sure data is available at its full, original resolution.**
  Don't aggregate the data unnecessarily when it is brought into PUDL. However,
  creating tools to aggregate it in derived data products is very useful.

.. todo::

    Need fuller enumeration of data alteration / preservation principles.

Examples of Acceptable Changes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Converting all power plant capacities to MW, or all generation to MWh.
* Assigning uniform ``NA`` values.
* Standardizing :mod:`datetime` types.
* Re-naming columns to be the same across years and datasets.
* Assigning simple fuel type codes when the original data source uses free-form
  strings that are not programmatically usable.

Examples of Unacceptable Changes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Applying an inflation adjustment to a financial variable like fuel cost.
  There are a variety of possible inflation indices users might want to use,
  so that transformation should be applied in the output layer that sits on
  top of the original data.
* Aggregating data that has date/time information associated with it into a
  time series, when the individual records do not pertain to unique timesteps.
  For example, the :ref:`data-eia923` Fuel Receipts and Costs table lists fuel
  deliveries by month, but each plant might receive several deliveries from the
  same supplier of the same fuel type in a month -- the individual delivery
  information should be retained.
* Computing heat rates for generators in an original table that contains both
  fuel heat content and net electricity generation, since the heat rate would
  be a derived value, and not part of the original data.

-------------------------------------------------------------------------------
Make Tidy Data
-------------------------------------------------------------------------------
The best practices in data organization go by different names in data science,
statistics, and database design, but they all try to minimize data duplication
and ensure an easy to transform uniform structure that can be used for a wide
variety of purposes -- at least in the source data (i.e. database tables or the
published data packages).

* Each column in a table represents a single, homogeneous variable.
* Each row in a table represents a single observation -- i.e. all of the
  variables reported in that row pertain to the same case/instance of
  something.
* Don't store the same value in more than one pace -- each piece of data should
  have an authoritative source.
* Don't store derived values in the archived data sources.

Reading on Tidy Data
^^^^^^^^^^^^^^^^^^^^
* `Tidy Data <https://vita.had.co.nz/papers/tidy-data.pdf>`__
  A paper on the benefits of organizing data into single variable,
  homogeneously typed columns, and complete single observation records.
  Oriented toward the R programming language, but the ideas apply universally
  to organizing data. (Hadley Wickham, The Journal of Statistical Software,
  2014)
* `Good enough practices in scientific computing <https://doi.org/10.1371/journal.pcbi.1005510>`__
  A whitepaper from the organizers of
  `Software and Data Carpentry <https://carpentries.org/>`__
  on good habits to ensure your work is
  reproducible and reusable — both by yourself and others!
  (Greg Wilson et al., PLOS Computational Biology, 2017)
* `Best practices for scientific computing <https://doi.org/10.1371/journal.pbio.1001745>`__
  An earlier version of the above whitepaper aimed at a more technical,
  data-oriented set of scientific users.
  (Greg Wilson et al., BLOS Biology, 2014)
* `A Simple Guide to Five Normal Forms <http://www.bkent.net/Doc/simple5.htm>`__
  A classic 1983 rundown of database normalization. Concise, informal, and
  understandable, with a few good illustrative examples. Bonus points for the
  ASCII art.

-------------------------------------------------------------------------------
Use Simple Data Types
-------------------------------------------------------------------------------
The Frictionless Data
`TableSchema <https://frictionlessdata.io/specs/table-schema/>`__
standard includes a modest selection of data types, which are meant to be very
widely usable in other contexts. Make sure that whatever data type you're using
is included within that specification, but also be as specific as possible
within that collection of options.

This is one aspect of a broader "least common denominator" strategy that is
common within the open data. This strategy is also behind our decision to
distribute the processed data as CSV files (with metadata stored as JSON).
Frictionless Data
`makes the case <https://frictionlessdata.io/docs/csv/>`__ for CSV files
in their documentation.

-------------------------------------------------------------------------------
Use Consistent Units
-------------------------------------------------------------------------------
Different data sources often use different units to describe the same type of
quantities. Rather than force users to do endless conversions while using the
data, we try to convert similar quantities into the same units during ETL. For
example, we typically convert all electrical generation to MWh, plant
capacities to MW, and heat content to MMBTUs (though, MMBTUs are awful:
seriously M=1000 because Roman numerals? So MM is a million, despite the fact
that M/Mega is a million in SI. And a `BTU
<https://en.wikipedia.org/wiki/British_thermal_unit>`__ is... the amount of
energy required to raise the temperature of one an *avoirdupois pound* of water
by 1 degree *Farenheit*?! What century even is this?).

-------------------------------------------------------------------------------
Silo the ETL Process
-------------------------------------------------------------------------------
It should be possible to run the ETL process on each data source independently,
and with any combination of data sources included. This allows users to include
only the data need. In some cases like the :ref:`EIA 860 <data-eia860>` and
:ref:`EIA 923 <data-eia923>` data, two data sources may be so intertwined that
keeping them separate doesn't really make sense, but that should be the
exception, not the rule.

-------------------------------------------------------------------------------
Separate Data from Glue
-------------------------------------------------------------------------------
The glue that relates different data sources to each other should be applied
after or alongside the ETL process, and not as a mandatory part of ETL. This
makes it easy to pull individual data sources in and work with them even when
the glue isn't working, or doesn't yet exist.

-------------------------------------------------------------------------------
Partition Big Data
-------------------------------------------------------------------------------
Our goal is that users should be able to run the ETL process on a decent
laptop. However, some of the utility datasets are hundreds of gigabytes in size
(e.g. :ref:`EPA CEMS <data-epacems>`, :ref:`FERC EQR <data-ferceqr>`,
:ref:`ISO/RTO LMP <data-tmolmp>`). Many users will not need to use the entire
dataset for the work they are doing. Allow them to pull in only certain years,
or certain states, or other sensible partitions of the data if need be, so that
they don’t run out of memory or disk space, or have to wait hours while data
they don't need is being processed.

-------------------------------------------------------------------------------
Naming Conventions
-------------------------------------------------------------------------------
    *There are only two hard problems in computer science: caching,
    naming things, and off-by-one errors.*

Use Consistent Names
^^^^^^^^^^^^^^^^^^^^
If two columns in different tables record the same quantity in the same units,
give them the same name. That way if they end up in the same dataframe for
comparison it's easy to automatically rename them with suffixes indicating
where they came from. For example net electricity generation is reported to
both :ref:`FERC Form 1 <data-ferc1>` and :ref:`EIA 923 <data-eia923>`, so we've
named columns ``net_generation_mwh`` in each of those data sources. Similarly,
give non-comparable quantities reported in different data sources **different**
column names. This helps make it clear that the quantities are actually
different.

Follow Existing Conventions
^^^^^^^^^^^^^^^^^^^^^^^^^^^
We are trying to use consistent naming conventions for the data tables,
columns, data sources, and functions. Generally speaking PUDL is a collection
of subpackages organized by purpose (extract, transform, load, analysis,
output, datastore…), containing a module for each data source. Each data source
has a short name that is used everywhere throughout the project, composed of
the reporting agency and the form number or another identifying abbreviation:
``ferc1``, ``epacems``, ``eia923``, ``eia8601``, etc. See the :doc:`naming
conventions <naming_conventions>` document for more details.

-------------------------------------------------------------------------------
Complete, Continuous Time Series
-------------------------------------------------------------------------------
Most of the data in PUDL are time series, ranging from hourly to annual in
resolution.

* **Assume and provide contiguous time series.** Otherwise there are just too
  many possible combinations of cases to deal with. E.g. don't expect things to
  work if you pull in data from 2009-2010, and then also from 2016-2018, but
  not 2011-2015.
* **Assume and provide complete time series.** In data that is indexed by date
  or time, ensure that it is available as a complete time series, even if some
  values are missing (and thus NA). Many time series analyses only work when
  all the timesteps are present.
