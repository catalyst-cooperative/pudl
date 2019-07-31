===============================================================================
Data and ETL Design Guidelines
===============================================================================

Some technical norms and expectations that we strive to adhere to, and hope
that contributors can also follow. Some of these are aspirational -- we still
need to go back and apply better naming conventions to much of the data we've
brought in, and the way dates and times are dealt with still needs some work.

Also, we're all learning as we go -- if you have suggestions for best practices
we might want to adopt, let us know!

.. todo::

    * Integrate this list w/ the rest of this section
    * Compare w/ guidelines from other projects

* To the extent possible, it should be possible to process each data source
  independent of the others (e.g. FERC Form 1 can be processed without EIA
  923).
* In some cases this isn't practical because the data is so intertwined (e.g.
  EIA 860 and EIA 923).
* If the data is a time series, it should be possible to load any continuous
  subset of the time series, at it's natural frequency (e.g. EIA 923 is annual
  so it should be possible to load the data for 2017, or 2015-2017, or
  2009-2012, but not necessarily the individual years 2009, 2012, and 2017 or
  the last half of 2016 and the first half of 2017).
* Relationships that connect distinct data sets (which we call "glue") should
  be an optional part of the relational structure.
* Data types must be compatible with the Frictionless Data `Table Schema
  specification <https://frictionlessdata.io/specs/table-schema/>`_
* Data should be minimally altered from what is reported, for instance to
  standardize units, data types, and ``NA`` values, to create linkages between
  and within the datasets which have been integrated, to create well normalized
  database tables with minimal duplication of data, and in some cases to
  categorize free-form strings into well defined taxonomies of categorical
  values.

-------------------------------------------------------------------------------
Make Tidy Data
-------------------------------------------------------------------------------

Minimizing data duplication helps keep data clean and consistent.

For example, we wouldn't compute
heat rates for generators in a table that contains both fuel heat content and
net electricity generation, since the heat rate would be a derived value.
Similarly, we wouldn't apply an inflation adjustment to reported financial
values in the database, since there are a variety of possible inflation indices
users might want to use -- that kind of transformation would be applied in the
output layer that sits on top of the database.

  * **`Tidy Data <https://vita.had.co.nz/papers/tidy-data.pdf>`__** (The Journal of Statistical Software, 2014). On the benefits of organizing data into single variable, homogeneously typed columns, and complete single observation records. By Hadley Wickham of `RStudio <https://www.rstudio.com/>`__, and oriented toward the R programming language, but the ideas apply universally to organizing data.
  * **`Good enough practices in scientific computing <https://doi.org/10.1371/journal.pcbi.1005510>`__** (PLOS Computational Biology, 2017). A whitepaper from the organizers of `Software and Data Carpentry <https://carpentries.org/>`__ on good habits to ensure your work is reproducible and reusable — both by yourself and others!
  * **`Best practices for scientific computing <https://doi.org/10.1371/journal.pbio.1001745>`__** (PLOS Biology, 2014). An earlier version of the above whitepaper aimed at a more technical, data-oriented set of scientific users.
  * **`A Simple Guide to Five Normal Forms <http://www.bkent.net/Doc/simple5.htm>`__** A classic 1983 rundown of database normalization. Concise, informal, and understandable, with a few good illustrative examples. Bonus points for the ASCII art.

-------------------------------------------------------------------------------
Minimize Derived Values
-------------------------------------------------------------------------------

Much more complex and interesting analysis can be performed on top of this
underlying archive of structured data, but at this point it not our intention
to store and distribute the results of such analyses, or to integrate them into
the ETL process. Instead, we provide analytical routines that take the
processed data as inputs. (e.g. calculating the marginal cost of electricity at
the generator level).

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

    "There are only two hard problems in computer science: caching,
    naming things, and off-by-one errors."

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
