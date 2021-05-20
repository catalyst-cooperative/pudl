.. _data-sources:

Data Sources
============

PUDL data comes from a range of primary sources including FERC, EIA, and
EPA. We run the original data through our Extract-Transform-Load (ETL) pipeline,
outlined in **OUTLINE**, and produce a variety of cleaned outputs that are ready
for further analyses. The data transformations that occur during the ETL are
specified in general terms **HERE** and more specifically in the doc strings for
each of the transform functions.

Visit the page links below to learn about the data that is currently
available through PUDL.

.. toctree::
   :caption: Available Data
   :maxdepth: 1

   data_sources/eia860
   data_sources/eia923
   data_sources/epacems
   data_sources/ferc1

.. toctree::
   :caption: Future Data / Need full pages for current data
   :maxdepth: 1

   data_sources/wip_future
