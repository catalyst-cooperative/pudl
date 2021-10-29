===============================================================================
Annual Updates
===============================================================================
Much of the data we work with is released in a "final" state annually. We typically
integrate the new year of data over 2-4 weeks in October of each year, since by that
time the final release for the previous year have been published by EIA and FERC.

As of fall 2021 the annual updates include:

* :doc:`/data_sources/eia860`
* :ref:`data-eia861`
* :doc:`/data_sources/eia923`
* :doc:`/data_sources/epacems`
* :ferc1:`data-ferc1`
* :ferc714:`data-ferc714`

This document outlines all the tasks required to complete the annual update, based on
our experience in 2021. You can look at :issue:`1255` to see all the commits that went
into integrating the 2020 data.

* Archive raw inputs on Zenodo
* Update production DOI dictionary in :mod:`pudl.workspace.datastore` to point at
  the new raw input archives.
* Update the :py:const:`pudl.constants.WORKING_PARTITIONS` dictionary to reflect the
  data that is now available within each dataset.
* Update the years of data to be processed in the ``etl_full.yml`` and ``etl_fast.yml``
  settings files stored under ``src/pudl/package_data/settings`` in the PUDL repo.
  You will also likely need to have working copies of the settings that you edit
  throughout the process of integrating the new year of data.
*

FERC 1 ONLY: Open pudl/src/pudl/package_data/meta/ferc1_row_maps/file_map.csv and add
the appropriate file path for the new year of data.

Update the FERC 1 row maps for individual tables, based on the row_num associated with
each named field in the f1_row_lit_tbl -- if the row_chg_yr column hasn’t changed, you
should be able to copy the previous year’s column over to the new year. You can use the
devtools/ferc1/ferc1-new-year.ipynb to check if row mappings have changed.

Either run the ETL and have it download the new archives for you or download them
separately with the pudl_datastore script. This enables you to download all of the new
data in bulk.

If columns with new semantic meanings (i.e. they don’t correspond to any of the columns
which have been mapped in previous years) have appeared in any of the xlsx maps or the
ferc1 row maps, you’ll need to integrate them into the transform functions, the
harvesting process, and the database schema -- where should these new columns end up in
the database, and how will the ETL process get them there?

Test the extract module
Test the transform module
Test the harvesting
Update minmax_row validation tests.
