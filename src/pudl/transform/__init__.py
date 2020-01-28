"""
Modules implementing the "Transform" step of the PUDL ETL pipeline.

Each module in this subpackage transforms the tabular data associated with a
single data source from the PUDL :ref:`data-catalog`. This process begins with
a dictionary of "raw" :class:`pandas.DataFrame` objects produced by the
corresponding data source specific routines from the :mod:`pudl.extract`
subpackage, and ends with a dictionary of :class:`pandas.DataFrame` objects
that are fully normalized, cleaned, and congruent with the tabular datapackage
metadata -- i.e. they are ready to be exported by the :mod:`pudl.load` module.

"""
