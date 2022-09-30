"""A subpackage for defining data source specific transformation parameters.

Each module in this subpackage is associated with and named after a single dataset (e.g.
ferc1, eia923) and must define a dictionary named ``TRANSFORM_PARAMS`` in which the keys
are all valid PUDL database table names provided by that dataset, and the values are
are constants which can be used as parameters to construct a TableTransformParams class.

These dictionaries are used by the AbstractTableTransformer class to look up the
parameters to be used in transforming a table, based on its name.
"""
from . import ferc1  # noqa: F401
