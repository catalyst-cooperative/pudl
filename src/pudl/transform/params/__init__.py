"""A subpackage for defining data source-specific transformation parameters.

Each module in this subpackage is associated with and named after a single dataset (e.g.
``ferc1`` or ``eia923``) and must define a dictionary named ``TRANSFORM_PARAMS``.
This dictionary is a nested data structure with 2 or 3 levels of keys:

* The first level has keys that table names (e.g. ``core_ferc1__yearly_plants_steam``).
* The second level has keys that are the names of transform functions (e.g.
  ``convert_units``).
* In the case of transform functions that operate on a single column and implement the
  :class:`pudl.transform.classes.ColumnTransformFunc` :class:`Protocol` the third level
  of keys is the name of the column the transform should be applied to.

The leaves of this tree structure are dictionaries of keyword arguments used to
instantiate :class:`pudl.transform.classes.TransformParams` objects of the type that
corresponds to the associated transform function, for use with the table or column
that's been identified.

These dictionaries are used by :class:`pudl.transform.classes.AbstractTableTransformer`
to look up the parameters to be used in transforming a table based on the table name.
"""
from . import ferc1  # noqa: F401
