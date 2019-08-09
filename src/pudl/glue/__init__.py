"""
Tools for integrating & reconciling different PUDL datasets with each other.

Many of the datasets integrated by PUDL report related information, but it's
often not easy to programmatically relate the datasets to each other. The glue
subpackage provides tools for doing so, making all of the individual datasets
more useful, and enabling richer analyses.

In this subpackage there are two basic types of modules:

* those that implement general tools for connecting datasets together (like the
  :mod:`pudl.glue.zipper` module which two tabular datasets based on a set of
  mutually reported variables with no common IDs), and
* those that implement a connection between two specific datasets (like the
  :mod:`pudl.glue.ferc1_eia` module).

In general we try to enable each dataset to be processed independently, and
optionally apply the glue to connect them to each other when both datasets for
which glue exists are being processed together.

"""
