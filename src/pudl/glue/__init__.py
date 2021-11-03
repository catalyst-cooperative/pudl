"""
Tools for integrating & reconciling different PUDL datasets with each other.

Many of the datasets integrated by PUDL report related information, but it's
often not easy to programmatically relate the datasets to each other. The glue
subpackage provides tools for doing so, making all of the individual datasets
more useful, and enabling richer analyses.

This subpackage contains modules that connect pairs of datasets, like
:mod:`pudl.glue.ferc1_eia` which links together the plants and utilities that
report to both FERC and EIA, but using different names and IDs.

In general we try to enable each dataset to be processed independently, and
optionally apply the glue to connect them to each other when both datasets for
which glue exists are being processed together.

"""
