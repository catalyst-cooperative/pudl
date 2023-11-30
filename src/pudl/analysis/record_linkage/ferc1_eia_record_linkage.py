"""Connect FERC1 plant tables to EIA's plant-parts via record linkage.

FERC plant records are reported very non-uniformly. In the same table there are records
that are reported as whole plants, individual generators, and collections of prime
movers. This means portions of EIA plants that correspond to a plant record in FERC
Form 1 are heterogeneous, which complicates using the two data sets together.

The EIA plant data is much cleaner and more uniformly structured. The are generators
with ids and plants with ids reported in *seperate* tables. Several generator IDs are
typically grouped under a single plant ID. In :mod:`pudl.analysis.plant_parts_eia`,
we create a large number of synthetic aggregated records representing many possible
slices of a power plant which could in theory be what is actually reported in the FERC
Form 1.

In this module we infer which of the many ``plant_parts_eia`` records is most likely to
correspond to an actually reported FERC Form 1 plant record.
"""
