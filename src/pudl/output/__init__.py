"""
Useful post-processing and denormalized outputs based on PUDL.

The datapackages which are output by the PUDL ETL pipeline are well normalized
and suitable for use as relational database tables. This minimizes data
duplication and helps avoid many kinds of data corruption and the potential for
internal inconsistency. However, that's not always the easiest kind of data to
work with. Sometimes we want all the names and IDs in a single dataframe or
table, for human readability. Sometimes you want the useful derived values.

This subpackage compiles a bunch of outputs we found we were commonly
generating, so that they can be done automatically and uniformly. They are
encapsulated within the :class:`pudl.output.pudltabl.PudlTabl` class.

"""
