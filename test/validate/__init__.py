"""
A collection of tests that check the compiled PUDL data for validity.

It's easy to accidentally mess up the data compilation in a way that results
in bad results, but code that doesn't fail. This suite of sanity checks tries
to catch those errors in a few different ways:

* examining the distribution of variables with known "reasonable" values like
  fuel cost per unit heat content, power plant heat rates, capacity factors,
  etc.
* testing for expected data structures, like unique per-plant-year records.

The tests are organized into modules that pertain to individual data sources,
and parameterized tests within each module that interrogate a particular
table within that data source.

Each test module has a corresponding notebook in the test/notebooks directory
that runs the same test cases and produces useful visual outputs that can help
debug what went wrong when the data validations fail.

"""
