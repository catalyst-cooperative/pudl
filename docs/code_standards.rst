===============================================================================
Code Standards
===============================================================================

* We are trying to keep our own code entirely written in Python.
* PUDL should work on Linux, Mac OS X, or Windows -- don't hard code anything
  that is platform specific, unless you make it work for all platforms.
* Intent is only to support the most recent actively used version or two of
  Python (Currently only Python 3.7, but should also include 3.8 by PUDL
  v0.4.0).
* Assuming that most if not all users will be using ``conda`` to maanage their
  Python software environment.
* Make sure the tests run locally, including the linters. See :doc:`testing`
  for more information.
* Don't decrease the overall test coverage -- if you introduce new code it
  also needs to be exercised by the tests. See :doc:`testing` for details.
* Write good docstrings, using the `Google docstring <https://www.sphinx-doc.org/en/latest/usage/extensions/example_google.html>`__ format.
* PUDL should work for use in application development or for interactive
  analysis (e.g. Jupyter Notebooks).

.. seealso::

    * :doc:`dev_setup`
    * :doc:`testing`
