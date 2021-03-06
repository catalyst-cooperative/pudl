===============================================================================
Building the Documentation
===============================================================================
We use `Sphinx <https://www.sphinx-doc.org/>`__ to semi-automatically generate our
documentation, based on docstrings embedded in the source code and other content stored
in the ``docs`` directory.

`Read The Docs <https://readthedocs.io>`__ is a platform that automatically
runs Sphinx every time you make a commit to GitHub, and publishes the results
online so that you always have up to date docs. It also archives docs for all
of your previous releases so folks using them can see how things work for their
version of the software, even if it's not the most recent.

Sphinx is tightly integrated with the Python programming language and needs to
be able to import and parse the source code to do its job. Thus, it also needs
to be able to create an appropriate python environment. This process is
controlled by ``docs/conf.py``.

If you are editing the documentation, and need to regenerate the outputs as you
go to see your changes reflected locally, the most reliable option is to use Tox,
which will remove the previously generated outputs, and regenerate everything from
scratch:

.. code-block:: console

    $ tox -e docs

If you're just working on a single page and don't care about the entire set of documents
being regenerated and linked together, you can call Sphinx directly:

.. code-block:: console

    $ sphinx-build -b html docs docs/_build/html

This will only update any files that have been changed since the last time the
documentation was generated.

To view the documentation that's been output at HTML, use the "Open File" option in
your web browser, and point it at the ``docs/_build/html/index.html`` file within the
PUDL repository.
