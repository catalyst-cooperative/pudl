===============================================================================
Building the Documentation
===============================================================================
We use `Sphinx <https://www.sphinx-doc.org/>`__ and
`Read The Docs <https://readthedocs.io>`__ to semi-automatically build and host
our documentation.

Sphinx is tightly integrated with the Python programming language and needs
to be able to import and parse the source code to do its job. Thus, it also
needs to be able to create an appropriate python environment. This process is
controlled by ``docs/conf.py``.

If you are editing the documentation and need to regenerate the outputs as
you go to see your changes reflected locally, the most reliable option is to
use Tox. Tox will remove the previously generated outputs and regenerate
everything from scratch:

.. code-block:: console

    $ tox -e docs

If you're just working on a single page and don't care about the entire set
of documents being regenerated and linked together, you can call Sphinx
directly:

.. code-block:: console

    $ sphinx-build -b html docs docs/_build/html

This will only update any files that have been changed since the last time the
documentation was generated.

To view the documentation that's been output at HTML, you'll need to open the
``docs/_build/html/index.html`` file within the PUDL repository with a web
browser. You may also be able to set up automatic previewing of the rendered
documentation in your text editor with appropriate plugins.

.. note::

    Some of the documentation files are dynamically generated. We use the
    sphinx-apidoc utility to generate RST files from the docstrings embedded
    in our source code, so you should never edit the files under ``docs/api``.
    If you create a new module, the corresponding documentation file will also
    need to be checked in to version control.

    Similarly the :doc:`../data_dictionaries/pudl_db` is generated dynamically
    by the :mod:`pudl.convert.metadata_to_rst` script that gets run by Tox when
    it builds the docs.
