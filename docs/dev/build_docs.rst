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
you go to see your changes reflected locally, use the ``pixi`` tasks. The
default task removes previously generated outputs and regenerates everything
from scratch:

.. code-block:: console

    $ pixi run docs-build

For CI or local validation-only checks where you don't need rendered HTML,
use the faster ``docs-check`` task:

.. code-block:: console

    $ pixi run docs-check

To check external links in the documentation, use ``docs-linkcheck``:

.. code-block:: console

    $ pixi run docs-linkcheck

You can alter docs-build behavior by setting environment variables when running
the command:

.. code-block:: console

    $ PUDL_DOCS_KEEP_GENERATED_FILES=1 pixi run docs-build
    $ PUDL_DOCS_DISABLE_INTERSPHINX=1 pixi run docs-build

The ``docs-check`` task also runs ``docs-clean`` first. This is intentional:
our Sphinx configuration generates intermediate RST, CSV, and AutoAPI files,
and starting from a clean state avoids validating against stale artifacts from
an earlier partial or failed build.

The ``docs-check`` task also always disables intersphinx, since
the task is intended for fast validation rather than fully rendered external-link
resolution.

By default:

    * Generated RST / CSV files are cleaned up at the end of the build.
    * Intersphinx is enabled, and Sphinx will attempt to fetch external inventories.

Setting ``PUDL_DOCS_KEEP_GENERATED_FILES`` keeps generated files after the build,
which is useful when debugging generated documentation.

Setting ``PUDL_DOCS_DISABLE_INTERSPHINX`` disables intersphinx inventory lookups,
which can make builds more resilient when external documentation sites are
temporarily unavailable.

If you're just working on a single page and don't care about the entire set
of documents being regenerated and linked together, you can call Sphinx
directly:

.. code-block:: console

    $ sphinx-build -b html docs docs/_build/html

This will only update any files that have been changed since the last time the
documentation was generated.

To view the documentation that's been output as HTML, you'll need to open the
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
    by the :mod:`pudl.convert.metadata_to_rst` script that gets run by Sphinx during
    the docs build.

    ``pixi run docs-build`` will build and then delete all generated files via
    ``cleanup_rsts`` and ``cleanup_csv_dir`` in ``docs/conf.py``. If you want to
    preserve them for a one-off build, set
    ``PUDL_DOCS_KEEP_GENERATED_FILES=1`` in the environment when running docs-build.
