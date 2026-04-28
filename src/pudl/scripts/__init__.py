"""Command line interface scripts for PUDL.

All PUDL console scripts (CLI entry points) should be defined in this subpackage.
Each script should live in its own module and expose a single Click command or
group as its public entry point.

Guidelines for contributors and agents
---------------------------------------

- **One module per script**: name the module after the command it provides
  (e.g. ``pudl_datastore.py`` → ``pudl_datastore``).
- **Always name the entry point** ``main``: the Click command or group must be
  called ``main`` in every module. The registered console script name (e.g.
  ``pudl_datastore``) comes from ``pyproject.toml``, not from the Python
  function name. This keeps entry point wiring uniform and avoids name
  collisions between the CLI name and the Python symbol.
- **Thin wrappers only**: the module should contain the Click option/argument
  declarations and just enough glue code to call into the real implementation
  living elsewhere in the ``pudl`` package. Avoid putting substantive business
  logic here.
- **Register the entry point**: after adding a new module, add a corresponding
  line under ``[project.scripts]`` in ``pyproject.toml``, following the
  pattern:

  .. code-block:: toml

      my_command = "pudl.scripts.my_command:main"

- **CLI-only helpers are fine here**: small helpers that exist solely to support
  the CLI (e.g. Click callbacks, output-formatting utilities) may live in the
  same module as the command they serve. General-purpose helpers belong in the
  module that owns the underlying functionality, not here.
- **Decoupling aliases**: if the Click command is imported by name from other
  modules (e.g. tests), keep the existing name as an alias after defining
  ``main``:

  .. code-block:: python

      main = <click-decorated function>
      legacy_name = main  # imported by some_other_module

- **Always support** ``-h`` **and** ``--help``: every Click command must set
  ``context_settings={"help_option_names": ["-h", "--help"]}`` so users can
  get help with either flag without waiting for heavy imports to finish.

Keeping ``--help`` fast: defer heavy imports inside ``main()``
--------------------------------------------------------------

``pudl/__init__.py`` eagerly imports the entire ``pudl`` package tree (analysis,
extract, metadata, transform, …), which adds roughly 7-8 seconds to any
``import pudl.*`` that appears at module level. Click processes ``--help`` and
``-h`` *before* calling the decorated function body, so placing heavy imports
inside ``main()`` is the correct approach and the code is structured this way
throughout the scripts subpackage. **However, this does not yet deliver fast
help output** because ``pudl/__init__.py`` still runs eagerly — the improvement
will only take effect once ``pudl/__init__.py`` is thinned to avoid importing
the full package tree at startup (tracked as a separate task).

Pattern to follow:

.. code-block:: python

    import click                          # lightweight — module level is fine

    @click.command(context_settings={"help_option_names": ["-h", "--help"]})
    @click.option(...)
    def main(...):
        # Deferred to keep --help fast; see pudl/scripts/__init__.py for rationale.
        import pudl                                    # noqa: PLC0415
        from pudl.some.module import SomeClass         # noqa: PLC0415
        ...

The ``# noqa: PLC0415`` suppresses the ruff/pylint "import not at top of file"
warning for each deferred import. Add it to every import inside a function body.

If a module-level constant *must* be computed from a pudl import (e.g. for a
``click.Choice`` list), prefer lightweight class-level introspection over
constructing full objects. For example, use ``sorted(SomeSettings.model_fields)``
rather than ``SomeClass().get_known_values()`` to avoid triggering I/O or
heavyweight initialisation at decoration time.
"""
